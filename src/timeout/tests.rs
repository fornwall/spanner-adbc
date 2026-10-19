use super::*;
use adbc_core::options::OptionValue;

use crate::options::SharedConfig;
use crate::runtime::{
    CancelSignal, ChunkSource, block_on_cancellable, new_runtime, spawn_prefetch,
};
use crate::{
    OPTION_RPC_TIMEOUT_FETCH as FETCH, OPTION_RPC_TIMEOUT_QUERY as QUERY,
    OPTION_RPC_TIMEOUT_UPDATE as UPDATE,
};

fn s(v: &str) -> OptionValue {
    OptionValue::String(v.to_string())
}

#[test]
fn parses_numeric_strings_ints_and_doubles() {
    let mut config = SharedConfig::default();
    config.set_option(QUERY, s(" 2.5 ")).unwrap();
    assert_eq!(config.option_string(QUERY).unwrap(), "2.5");
    assert_eq!(
        config.timeouts.query_timeout(),
        Some(Duration::from_millis(2500))
    );
    config.set_option(UPDATE, OptionValue::Int(30)).unwrap();
    assert_eq!(config.option_string(UPDATE).unwrap(), "30");
    assert_eq!(
        config.timeouts.update_timeout(),
        Some(Duration::from_secs(30))
    );
    config.set_option(FETCH, OptionValue::Double(0.05)).unwrap();
    assert_eq!(config.option_string(FETCH).unwrap(), "0.05");
    assert_eq!(
        config.timeouts.fetch_timeout(),
        Some(Duration::from_millis(50))
    );
}

#[test]
fn zero_disables_but_still_round_trips() {
    let mut config = SharedConfig::default();
    for value in [s("0"), OptionValue::Int(0), OptionValue::Double(0.0)] {
        config.set_option(QUERY, value).unwrap();
        assert_eq!(config.option_string(QUERY).unwrap(), "0");
        assert_eq!(config.timeouts.query_timeout(), None);
    }
}

#[test]
fn empty_string_unsets() {
    let mut config = SharedConfig::default();
    config.set_option(FETCH, s("1.5")).unwrap();
    assert!(config.option_string(FETCH).ok().is_some());
    config.set_option(FETCH, s("")).unwrap();
    assert_eq!(
        config.option_string(FETCH).unwrap_err().status,
        Status::NotFound
    );
    assert_eq!(config.timeouts.fetch_timeout(), None);
    // Whitespace-only counts as empty too.
    config.set_option(FETCH, s("2")).unwrap();
    config.set_option(FETCH, s("  ")).unwrap();
    assert_eq!(
        config.option_string(FETCH).unwrap_err().status,
        Status::NotFound
    );
}

#[test]
fn rejects_nan_infinities_negatives_and_garbage() {
    let mut config = SharedConfig::default();
    config.set_option(QUERY, s("5")).unwrap();
    let bad_values = [
        OptionValue::Double(f64::NAN),
        OptionValue::Double(f64::INFINITY),
        OptionValue::Double(f64::NEG_INFINITY),
        OptionValue::Double(-1.0),
        OptionValue::Int(-1),
        s("NaN"),
        s("inf"),
        s("-2"),
        s("abc"),
        s("1s"),
        OptionValue::Double(1e300), // too large for Duration
        OptionValue::Bytes(vec![1, 2, 3]),
    ];
    for value in bad_values {
        let error = config.set_option(QUERY, value.clone()).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
        assert!(error.message.contains(QUERY), "{}", error.message);
        assert_eq!(
            config.option_string(QUERY).ok().as_deref(),
            Some("5"),
            "value {value:?}"
        );
    }
}

#[test]
fn the_three_timeouts_are_independent() {
    let mut config = SharedConfig::default();
    config.set_option(QUERY, s("1")).unwrap();
    config.set_option(UPDATE, s("2")).unwrap();
    config.set_option(FETCH, s("3")).unwrap();
    config.set_option(UPDATE, s("")).unwrap();
    assert_eq!(config.option_string(QUERY).unwrap(), "1");
    assert_eq!(
        config.option_string(UPDATE).unwrap_err().status,
        Status::NotFound
    );
    assert_eq!(config.option_string(FETCH).unwrap(), "3");
}

#[test]
fn statement_inherits_then_overrides_independently() {
    let mut connection = SharedConfig::default();
    connection.set_option(QUERY, s("10")).unwrap();
    connection.set_option(FETCH, s("20")).unwrap();

    let mut statement = connection.inherit();
    assert_eq!(statement.option_string(QUERY).unwrap(), "10");
    assert_eq!(statement.option_string(FETCH).unwrap(), "20");

    statement.set_option(QUERY, s("1.5")).unwrap();
    statement.set_option(FETCH, s("")).unwrap();
    assert_eq!(statement.option_string(QUERY).unwrap(), "1.5");
    assert_eq!(
        statement.option_string(FETCH).unwrap_err().status,
        Status::NotFound
    );
    assert_eq!(connection.option_string(QUERY).unwrap(), "10");
    assert_eq!(connection.option_string(FETCH).unwrap(), "20");
}

#[test]
fn with_timeout_passes_a_completing_future_through() {
    let runtime = new_runtime().unwrap();
    let cancel = CancelSignal::new();
    for limit in [None, Some(Duration::from_secs(30))] {
        let result: Result<i32> = block_on_cancellable(
            &runtime,
            &cancel,
            with_timeout(limit, QUERY, async { Ok(7) }),
        );
        assert_eq!(result.unwrap(), 7);
    }
}

#[test]
fn with_timeout_fails_a_hung_operation_with_timeout_status() {
    let runtime = new_runtime().unwrap();
    let cancel = CancelSignal::new();
    let result: Result<()> = block_on_cancellable(
        &runtime,
        &cancel,
        with_timeout(
            Some(Duration::from_millis(50)),
            QUERY,
            std::future::pending(),
        ),
    );
    let error = result.unwrap_err();
    assert_eq!(error.status, Status::Timeout);
    assert!(error.message.contains(QUERY), "{}", error.message);
    assert!(error.message.contains("0.05s"), "{}", error.message);
}

/// A source whose fetches hang, bounded by the fetch timeout like `ResultSetChunks`.
struct StallingSource {
    timeout: Option<Duration>,
}

impl ChunkSource for StallingSource {
    type Row = i32;

    fn next_chunk(&mut self) -> impl std::future::Future<Output = Result<Vec<i32>>> + Send {
        with_timeout(
            self.timeout,
            FETCH,
            std::future::pending::<Result<Vec<i32>>>(),
        )
    }
}

#[test]
fn fetch_timeout_fires_inside_the_prefetch_task() {
    let runtime = new_runtime().unwrap();
    let (mut rx, task) = spawn_prefetch(
        &runtime,
        CancelSignal::new(),
        StallingSource {
            timeout: Some(Duration::from_millis(50)),
        },
    );
    let error = rx.blocking_recv().unwrap().unwrap_err();
    assert_eq!(error.status, Status::Timeout);
    assert!(error.message.contains(FETCH), "{}", error.message);
    assert!(rx.blocking_recv().is_none());
    runtime.block_on(task).unwrap();
}
