use super::*;

struct PanickingSource;

impl ChunkSource for PanickingSource {
    type Row = Row;

    async fn next_chunk(&mut self) -> Result<Vec<Row>> {
        panic!("row decoder failed");
    }
}

#[test]
fn background_fetch_panic_is_an_error_not_end_of_stream() {
    let mut reader = SpannerBatchReader::new(
        crate::runtime::new_runtime().unwrap(),
        CancelSignal::new(),
        Arc::new(Schema::empty()),
        None,
        PanickingSource,
    );
    let error = reader
        .next()
        .expect("failed fetch must not report EOF")
        .unwrap_err();
    let ArrowError::ExternalError(error) = error else {
        panic!("expected an ADBC error");
    };
    let error = error.downcast_ref::<adbc_core::error::Error>().unwrap();
    assert_eq!(error.status, Status::Internal);
    assert!(error.message.contains("result is incomplete"), "{error}");
    assert!(error.message.contains("row decoder failed"), "{error}");
    assert!(reader.next().is_none());
}

/// The FFI stream export (`src/ffi/stream.rs`) recovers the ADBC status by downcasting. If the
/// error were stringified, cancelled reads would stop reporting `ECANCELED` without any FFI test
/// failing.
#[test]
fn to_arrow_error_boxes_the_adbc_error_for_the_ffi_stream_export() {
    let arrow = to_arrow_error(err("operation cancelled", Status::Cancelled));
    let ArrowError::ExternalError(boxed) = &arrow else {
        panic!("the driver error must stay an ExternalError, not be stringified: {arrow:?}")
    };
    let recovered = boxed.downcast_ref::<adbc_core::error::Error>().unwrap();
    assert_eq!(recovered.status, Status::Cancelled);
    assert_eq!(recovered.message, "operation cancelled");
}

#[test]
fn chunk_budget_counts_strings_and_nested_value_storage() {
    let nulls = Value::from(vec![None::<bool>; 4096]);
    let bools = Value::from(vec![false; 4096]);
    let empty_strings = Value::from(vec![""; 4096]);
    for value in [&nulls, &bools, &empty_strings] {
        assert!(approx_value_bytes(value) >= 4096 * size_of::<Value>());
    }
    let nested = Value::from(vec![nulls, bools, empty_strings]);
    assert!(approx_value_bytes(&nested) >= 3 * 4096 * size_of::<Value>());

    let short = Value::from("hello");
    let empty = Value::from("");
    assert_eq!(approx_value_bytes(&short) - approx_value_bytes(&empty), 5);
}
