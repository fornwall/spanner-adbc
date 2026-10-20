#![allow(
    clippy::single_range_in_vec_init,
    reason = "a list of one chunk is a single range"
)]

use std::sync::Arc;

use adbc_core::error::Status;
use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatchOptions, StringArray};
use arrow_schema::{DataType, Field, Schema};

use super::*;

/// A batch of `rows` rows with `columns` Int64 columns, 8 bytes per cell.
fn int_batch(rows: usize, columns: usize) -> RecordBatch {
    let fields: Vec<Field> = (0..columns)
        .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
        .collect();
    let arrays: Vec<ArrayRef> = (0..columns)
        .map(|_| Arc::new(Int64Array::from(vec![0_i64; rows])) as ArrayRef)
        .collect();
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .unwrap()
}

/// A batch of `rows` rows with one binary column of `bytes`-byte values.
fn binary_batch(rows: usize, bytes: usize) -> RecordBatch {
    let value = vec![0_u8; bytes];
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("b", DataType::Binary, false)])),
        vec![Arc::new(BinaryArray::from_iter_values(
            std::iter::repeat_n(&value, rows),
        ))],
    )
    .unwrap()
}

#[test]
fn ingest_chunks_cut_at_the_mutation_limit_across_batches() {
    let per_chunk = (INGEST_CHUNK_MUTATION_LIMIT / 10) as usize;
    assert_eq!(chunk_ranges(&[int_batch(per_chunk, 10)]), [0..per_chunk]);
    assert_eq!(
        chunk_ranges(&[int_batch(per_chunk + 1, 10)]),
        [0..per_chunk, per_chunk..per_chunk + 1]
    );
    // Rows are numbered across batches, and a chunk can span them.
    let half = per_chunk / 2 + 1;
    assert_eq!(
        chunk_ranges(&[int_batch(half, 10), int_batch(0, 10), int_batch(half, 10)]),
        [0..per_chunk, per_chunk..2 * half]
    );
    // 20,000 / 3 rounds down.
    assert_eq!(
        chunk_ranges(&[int_batch(6_667, 3)]),
        [0..6_666, 6_666..6_667]
    );
}

#[test]
fn ingest_chunks_cut_at_the_byte_budget() {
    // Two rows of just under 2 MiB fit the 4 MiB budget; a third doesn't.
    let batch = binary_batch(5, 2 * 1024 * 1024 - 8);
    assert_eq!(chunk_ranges(&[batch]), [0..2, 2..4, 4..5]);
}

#[test]
fn ingest_chunks_never_starve_on_an_oversized_row() {
    let batch = binary_batch(3, INGEST_CHUNK_BYTE_BUDGET as usize + 1);
    assert_eq!(chunk_ranges(&[batch]), [0..1, 1..2, 2..3]);
    assert_eq!(chunk_ranges(&[int_batch(100_000, 0)]), [0..100_000]);
}

#[test]
fn ingest_of_zero_rows_emits_no_chunks() {
    assert!(chunk_ranges(&[]).is_empty());
    assert!(chunk_ranges(&[int_batch(0, 10)]).is_empty());
}

#[test]
fn mid_ingest_failure_notes_committed_rows() {
    let source = || {
        let mut e = err("Spanner error: row already exists", Status::AlreadyExists);
        e.vendor_code = 6; // gRPC ALREADY_EXISTS
        e.details = Some(vec![(
            "google.rpc.errorinfo".to_string(),
            br#"{"reason":"DUPLICATE_KEY"}"#.to_vec(),
        )]);
        e
    };
    let untouched = note_rows_already_committed(source(), 0);
    assert_eq!(untouched.message, "Spanner error: row already exists");
    assert_eq!(untouched.status, Status::AlreadyExists);
    let annotated = note_rows_already_committed(source(), 4_000);
    assert!(
        annotated
            .message
            .contains("4000 row(s) from this bulk ingest were already committed"),
        "{}",
        annotated.message
    );
    assert!(
        annotated.message.contains("row already exists"),
        "{}",
        annotated.message
    );
    assert_eq!(annotated.status, Status::AlreadyExists);
    assert_eq!(annotated.vendor_code, 6);
    assert_eq!(annotated.details, source().details);
}

#[test]
fn commit_outcome_is_unknown_after_a_timeout_cancel_or_transport_failure() {
    let error = |status, vendor_code| {
        let mut error = err("commit failed", status);
        error.vendor_code = vendor_code;
        error
    };
    for (status, vendor_code, unknown) in [
        (Status::Timeout, 0, true),
        (Status::Cancelled, 1, true),
        (Status::IO, 0, true),
        (Status::IO, Code::Unavailable as i32, true),
        (Status::IO, Code::Aborted as i32, false),
        (Status::IO, Code::ResourceExhausted as i32, false),
        (Status::Integrity, Code::AlreadyExists as i32, false),
    ] {
        assert_eq!(
            commit_outcome_unknown(&error(status, vendor_code)),
            unknown,
            "{status:?} {vendor_code}"
        );
    }
}

#[test]
fn timed_out_or_cancelled_chunk_reports_unknown_outcome() {
    for status in [Status::Timeout, Status::Cancelled] {
        // The failing chunk may have landed even if no earlier chunk did.
        let first = note_rows_already_committed(err("commit interrupted", status), 0);
        assert_eq!(first.status, status);
        assert!(
            first.message.contains("outcome is unknown")
                && first.message.contains("duplicate rows"),
            "{}",
            first.message
        );

        let later = note_rows_already_committed(err("commit interrupted", status), 4_000);
        assert!(
            later
                .message
                .contains("4000 row(s) from this bulk ingest were already committed"),
            "{}",
            later.message
        );
        assert!(
            later.message.contains("outcome is unknown"),
            "{}",
            later.message
        );
    }
}

#[test]
fn mutation_limit_predicate_matches_only_the_too_many_mutations_error() {
    let mut over_limit = err(
        "Spanner error: The transaction contains too many mutations. Insert and update \
         operations count with the multiplicity of the number of columns they affect. ...Please \
         reduce the number of writes, or use fewer indexes. (Maximum number: 80000)",
        Status::InvalidArguments,
    );
    over_limit.vendor_code = 3; // INVALID_ARGUMENT
    assert!(is_mutation_limit_exceeded(&over_limit));

    let mut wrong_status = over_limit.clone();
    wrong_status.status = Status::Internal;
    assert!(!is_mutation_limit_exceeded(&wrong_status));

    for message in [
        "Spanner error: Invalid value for column Foo: expected INT64",
        "Spanner error: Syntax error: Unexpected token",
        "Spanner error: The commit request is too large",
        "Spanner error: Table not found: Nope",
    ] {
        let other = err(message, Status::InvalidArguments);
        assert!(
            !is_mutation_limit_exceeded(&other),
            "must not match: {message}"
        );
    }

    let mut dup = err("Spanner error: row already exists", Status::AlreadyExists);
    dup.vendor_code = 6;
    assert!(!is_mutation_limit_exceeded(&dup));
}

#[test]
fn accepts_only_the_connections_own_ingest_catalog() {
    assert_eq!(
        check_target_catalog("adbc-test".to_string(), "adbc-test").unwrap(),
        "adbc-test"
    );
    for other in ["main", ""] {
        let error = check_target_catalog(other.to_string(), "adbc-test").unwrap_err();
        assert_eq!(error.status, Status::NotImplemented);
    }
}

#[test]
fn ingest_mode_parses_both_spellings_and_rejects_unknown() {
    let key = OptionStatement::IngestMode;
    for (canonical, short, mode) in [
        ("adbc.ingest.mode.append", "append", IngestMode::Append),
        ("adbc.ingest.mode.create", "create", IngestMode::Create),
        (
            "adbc.ingest.mode.create_append",
            "create_append",
            IngestMode::CreateAppend,
        ),
        ("adbc.ingest.mode.replace", "replace", IngestMode::Replace),
    ] {
        for spelling in [canonical, short] {
            assert_eq!(
                ingest_mode_option(&key, OptionValue::String(spelling.into())).unwrap(),
                mode,
                "spelling {spelling:?}"
            );
        }
        assert_eq!(String::from(mode), canonical);
    }
    let error = ingest_mode_option(&key, OptionValue::String("upsert".into())).unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
    assert!(error.message.contains("ingest mode \"upsert\""), "{error}");
    let error = ingest_mode_option(&key, OptionValue::Int(1)).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("adbc.ingest.mode"), "{error}");
}

/// A small slice of a large batch keeps the parent's buffers; counting them would put each of its
/// rows in a commit of its own.
#[test]
fn row_size_estimate_counts_only_a_slices_rows() {
    let rows = 200_000;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(0..rows)),
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|i| format!("{i:0>32}")),
            )),
        ],
    )
    .unwrap();
    // 8 bytes of id, 4 of offset and 32 of string per row.
    let slice = batch.slice(1_000, 10);
    assert_eq!(estimated_row_bytes(&slice), 44);
    assert_eq!(estimated_row_bytes(&batch), 44);
    assert_eq!(chunk_ranges(&[slice]), [0..10]);
}
