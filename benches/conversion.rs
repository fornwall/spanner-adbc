//! Benchmarks for decoding synthetic Spanner wire values into Arrow arrays, through the hidden
//! `bench_support` module.
//!
//! Run with `cargo bench`, or `cargo bench -- --test` for a single pass. Each benchmark converts
//! one default-size chunk of 8192 rows.

use std::hint::black_box;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, TimeUnit};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use google_cloud_spanner::value::{ToValue, Value};
use spanner_adbc::bench_support::build_array;

/// The default `spanner.rows_per_batch`.
const ROWS: usize = 8192;

fn column(f: impl Fn(usize) -> Value) -> Vec<Value> {
    (0..ROWS).map(f).collect()
}

/// Borrow a column the way `build_batch` passes it to `build_array`.
fn refs(column: &[Value]) -> Vec<Option<&Value>> {
    column.iter().map(Some).collect()
}

/// Convert every column, as `build_batch` does once per chunk.
fn convert(columns: &[(DataType, Vec<Option<&Value>>)]) {
    for (data_type, values) in columns {
        black_box(build_array(data_type, values).expect("benchmark values must decode"));
    }
}

fn bench_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_to_arrow");
    group.throughput(Throughput::Elements(ROWS as u64));

    // The common flat shape. INT64 arrives as a string, FLOAT64 and BOOL natively.
    {
        let ints = column(|i| {
            if i % 16 == 0 {
                None::<i64>.to_value()
            } else {
                (i as i64).wrapping_mul(2_654_435_761).to_value()
            }
        });
        let floats = column(|i| (i as f64 * 0.25).to_value());
        let strings = column(|i| format!("row-{i}-payload").to_value());
        let bools = column(|i| (i % 3 == 0).to_value());
        let columns = [
            (DataType::Int64, refs(&ints)),
            (DataType::Float64, refs(&floats)),
            (DataType::Utf8, refs(&strings)),
            (DataType::Boolean, refs(&bools)),
        ];
        group.bench_function("scalars_int64_float64_string_bool", |b| {
            b.iter(|| convert(&columns))
        });
    }

    // Types parsed from strings.
    {
        let dates = column(|i| format!("2024-{:02}-{:02}", 1 + i % 12, 1 + i % 28).to_value());
        let timestamps = column(|i| {
            format!(
                "2024-01-15T12:{:02}:{:02}.{:09}Z",
                (i / 60) % 60,
                i % 60,
                i % 1_000_000_000
            )
            .to_value()
        });
        let numerics = column(|i| format!("{}.{:09}", i, i % 1_000_000_000).to_value());
        let columns = [
            (DataType::Date32, refs(&dates)),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                refs(&timestamps),
            ),
            (DataType::Decimal128(38, 9), refs(&numerics)),
        ];
        group.bench_function("temporal_date_timestamp_numeric", |b| {
            b.iter(|| convert(&columns))
        });
    }

    {
        let dates = column(|i| {
            if i % 16 == 0 {
                None::<String>.to_value()
            } else {
                format!("2024-{:02}-{:02}", 1 + i % 12, 1 + i % 28).to_value()
            }
        });
        let columns = [(DataType::Date32, refs(&dates))];
        group.bench_function("dates_date32", |b| b.iter(|| convert(&columns)));
    }

    // 48 decoded bytes per cell.
    {
        use base64::Engine as _;
        let bytes = column(|i| {
            if i % 16 == 0 {
                None::<String>.to_value()
            } else {
                let payload: Vec<u8> = (0..48).map(|j| ((i + j) % 256) as u8).collect();
                base64::engine::general_purpose::STANDARD
                    .encode(&payload)
                    .to_value()
            }
        });
        let columns = [(DataType::Binary, refs(&bytes))];
        group.bench_function("bytes_binary", |b| b.iter(|| convert(&columns)));
    }

    // Eight elements per array; structs are positional lists, as on the wire.
    {
        let lists = column(|i| {
            (0..8)
                .map(|j| ((i * 8 + j) as i64).to_value())
                .collect::<Vec<_>>()
                .to_value()
        });
        let structs =
            column(|i| vec![(i as i64).to_value(), format!("name-{i}").to_value()].to_value());
        let columns = [
            (
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                refs(&lists),
            ),
            (
                DataType::Struct(Fields::from(vec![
                    Field::new("id", DataType::Int64, true),
                    Field::new("name", DataType::Utf8, true),
                ])),
                refs(&structs),
            ),
        ];
        group.bench_function("nested_array_int64_struct", |b| {
            b.iter(|| convert(&columns))
        });
    }

    {
        let fields = (0..32)
            .map(|i| Field::new(format!("c{i}"), DataType::Int64, true))
            .collect::<Fields>();
        let structs = column(|i| {
            if i % 16 == 0 {
                None::<i64>.to_value()
            } else {
                Value::from((0..32).map(|j| (i * 32 + j) as i64).collect::<Vec<_>>())
            }
        });
        let columns = [(DataType::Struct(fields), refs(&structs))];
        group.bench_function("wide_struct_32_int64", |b| b.iter(|| convert(&columns)));
    }

    group.finish();
}

criterion_group!(benches, bench_conversion);
criterion_main!(benches);
