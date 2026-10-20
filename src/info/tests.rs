use super::*;
use arrow_array::{Array, UnionArray};

/// The rows of `build(codes)` as (code, value), with each value as text and a null as `None`.
fn rows(codes: Option<&[InfoCode]>) -> Vec<(u32, Option<String>)> {
    let batch = build(codes.map(|codes| codes.iter().copied().collect())).unwrap();
    assert_eq!(batch.schema(), GET_INFO_SCHEMA.clone());
    let names = batch.column(0).as_any().downcast_ref::<UInt32Array>();
    let values = batch.column(1).as_any().downcast_ref::<UnionArray>();
    let (names, values) = (names.unwrap(), values.unwrap());
    (0..batch.num_rows())
        .map(|i| {
            let value = values.value(i);
            let any = value.as_any();
            let text = match values.type_id(i) {
                STRING_VALUE => {
                    let strings = any.downcast_ref::<StringArray>().unwrap();
                    strings.is_valid(0).then(|| strings.value(0).to_string())
                }
                BOOL_VALUE => Some(
                    any.downcast_ref::<BooleanArray>()
                        .unwrap()
                        .value(0)
                        .to_string(),
                ),
                _ => Some(
                    any.downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0)
                        .to_string(),
                ),
            };
            (names.value(i), text)
        })
        .collect()
}

fn text(value: &str) -> Option<String> {
    Some(value.to_string())
}

/// Every recognised code, in code order. Spanner exposes no server version, so the vendor
/// version is null rather than the driver's.
#[test]
fn all_codes_report_their_values_in_code_order() {
    assert_eq!(
        rows(None),
        [
            (0, text(VENDOR_NAME)),
            (1, None),
            (2, None),
            (3, text("true")),
            (4, text("false")),
            (5, None),
            (6, None),
            (100, text(DRIVER_NAME)),
            (101, text(DRIVER_VERSION)),
            (102, text(ARROW_VERSION)),
            (103, text("1001000")),
        ]
    );
    assert!(ARROW_VERSION.len() > 1, "a v-prefixed arrow-array version");
}

/// adbc.h: "Drivers/vendors will ignore requests for unrecognized codes (the row will be omitted
/// from the result)". XDBC-range and vendor-specific codes arrive as `InfoCode::Other`.
#[test]
fn requested_codes_yield_their_recognised_rows_in_code_order() {
    let requested = [
        InfoCode::Other(10_042),
        InfoCode::DriverVersion,
        InfoCode::VendorName,
        InfoCode::Other(500),
        InfoCode::VendorVersion,
    ];
    assert_eq!(
        rows(Some(&requested)),
        [
            (0, text(VENDOR_NAME)),
            (1, None),
            (101, text(DRIVER_VERSION))
        ]
    );
    assert!(rows(Some(&[InfoCode::Other(500)])).is_empty());
    assert!(rows(Some(&[])).is_empty());
}
