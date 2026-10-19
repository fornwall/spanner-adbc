use super::*;
use arrow_array::{Array, UnionArray};

#[test]
fn build_matches_schema_and_reports_defaults() {
    let batch = build(None).unwrap();
    assert_eq!(batch.schema(), GET_INFO_SCHEMA.clone());
    assert_eq!(batch.num_rows(), REPORTED.len());
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    let got: Vec<u32> = (0..names.len()).map(|i| names.value(i)).collect();
    let want: Vec<u32> = REPORTED.iter().map(u32::from).collect();
    assert_eq!(got, want);
    assert!(
        want.windows(2).all(|w| w[0] < w[1]),
        "REPORTED must be in strict code order (it is the get_info(None) row order)"
    );
}

#[test]
fn all_codes_result_covers_every_explicitly_answered_code() {
    // Mirrors `MetadataGetInfoAllCodes` in the C++ validation suite.
    let all = build(None).unwrap();
    let all_names = all
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    let all_codes: HashSet<u32> = (0..all_names.len()).map(|i| all_names.value(i)).collect();

    for code in REPORTED {
        let batch = build(Some([*code].into_iter().collect())).unwrap();
        assert_eq!(batch.num_rows(), 1, "explicit request for {code:?}");
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert!(
            all_codes.contains(&names.value(0)),
            "{code:?} answered explicitly but missing from the all-codes result"
        );
    }
}

#[test]
fn requested_codes_yield_one_row_each_in_code_order() {
    let requested = [
        InfoCode::VendorName,
        InfoCode::DriverVersion,
        InfoCode::DriverName,
        InfoCode::VendorVersion, // recognised but valued null
    ];
    let batch = build(Some(requested.iter().copied().collect())).unwrap();
    assert_eq!(batch.schema(), GET_INFO_SCHEMA.clone());
    assert_eq!(batch.num_rows(), 4);

    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    let mut want: Vec<u32> = requested.iter().map(u32::from).collect();
    want.sort_unstable();
    let got: Vec<u32> = (0..names.len()).map(|i| names.value(i)).collect();
    assert_eq!(got, want, "rows are ordered by info code");

    let union = batch
        .column(1)
        .as_any()
        .downcast_ref::<UnionArray>()
        .unwrap();
    let vendor_row = want
        .iter()
        .position(|&c| c == u32::from(&InfoCode::VendorName))
        .unwrap();
    let value = union.value(vendor_row);
    let s = value.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(s.value(0), VENDOR_NAME);
}

#[test]
fn driver_arrow_version_is_reported_with_a_leading_v() {
    assert!(REPORTED.contains(&InfoCode::DriverArrowVersion));

    let batch = build(Some([InfoCode::DriverArrowVersion].into_iter().collect())).unwrap();
    assert_eq!(batch.num_rows(), 1);
    let union = batch
        .column(1)
        .as_any()
        .downcast_ref::<UnionArray>()
        .unwrap();
    let value = union.value(0);
    let s = value.as_any().downcast_ref::<StringArray>().unwrap();
    assert!(!s.is_null(0));
    let version = s.value(0);
    assert!(
        version.starts_with('v') && version.len() > 1,
        "expected a v-prefixed arrow version, got {version:?}"
    );
}

#[test]
fn vendor_version_is_null_not_the_driver_version() {
    // Spanner exposes no server version; reporting the driver's version would mislead.
    assert!(REPORTED.contains(&InfoCode::VendorVersion));
    assert!(REPORTED.contains(&InfoCode::VendorArrowVersion));

    let batch = build(Some(
        [InfoCode::VendorVersion, InfoCode::DriverVersion]
            .into_iter()
            .collect(),
    ))
    .unwrap();
    assert_eq!(batch.num_rows(), 2);

    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    let union = batch
        .column(1)
        .as_any()
        .downcast_ref::<UnionArray>()
        .unwrap();

    let vendor_row = (0..names.len())
        .find(|&i| names.value(i) == u32::from(&InfoCode::VendorVersion))
        .unwrap();
    let vendor_value = union.value(vendor_row);
    let vendor_str = vendor_value
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("VendorVersion lands in the string branch");
    assert!(
        vendor_str.is_null(0),
        "VendorVersion must be null, got {:?}",
        vendor_str.value(0)
    );

    let driver_row = (0..names.len())
        .find(|&i| names.value(i) == u32::from(&InfoCode::DriverVersion))
        .unwrap();
    let driver_value = union.value(driver_row);
    let driver_str = driver_value.as_any().downcast_ref::<StringArray>().unwrap();
    assert!(!driver_str.is_null(0));
    assert_eq!(driver_str.value(0), DRIVER_VERSION);
}

#[test]
fn unrecognized_codes_are_omitted_rather_than_rejected() {
    // adbc.h: "Drivers/vendors will ignore requests for unrecognized codes (the row will be
    // omitted from the result)". XDBC-range and vendor-specific codes arrive as `InfoCode::Other`.
    let batch = build(Some(
        [InfoCode::Other(500), InfoCode::Other(10_042)]
            .into_iter()
            .collect(),
    ))
    .unwrap();
    assert_eq!(batch.schema(), GET_INFO_SCHEMA.clone());
    assert_eq!(batch.num_rows(), 0, "unrecognized codes yield no rows");

    let batch = build(Some(
        [
            InfoCode::Other(10_042),
            InfoCode::DriverName,
            InfoCode::Other(999),
        ]
        .into_iter()
        .collect(),
    ))
    .unwrap();
    assert_eq!(batch.num_rows(), 1);
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    assert_eq!(names.value(0), u32::from(&InfoCode::DriverName));
}

#[test]
fn empty_request_is_an_empty_batch() {
    let batch = build(Some(HashSet::new())).unwrap();
    assert_eq!(batch.schema(), GET_INFO_SCHEMA.clone());
    assert_eq!(batch.num_rows(), 0);
}
