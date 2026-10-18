use super::wire::WireStatus;
use super::*;

mod transport;

#[test]
fn pending_operations_keep_their_polling_name() {
    let operation = WireOperation {
        name: "projects/p/instances/i/databases/d/operations/ddl".to_owned(),
        done: false,
        result: None,
    }
    .into_operation()
    .unwrap();
    assert!(!operation.done);
    assert_eq!(
        operation.name,
        "projects/p/instances/i/databases/d/operations/ddl"
    );
    assert!(operation.result.is_none());
}

#[test]
fn successful_ddl_operations_unpack_as_empty() {
    let operation = WireOperation {
        done: true,
        result: Some(OperationResult::Response(prost_types::Any {
            type_url: "type.googleapis.com/google.protobuf.Empty".to_owned(),
            value: Vec::new(),
        })),
        ..Default::default()
    }
    .into_operation()
    .unwrap();
    assert!(operation.done);
    operation.response().unwrap().to_msg::<Empty>().unwrap();
}

#[test]
fn terminal_operation_errors_keep_status_and_details() {
    let retry = google_cloud_rpc::model::RetryInfo::new()
        .set_retry_delay(google_cloud_wkt::Duration::clamp(1, 0));
    let detail =
        google_cloud_gax_internal::grpc::status::any_to_prost(Any::from_msg(&retry).unwrap())
            .unwrap();
    let operation = WireOperation {
        done: true,
        result: Some(OperationResult::Error(WireStatus {
            code: 6,
            message: "table already exists".to_owned(),
            details: vec![detail],
        })),
        ..Default::default()
    }
    .into_operation()
    .unwrap();
    assert!(operation.done);
    let error = operation.error().unwrap();
    assert_eq!(error.code, 6);
    assert_eq!(error.message, "table already exists");
    let detail = error.details[0]
        .to_msg::<google_cloud_rpc::model::RetryInfo>()
        .unwrap();
    assert_eq!(detail.retry_delay.unwrap().seconds(), 1);
}

#[test]
fn unexpected_operation_responses_are_rejected() {
    for response in [
        prost_types::Any {
            type_url: "type.googleapis.com/google.spanner.admin.database.v1.Database".to_owned(),
            value: Vec::new(),
        },
        prost_types::Any {
            type_url: "type.googleapis.com/google.protobuf.Empty".to_owned(),
            value: vec![0xff],
        },
    ] {
        assert!(
            WireOperation {
                done: true,
                result: Some(OperationResult::Response(response)),
                ..Default::default()
            }
            .into_operation()
            .is_err()
        );
    }
}
