//! gRPC-only fixture setup for Omni; the wrapper provisions the database with Omni's CLI.

use prost::Message;
use tonic::transport::Channel;

#[path = "../../src/driver/admin/wire.rs"]
mod wire;
use wire::{OperationResult, WireGetOperation, WireOperation, WireUpdateDatabaseDdl};

async fn call<T: Message + Default + Send + Sync + 'static>(
    channel: Channel,
    path: &'static str,
    message: T,
) -> Result<WireOperation, tonic::Status> {
    let mut client = tonic::client::Grpc::new(channel);
    client
        .ready()
        .await
        .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
    client
        .unary(
            tonic::Request::new(message),
            http::uri::PathAndQuery::from_static(path),
            tonic_prost::ProstCodec::default(),
        )
        .await
        .map(tonic::Response::into_inner)
}

pub(crate) async fn update_ddl(
    endpoint: &str,
    database: String,
    statements: Vec<String>,
    proto_descriptors: Vec<u8>,
) -> Result<(), tonic::Status> {
    tokio::time::timeout(std::time::Duration::from_secs(120), async {
        let channel = Channel::from_shared(endpoint.to_string())
            .expect("valid Omni endpoint")
            .connect()
            .await
            .expect("connect to Omni admin API");
        let mut operation = call(
            channel.clone(),
            "/google.spanner.admin.database.v1.DatabaseAdmin/UpdateDatabaseDdl",
            WireUpdateDatabaseDdl {
                database,
                statements,
                proto_descriptors,
                operation_id: String::new(),
            },
        )
        .await?;
        while !operation.done {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            operation = call(
                channel.clone(),
                "/google.longrunning.Operations/GetOperation",
                WireGetOperation {
                    name: operation.name,
                },
            )
            .await?;
        }
        match operation.result {
            Some(OperationResult::Error(error)) => Err(tonic::Status::new(
                tonic::Code::from_i32(error.code),
                error.message,
            )),
            _ => Ok(()),
        }
    })
    .await
    .expect("Omni DDL setup timed out")
}
