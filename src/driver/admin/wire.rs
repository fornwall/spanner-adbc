//! Wire types for the DDL subset of google.spanner.admin.database.v1 and google.longrunning.
//! Unused operation metadata is skipped. Shared with the integration fixture transport.

use prost::Message;

#[derive(Clone, Message)]
pub(crate) struct WireUpdateDatabaseDdl {
    #[prost(string, tag = "1")]
    pub(crate) database: String,
    #[prost(string, repeated, tag = "2")]
    pub(crate) statements: Vec<String>,
    #[prost(string, tag = "3")]
    pub(crate) operation_id: String,
    #[prost(bytes = "vec", tag = "4")]
    pub(crate) proto_descriptors: Vec<u8>,
}

#[derive(Clone, Message)]
pub(crate) struct WireGetOperation {
    #[prost(string, tag = "1")]
    pub(crate) name: String,
}

#[derive(Clone, Message)]
pub(crate) struct WireOperation {
    #[prost(string, tag = "1")]
    pub(crate) name: String,
    #[prost(bool, tag = "3")]
    pub(crate) done: bool,
    #[prost(oneof = "OperationResult", tags = "4, 5")]
    pub(crate) result: Option<OperationResult>,
}

#[derive(Clone, prost::Oneof)]
pub(crate) enum OperationResult {
    #[prost(message, tag = "4")]
    Error(WireStatus),
    #[prost(message, tag = "5")]
    Response(prost_types::Any),
}

#[derive(Clone, Message)]
pub(crate) struct WireStatus {
    #[prost(int32, tag = "1")]
    pub(crate) code: i32,
    #[prost(string, tag = "2")]
    pub(crate) message: String,
    #[prost(message, repeated, tag = "3")]
    pub(crate) details: Vec<prost_types::Any>,
}
