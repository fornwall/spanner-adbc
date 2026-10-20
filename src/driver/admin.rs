//! gRPC DDL transport for Cloud Spanner, the emulator and Omni.
//!
//! Reuse the SDK's authenticated transport and LRO poller. Only the two RPCs needed by DDL
//! are adapted; remove this module when the pinned admin client supports gRPC directly.

use std::sync::Arc;
use std::time::Duration;

use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::options::internal::set_default_idempotency;
use google_cloud_gax::response::{Parts, Response};
use google_cloud_gax::{Result, error::Error};
use google_cloud_gax_internal::grpc::{Client, status::any_from_prost, tonic};
use google_cloud_gax_internal::options::ClientConfig;
use google_cloud_longrunning::model::{GetOperationRequest, Operation};
use google_cloud_spanner_admin_database_v1::{
    client::DatabaseAdmin, model::UpdateDatabaseDdlRequest,
};
use google_cloud_wkt::{Any, Empty};
use prost::Message;

mod wire;
use wire::{OperationResult, WireGetOperation, WireOperation, WireUpdateDatabaseDdl};

pub(crate) async fn database_admin(
    mut config: ClientConfig,
) -> google_cloud_gax::client_builder::Result<DatabaseAdmin> {
    // Cloud schema changes take seconds to minutes. The default poll interval doubles up to a
    // minute, so it can notice completion almost as late again as the change took.
    let polling = ExponentialBackoffBuilder::new()
        .with_initial_delay(Duration::from_secs(1))
        .with_maximum_delay(Duration::from_secs(10))
        .with_scaling(1.5)
        .clamp();
    config.polling_backoff_policy = Some(Arc::new(polling));
    let transport = Client::new(config, "https://spanner.googleapis.com").await?;
    Ok(DatabaseAdmin::from_stub(DdlTransport(transport)))
}

#[derive(Debug)]
struct DdlTransport(Client);

impl DdlTransport {
    async fn call<T: Message + Clone + 'static>(
        &self,
        path: &'static str,
        request: T,
        options: RequestOptions,
        request_params: &str,
    ) -> Result<Response<Operation>> {
        let response = self
            .0
            .execute::<T, WireOperation>(
                tonic::Extensions::new(),
                http::uri::PathAndQuery::from_static(path),
                request,
                options,
                concat!("spanner-adbc/", env!("CARGO_PKG_VERSION")),
                request_params,
            )
            .await?;
        let (metadata, body, _) = response.into_parts();
        Ok(Response::from_parts(
            Parts::new().set_headers(metadata.into_headers()),
            body.into_operation()?,
        ))
    }
}

impl google_cloud_spanner_admin_database_v1::stub::DatabaseAdmin for DdlTransport {
    async fn update_database_ddl(
        &self,
        request: UpdateDatabaseDdlRequest,
        options: RequestOptions,
    ) -> Result<Response<Operation>> {
        let routing = google_cloud_gax_internal::routing_parameter::format(&[Some((
            "database",
            request.database.as_str(),
        ))]);
        self.call(
            "/google.spanner.admin.database.v1.DatabaseAdmin/UpdateDatabaseDdl",
            WireUpdateDatabaseDdl {
                database: request.database,
                statements: request.statements,
                operation_id: request.operation_id,
                proto_descriptors: request.proto_descriptors.to_vec(),
            },
            set_default_idempotency(options, false),
            &routing,
        )
        .await
    }

    async fn get_operation(
        &self,
        request: GetOperationRequest,
        options: RequestOptions,
    ) -> Result<Response<Operation>> {
        let routing = google_cloud_gax_internal::routing_parameter::format(&[Some((
            "name",
            request.name.as_str(),
        ))]);
        self.call(
            "/google.longrunning.Operations/GetOperation",
            WireGetOperation { name: request.name },
            set_default_idempotency(options, true),
            &routing,
        )
        .await
    }

    fn get_polling_error_policy(
        &self,
        options: &RequestOptions,
    ) -> Arc<dyn google_cloud_gax::polling_error_policy::PollingErrorPolicy> {
        self.0.get_polling_error_policy(options)
    }

    fn get_polling_backoff_policy(
        &self,
        options: &RequestOptions,
    ) -> Arc<dyn google_cloud_gax::polling_backoff_policy::PollingBackoffPolicy> {
        self.0.get_polling_backoff_policy(options)
    }
}

impl WireOperation {
    fn into_operation(self) -> Result<Operation> {
        let operation = Operation::new().set_name(self.name).set_done(self.done);
        Ok(match self.result {
            Some(OperationResult::Error(status)) => operation.set_error(
                google_cloud_rpc::model::Status::new()
                    .set_code(status.code)
                    .set_message(status.message)
                    .set_details(status.details.into_iter().filter_map(any_from_prost)),
            ),
            Some(OperationResult::Response(response)) => {
                if response.type_url != "type.googleapis.com/google.protobuf.Empty" {
                    return Err(Error::deser(
                        "unexpected response type in Spanner DDL operation",
                    ));
                }
                <()>::decode(response.value.as_slice()).map_err(Error::deser)?;
                operation.set_response(Any::from_msg(&Empty::default()).map_err(Error::deser)?)
            }
            None => operation,
        })
    }
}

#[cfg(test)]
mod tests;
