use log::debug;
use p2panda_rs::api::publish;
use p2panda_rs::operation::{decode::decode_operation, traits::Schematic, EncodedOperation, OperationId};
use p2panda_rs::entry::{EncodedEntry, traits::AsEncodedEntry};
use tonic::{Request, Response, Result, Status};

use crate::aquadoggo_rpc::{connect_server::Connect, Document, NextArgs};
use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;

pub struct RpcServer {
    context: Context,
    // store: SqlStore,
    // schema_provider: SchemaProvider,
    tx: ServiceSender,
}

impl RpcServer {
    pub fn new(context: Context, tx: ServiceSender) -> Self {
        RpcServer {
            context,
            tx,
        }
    }
}

#[tonic::async_trait]
impl Connect for RpcServer {
    async fn publish(&self, request: Request<Document>) -> Result<Response<NextArgs>> {
        let req = request.into_inner();
        let encoded_entry = EncodedEntry::from_bytes(&req.entry);
        let encoded_operation = EncodedOperation::from_bytes(&req.operation);

        debug!(
            "Query to publish received containing entry with hash {}",
            encoded_entry.hash()
        );

        let operation = decode_operation(&encoded_operation)
            .or_else(|e| Err(Status::invalid_argument(e.to_string())))?;

        let schema = self.context.schema_provider
            .get(operation.schema_id())
            .await
            .ok_or_else(|| "Schema not found")
            .or_else(|e| Err(Status::invalid_argument(e)))?;

        /////////////////////////////////////
        // PUBLISH THE ENTRY AND OPERATION //
        /////////////////////////////////////

        let (backlink, skiplink, seq_num, log_id) = publish(
            &self.context.store,
            &schema,
            &encoded_entry,
            &operation,
            &encoded_operation,
        )
        .await
        .or_else(|e| Err(Status::internal(e.to_string())))?;

        ////////////////////////////////////////
        // SEND THE OPERATION TO MATERIALIZER //
        ////////////////////////////////////////

        // Send new operation on service communication bus, this will arrive eventually at
        // the materializer service

        let operation_id: OperationId = encoded_entry.hash().into();

        if self.tx.send(ServiceMessage::NewOperation(operation_id)).is_err() {
            // Silently fail here as we don't mind if there are no subscribers. We have
            // tests in other places to check if messages arrive.
        }

        let next_args = NextArgs {
            log_id: log_id.as_u64(),
            seq_num: seq_num.as_u64(),
            backlink: backlink.map(|hash| hash.to_string()),
            skiplink: skiplink.map(|hash| hash.to_string()),
        };
        Ok(Response::new(next_args))
    }
}

