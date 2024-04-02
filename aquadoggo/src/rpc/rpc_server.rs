use futures::TryFutureExt;
use log::debug;
use p2panda_rs::api::{self, publish};
use p2panda_rs::document::DocumentViewId;
use p2panda_rs::identity::PublicKey;
use p2panda_rs::operation::{decode::decode_operation, traits::Schematic, EncodedOperation, OperationId};
use p2panda_rs::entry::{EncodedEntry, traits::AsEncodedEntry};
use std::str::FromStr;
use tonic::{Request, Response, Result, Status};

use crate::aquadoggo_rpc::{NextArgsRequest, Document, NextArgsResponse};
use crate::aquadoggo_rpc::connect_server::Connect;
use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;

pub struct RpcServer {
    context: Context,
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
    async fn next_args(&self, request: Request<NextArgsRequest>) -> Result<Response<NextArgsResponse>> {
        let req = request.into_inner();
        
        let public_key = PublicKey::new(&req.public_key)
            .or_else(|e| Err(Status::invalid_argument(e.to_string())))?;

        let document_view_id = match req.document_view_id {
            Some(id) => Some(
                DocumentViewId::from_str(&id).or_else(|e| Err(Status::invalid_argument(e.to_string())))?
            ),
            None => None
        };
        
        // Calculate next entry's arguments.
        let (backlink, skiplink, seq_num, log_id) = api::next_args(
            &self.context.store,
            &public_key,
            document_view_id.map(|id| id.into()).as_ref(),
        )
        .await
        .or_else(|e| Err(Status::internal(e.to_string())))?;

        // Construct and return the next args.
        let next_args = NextArgsResponse {
            log_id: log_id.as_u64(),
            seq_num: seq_num.as_u64(),
            backlink: backlink.map(|hash| hash.to_string()),
            skiplink: skiplink.map(|hash| hash.to_string()),
        };
        Ok(Response::new(next_args))
    }

    async fn publish(&self, request: Request<Document>) -> Result<Response<NextArgsResponse>> {
        let req = request.into_inner();
        
        let entry_bytes = hex::decode(&req.entry).or_else(|e| Err(Status::invalid_argument(e.to_string())))?;
        let encoded_entry = EncodedEntry::from_bytes(&entry_bytes);
        
        let op_bytes = hex::decode(&req.operation).or_else(|e| Err(Status::invalid_argument(e.to_string())))?;
        let encoded_operation = EncodedOperation::from_bytes(&op_bytes);

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

        let next_args = NextArgsResponse {
            log_id: log_id.as_u64(),
            seq_num: seq_num.as_u64(),
            backlink: backlink.map(|hash| hash.to_string()),
            skiplink: skiplink.map(|hash| hash.to_string()),
        };
        Ok(Response::new(next_args))
    }
}

