use capnp_rpc::pry;
use log::debug;
use p2panda_rs::api::publish;
use p2panda_rs::entry::{EncodedEntry, traits::AsEncodedEntry};
use p2panda_rs::operation::{decode::decode_operation, traits::Schematic, EncodedOperation, OperationId};

use crate::bus::ServiceMessage;
use crate::{bus::ServiceSender, context::Context};
use crate::aquadoggo_capnp::document_repository;
use capnp::{capability::Promise, Error};

pub struct DocumentRepository {
    context: Context,
    tx: ServiceSender,
}

impl document_repository::Server for DocumentRepository {
    fn publish(
        &mut self, request: document_repository::PublishParams, mut response: document_repository::PublishResults
    ) ->  Promise<(), Error> {
        let req = pry!(request.get());

        let entry = pry!(req.get_entry()).as_bytes();
        let entry_bytes = pry!(hex::decode(entry).or_else(|e| Err(Error::failed(e.to_string()))));
        let encoded_entry = EncodedEntry::from_bytes(&entry_bytes);

        let operation = pry!(req.get_operation()).as_bytes();
        let op_bytes = pry!(hex::decode(operation).or_else(|e| Err(Error::failed(e.to_string()))));
        let encoded_operation = EncodedOperation::from_bytes(&op_bytes);

        debug!(
            "Query to publish received containing entry with hash {}",
            encoded_entry.hash()
        );

        let context = self.context.clone();
        let tx = self.tx.clone();

        Promise::from_future(async move {
            let operation = decode_operation(&encoded_operation)
                .or_else(|e| Err(Error::failed(e.to_string())))?;

            let schema = context.schema_provider
                .get(operation.schema_id())
                .await
                .ok_or_else(|| "Schema not found")
                .or_else(|e| Err(Error::failed(e.to_string())))?;

            /////////////////////////////////////
            // PUBLISH THE ENTRY AND OPERATION //
            /////////////////////////////////////

            let (backlink, skiplink, seq_num, log_id) = publish(
                &context.store,
                &schema,
                &encoded_entry,
                &operation,
                &encoded_operation,
            )
            .await
            .or_else(|e| Err(Error::failed(e.to_string())))?;

            ////////////////////////////////////////
            // SEND THE OPERATION TO MATERIALIZER //
            ////////////////////////////////////////

            // Send new operation on service communication bus, this will arrive eventually at
            // the materializer service

            let operation_id: OperationId = encoded_entry.hash().into();

            if tx.send(ServiceMessage::NewOperation(operation_id)).is_err() {
                // Silently fail here as we don't mind if there are no subscribers. We have
                // tests in other places to check if messages arrive.
            }

            let resp = response.get();
            let mut next_args = resp.get_next_args()?;
            next_args.set_log_id(log_id.as_u64());
            next_args.set_seq_num(seq_num.as_u64());
            next_args.set_backlink(backlink.map(|hash| hash.to_string()).unwrap_or_else(|| String::new()));
            next_args.set_skiplink(skiplink.map(|hash| hash.to_string()).unwrap_or_else(|| String::new()));

            Ok(())
        })
    }
}
