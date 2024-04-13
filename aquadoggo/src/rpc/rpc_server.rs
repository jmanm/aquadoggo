use async_recursion::async_recursion;
use futures::future;
use log::debug;
use p2panda_rs::api::{self, publish};
use p2panda_rs::document::{DocumentId, DocumentViewId, DocumentViewValue};
use p2panda_rs::identity::PublicKey;
use p2panda_rs::operation::OperationValue;
use p2panda_rs::operation::{decode::decode_operation, traits::Schematic, EncodedOperation, OperationId};
use p2panda_rs::entry::{EncodedEntry, traits::AsEncodedEntry};
use p2panda_rs::schema::SchemaId;
use p2panda_rs::storage_provider::traits::DocumentStore;
use std::str::FromStr;
use tonic::{Request, Response, Result, Status};

use crate::aquadoggo_rpc::field::Value;
use crate::aquadoggo_rpc::{CollectionRequest, CollectionResponse, Document, DocumentCursorTuple, DocumentMeta, DocumentRequest, DocumentResponse, Field, NextArgsRequest, NextArgsResponse, PaginationCursor, PaginationData, PublishRequest};
use crate::aquadoggo_rpc::connect_server::Connect;
use crate::bus::{ServiceMessage, ServiceSender};
use crate::context::Context;
use crate::db::stores;
use crate::db::types::StorageDocument;

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

    async fn get_document_from_store(&self, document_id: Option<DocumentId>, document_view_id: Option<DocumentViewId>) -> Result<Option<StorageDocument>> {
        let doc = match (document_id, document_view_id) {
            (None, Some(document_view_id)) => self.context.store
                .get_document_by_view_id(&DocumentViewId::from(document_view_id.to_owned())).await,
            (Some(document_id), None) => self.context.store
                .get_document(&DocumentId::from(document_id)).await,
            _ => panic!("Invalid values passed from query field parent"),
        };
        doc.or_else(|e| Err(Status::internal(e.to_string())))
    }

    async fn get_document_cursor_tuple(&self, cur: &stores::PaginationCursor, doc: &StorageDocument) -> Result<DocumentCursorTuple> {
        let cursor = PaginationCursor {
            operation_cursor: cur.operation_cursor.to_string(),
            root_operation_cursor: cur.root_operation_cursor.as_ref().map(|c| c.to_string()),
            root_view_id: cur.root_view_id.as_ref().map(|id| id.to_string())
        };
        let document = self.build_document(doc).await?;
        Ok(DocumentCursorTuple {
            cursor: Some(cursor),
            document: Some(document),
        })
    }

    async fn build_field(&self, name: String, val: DocumentViewValue) -> Result<Field> {
        let field = match val.value() {
            OperationValue::Boolean(bool) => Field {
                name,
                value: Some(Value::BoolVal(bool.clone()))
            },

            OperationValue::Bytes(vec) => Field {
                name,
                value: Some(Value::ByteVal(vec.clone()))
            },

            OperationValue::Integer(int) => Field {
                name,
                value: Some(Value::IntVal(int.clone()))
            },

            OperationValue::Float(float) => Field {
                name,
                value: Some(Value::FloatVal(float.clone()))
            },

            OperationValue::String(string) => Field {
                name,
                value: Some(Value::StringVal(string.clone()))
            },

            OperationValue::Relation(relation) => {
                let related_doc = self.get_document_from_store(Some(relation.document_id().clone()), None)
                    .await?
                    .unwrap();
                Field {
                    name,
                    value: Some(Value::RelVal(
                        self.build_document(&related_doc).await?
                    ))
                }
            },

            // OperationValue::RelationList(RelationList),

            OperationValue::PinnedRelation(pinned_relation) => {
                let related_doc = self.get_document_from_store(None, Some(pinned_relation.view_id().clone()))
                    .await?
                    .unwrap();
                Field {
                    name,
                    value: Some(Value::RelVal(
                        self.build_document(&related_doc).await?
                    ))
                }
            },

            // OperationValue::PinnedRelationList(PinnedRelationList),

            _ => Field {
                name,
                value: None
            }
        };
        Ok(field)
    }

    #[async_recursion]
    async fn build_document(&self, document: &StorageDocument) -> Result<Document> {
        let meta = Some(DocumentMeta {
            document_id: document.id.to_string(),
            view_id: document.view_id.to_string(),
            owner: document.author.to_string(),
        });

        let futures = match &document.fields {
            Some(fields) => fields
                .iter()
                .map(|(name, val)| self.build_field(name.clone(), val.clone()))
                .collect(),
            None => vec![]
        };
        let fields = future::try_join_all(futures).await?;

        Ok(Document {
            meta,
            fields
        })
    }
}

#[tonic::async_trait]
impl Connect for RpcServer {
    async fn get_collection(&self, request: Request<CollectionRequest>) -> Result<Response<CollectionResponse>> {
        let req = request.into_inner();
        let schema_id = SchemaId::new(&req.schema_id)
            .or_else(|e| Err(Status::invalid_argument(e.to_string())))?;
        let schema = self.context.schema_provider
            .get(&schema_id)
            .await
            .ok_or_else(|| "Schema not found")
            .or_else(|e| Err(Status::invalid_argument(e)))?;
        let query = req.to_query(&schema)?;

        let (pagination_data, document_data) = self.context.store.query(&schema, &query, None)
            .await
            .or_else(|e| Err(Status::internal(e.to_string())))?;

        let pagination = Some(PaginationData {
            total_count: pagination_data.total_count.unwrap_or(0),
            has_next_page: pagination_data.has_next_page,
            has_previous_page: pagination_data.has_previous_page,
            start_cursor: pagination_data.start_cursor.map(|c| c.into()),
            end_cursor: pagination_data.end_cursor.map(|c| c.into()),
        });

        let futures = document_data
            .iter()
            .map(|(cur, doc)| self.get_document_cursor_tuple(cur, doc));
        let documents = future::try_join_all(futures).await?;

        Ok(Response::new(CollectionResponse {
            documents,
            pagination,
        }))
    }

    async fn get_document(&self, request: Request<DocumentRequest>) -> Result<Response<DocumentResponse>> {
        let req = request.into_inner();
        let document_id = match req.document_id {
            Some(id) => {
                let doc_id = DocumentId::from_str(&id).or_else(|e| Err(Status::invalid_argument(e.to_string())))?;
                Some(doc_id)
            }
            None => None
        };
        let document_view_id = match req.document_view_id {
            Some(id) => {
                let view_id = DocumentViewId::from_str(&id).or_else(|e| Err(Status::invalid_argument(e.to_string())))?;
                Some(view_id)
            },
            None => None
        };

        // TODO - preload all related documents from store - possibly leverage get_child_document_ids()?
        let document = self.get_document_from_store(document_id, document_view_id).await?;
        let doc_response = match document {
            Some(document) => DocumentResponse { document: Some(self.build_document(&document).await?) },
            None => DocumentResponse { document: None },
        };

        Ok(Response::new(doc_response))
    }

    async fn get_next_args(&self, request: Request<NextArgsRequest>) -> Result<Response<NextArgsResponse>> {
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

    async fn do_publish(&self, request: Request<PublishRequest>) -> Result<Response<NextArgsResponse>> {
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
