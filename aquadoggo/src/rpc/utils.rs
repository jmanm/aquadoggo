use std::num::NonZeroU64;
use std::str::FromStr;

use p2panda_rs::document::DocumentId;
use p2panda_rs::operation::{OperationValue, Relation};
use p2panda_rs::{document::DocumentViewId, schema::Schema};
use tonic::{Result, Status};

use crate::aquadoggo_rpc::{self, CollectionRequest};
use crate::db::query::{self, Direction, Filter, MetaField, Order, Pagination, Select};
use crate::db::stores::{OperationCursor, PaginationCursor, Query};

impl CollectionRequest {
    pub fn to_query(&self, schema: &Schema) -> Result<Query<PaginationCursor>> {
        let mut pagination = Pagination::<PaginationCursor>::default();
        let mut order = Order::default();
        let mut filter = Filter::default();
        let mut select = Select::default();

        if let Some(pag) = &self.pagination {
            pagination.first = NonZeroU64::new(pag.first).unwrap();
            if let Some(after) = &pag.after {
                pagination.after = Some(after.clone().into());
            }
            // TODO - add boolean arg to indicate caller needs pagination
            // pagination.fields
        }

        for filter_setting in &self.filter {
            match &filter_setting.filter_by {
                Some(aquadoggo_rpc::filter_setting::FilterBy::Meta(meta_field)) => {

                }
                Some(aquadoggo_rpc::filter_setting::FilterBy::Field(field)) => {
                    let value = field.to_operation_value()?;
                    match filter_setting.operator() {
                        aquadoggo_rpc::FilterOperator::Contains => {
                            // filter.add_contains(&filter_field, value);
                        }
                        aquadoggo_rpc::FilterOperator::Eq => {
                            filter.add(&field.name.as_str().into(), &value);
                        }
                        aquadoggo_rpc::FilterOperator::Gt => {
                            filter.add_gt(&field.name.as_str().into(), &value);
                        }
                        aquadoggo_rpc::FilterOperator::Gte => {
                            filter.add_gte(&field.name.as_str().into(), &value);
                        }
                        aquadoggo_rpc::FilterOperator::In => {
                            // filter.add_in(&filter_field, values);
                        }
                        aquadoggo_rpc::FilterOperator::Lt => {
                            filter.add_lt(&field.name.as_str().into(), &value);
                        }
                        aquadoggo_rpc::FilterOperator::Lte => {
                            filter.add_lte(&field.name.as_str().into(), &value);
                        }
                        aquadoggo_rpc::FilterOperator::NotContains => {
                            // filter.add_not_contains(&field.name.as_str().into(), &value);
                        }
                        aquadoggo_rpc::FilterOperator::NotEq => {
                            filter.add_not(&field.name.as_str().into(), &value);
                        }
                        aquadoggo_rpc::FilterOperator::NotIn => {
                            // filter.add_not_in(&field.name.as_str().into(), values);
                        }
                    }
                },
                None => ()
            }
        }

        if let Some(ord) = &self.order {
            if let Some(aquadoggo_rpc::order::Field::FieldName(name)) = &ord.field {
                let order_by = match name.as_str() {
                    "OWNER" => query::Field::Meta(MetaField::Owner),
                    "DOCUMENT_ID" => query::Field::Meta(MetaField::DocumentId),
                    "DOCUMENT_VIEW_ID" => query::Field::Meta(MetaField::DocumentViewId),
                    field_name => query::Field::new(field_name),
                };
                order.field = Some(order_by);
            }
            let direction = match ord.direction() {
                aquadoggo_rpc::Direction::Ascending => Direction::Ascending,
                aquadoggo_rpc::Direction::Descending => Direction::Descending,
                _ => panic!("Unknown order direction argument key received"),
            };
            order.direction = direction;
        }

        for (field_name, _) in schema.fields().iter() {
            let field = query::Field::Field(field_name.clone());
            select.add(&field);
        }

        Ok(Query {
            pagination,
            order,
            filter,
            select,
        })
    }
}

impl aquadoggo_rpc::Field {
    fn to_operation_value(&self) -> Result<OperationValue> {
        match &self.value {
            Some(val) => {
                match val {
                    aquadoggo_rpc::field::Value::BoolVal(b) => Ok(OperationValue::Boolean(b.clone())),
                    aquadoggo_rpc::field::Value::ByteVal(b) => Ok(OperationValue::Bytes(b.clone())),
                    aquadoggo_rpc::field::Value::FloatVal(f) => Ok(OperationValue::Float(f.clone())),
                    aquadoggo_rpc::field::Value::IntVal(i) => Ok(OperationValue::Integer(i.clone())),
                    aquadoggo_rpc::field::Value::StringVal(s) => Ok(OperationValue::String(s.clone())),
                    aquadoggo_rpc::field::Value::RelVal(rel) => {
                        if let Some(meta) = &rel.meta {
                            let doc_id = DocumentId::from_str(&meta.document_id)
                                .or_else(|e| Err(Status::invalid_argument(e.to_string())))?;
                            return Ok(OperationValue::Relation(Relation::new(doc_id)));
                        }
                        Err(Status::invalid_argument("No document id provided"))
                    }
                    _ => Err(Status::invalid_argument("Unsupported"))
                }
            }
            None => Err(Status::invalid_argument("No value provided"))
        }
    }
}

impl From<PaginationCursor> for aquadoggo_rpc::PaginationCursor {
    fn from(value: PaginationCursor) -> Self {
        Self {
            operation_cursor: value.operation_cursor.to_string(),
            root_operation_cursor: value.root_operation_cursor.map(|c| c.to_string()),
            root_view_id: value.root_view_id.map(|id| id.to_string()),
        }
    }
}

impl From<aquadoggo_rpc::PaginationCursor> for PaginationCursor {
    fn from(value: aquadoggo_rpc::PaginationCursor) -> Self {
        Self {
            operation_cursor: value.operation_cursor.as_str().into(),
            root_operation_cursor: value.root_operation_cursor.map(|c| OperationCursor::from(c.as_str())),
            root_view_id: value.root_view_id.map(|id| DocumentViewId::from_str(&id).unwrap()),
        }
    }
}