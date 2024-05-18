use std::num::NonZeroU64;
use std::str::FromStr;

use p2panda_rs::document::DocumentId;
use p2panda_rs::operation::{OperationValue, Relation};
use p2panda_rs::{document::DocumentViewId, schema::Schema};
use tonic::{Result, Status};

use crate::aquadoggo_rpc::{self, field, CollectionRequest};
use crate::db::query::{self, Direction, Filter, MetaField, Order, Pagination, Select};
use crate::db::stores::{OperationCursor, PaginationCursor, Query};

impl CollectionRequest {
    pub fn to_query(&self, schema: &Schema) -> Result<Query<PaginationCursor>> {
        let mut pagination = Pagination::<PaginationCursor>::default();
        let mut order = Order::default();
        let mut filter = Filter::default();
        let mut select = Select::default();

        if  let Some(first) = &self.first {
            pagination.first = NonZeroU64::new(first);
        }

        if let Some(after) = &self.after {
            pagination.after = Some(PaginationCursor::decode(after));
        }
        
        for (field_name, condition) in &self.filter {
            let value = field.to_operation_value()?;
            match filter_setting.operator() {
                aquadoggo_rpc::FilterOperator::Contains => {
                    // filter.add_contains(&filter_field, value);
                }
                aquadoggo_rpc::FilterOperator::Eq => {
                    filter.add(field_name.as_str().into(), &value);
                }
                aquadoggo_rpc::FilterOperator::Gt => {
                    filter.add_gt(field_name.as_str().into(), &value);
                }
                aquadoggo_rpc::FilterOperator::Gte => {
                    filter.add_gte(field_name.as_str().into(), &value);
                }
                aquadoggo_rpc::FilterOperator::In => {
                    // filter.add_in(&filter_field, values);
                }
                aquadoggo_rpc::FilterOperator::Lt => {
                    filter.add_lt(field_name.as_str().into(), &value);
                }
                aquadoggo_rpc::FilterOperator::Lte => {
                    filter.add_lte(field_name.as_str().into(), &value);
                }
                aquadoggo_rpc::FilterOperator::NotContains => {
                    // filter.add_not_contains(field_name.as_str().into(), &value);
                }
                aquadoggo_rpc::FilterOperator::NotEq => {
                    filter.add_not(field_name.as_str().into(), &value);
                }
                aquadoggo_rpc::FilterOperator::NotIn => {
                    // filter.add_not_in(field_name.as_str().into(), values);
                }
            }
        }

        if let Some(sort_field) = &self.order_by {
            let order_by = match sort_field.as_str() {
                "OWNER" => query::Field::Meta(MetaField::Owner),
                "DOCUMENT_ID" => query::Field::Meta(MetaField::DocumentId),
                "DOCUMENT_VIEW_ID" => query::Field::Meta(MetaField::DocumentViewId),
                field_name => query::Field::new(field_name),
            };
            order.field = Some(order_by);
            
            let direction = match &self.order_direction() {
                aquadoggo_rpc::Direction::Ascending => Direction::Ascending,
                aquadoggo_rpc::Direction::Descending => Direction::Descending,
                _ => Direction::Ascending
            };
            order.direction = direction;
        }

        let has_selections = self.selections.len() > 0;
        for (field_name, _) in schema.fields().iter() {
            if !has_selections || self.selections.contains_key(field_name) {
                let field = query::Field::Field(field_name.clone());
                select.add(&field);    
            }
        }

        Ok(Query {
            pagination,
            order,
            filter,
            select,
        })
    }
}

impl aquadoggo_rpc::FilterCondition {
    fn to_operation_value(&self) -> Result<OperationValue> {
        match &self.value {
            Some(val) => {
                match val {
                    aquadoggo_rpc::filter_condition::Value::BoolVal(b) => Ok(OperationValue::Boolean(b.clone())),
                    aquadoggo_rpc::filter_condition::Value::ByteVal(b) => Ok(OperationValue::Bytes(b.clone())),
                    aquadoggo_rpc::filter_condition::Value::FloatVal(f) => Ok(OperationValue::Float(f.clone())),
                    aquadoggo_rpc::filter_condition::Value::IntVal(i) => Ok(OperationValue::Integer(i.clone())),
                    aquadoggo_rpc::filter_condition::Value::StringVal(s) => Ok(OperationValue::String(s.clone())),
                    aquadoggo_rpc::filter_condition::Value::RelVal(rel) => {
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
