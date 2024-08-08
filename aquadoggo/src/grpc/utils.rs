use std::num::NonZeroU64;
use std::str::FromStr;

use p2panda_rs::document::DocumentId;
use p2panda_rs::operation::{OperationValue, Relation};
use p2panda_rs::schema::Schema;
use tonic::{Result, Status};

use crate::aquadoggo_rpc::{self, CollectionRequest};
use crate::db::query::Cursor;
use crate::db::query::{self, Direction, Field, Filter, MetaField, Order, Pagination, Select};
use crate::db::stores::{PaginationCursor, Query};

impl CollectionRequest {
    pub fn to_query(&self, schema: &Schema) -> Result<Query<PaginationCursor>> {
        let mut pagination = Pagination::<PaginationCursor>::default();
        let mut order = Order::default();
        let mut filter = Filter::default();
        let mut select = Select::default();

        if let Some(first) = self.first {
            pagination.first =
                NonZeroU64::new(first).unwrap_or_else(|| NonZeroU64::MIN.saturating_add(200));
        }

        if let Some(after) = &self.after {
            let cursor = PaginationCursor::decode(after)
                .or_else(|e| Err(Status::invalid_argument(e.to_string())))?;
            pagination.after = Some(cursor);
        }

        if let Some(meta_filter) = &self.meta {
            if let Some(did) = &meta_filter.document_id {
                let doc_id = DocumentId::from_str(did)
                    .or_else(|e| Err(Status::invalid_argument(e.to_string())))?;
                let value = OperationValue::Relation(Relation::new(doc_id));
                let filter_field = Field::Meta(MetaField::DocumentId);
                filter.add(&filter_field, &value);
            }
        }

        for (field_name, condition) in &self.filter {
            let value = &condition.to_operation_value()?;
            let field: &Field = &field_name.as_str().into();
            match condition.operator() {
                aquadoggo_rpc::FilterOperator::Contains => {
                    if let OperationValue::String(s) = value {
                        filter.add_contains(field, s);
                    } else {
                        return Err(Status::invalid_argument(
                            "Contains filter can only be used for string fields",
                        ));
                    }
                }
                aquadoggo_rpc::FilterOperator::Eq => {
                    filter.add(field, value);
                }
                aquadoggo_rpc::FilterOperator::Gt => {
                    filter.add_gt(field, value);
                }
                aquadoggo_rpc::FilterOperator::Gte => {
                    filter.add_gte(field, value);
                }
                aquadoggo_rpc::FilterOperator::In => {
                    // filter.add_in(&filter_field, values);
                }
                aquadoggo_rpc::FilterOperator::Lt => {
                    filter.add_lt(field, value);
                }
                aquadoggo_rpc::FilterOperator::Lte => {
                    filter.add_lte(field, value);
                }
                aquadoggo_rpc::FilterOperator::NotContains => {
                    if let OperationValue::String(s) = value {
                        filter.add_not_contains(field, s);
                    } else {
                        return Err(Status::invalid_argument(
                            "Not-contains filter can only be used for string fields",
                        ));
                    }
                }
                aquadoggo_rpc::FilterOperator::NotEq => {
                    filter.add_not(field, value);
                }
                aquadoggo_rpc::FilterOperator::NotIn => {
                    // filter.add_not_in(field, values);
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
            Some(val) => match val {
                aquadoggo_rpc::filter_condition::Value::BoolVal(b) => {
                    Ok(OperationValue::Boolean(b.clone()))
                }
                aquadoggo_rpc::filter_condition::Value::ByteVal(b) => {
                    Ok(OperationValue::Bytes(b.clone()))
                }
                aquadoggo_rpc::filter_condition::Value::FloatVal(f) => {
                    Ok(OperationValue::Float(f.clone()))
                }
                aquadoggo_rpc::filter_condition::Value::IntVal(i) => {
                    Ok(OperationValue::Integer(i.clone()))
                }
                aquadoggo_rpc::filter_condition::Value::StringVal(s) => {
                    Ok(OperationValue::String(s.clone()))
                }
                aquadoggo_rpc::filter_condition::Value::RelVal(rel) => {
                    if let Some(meta) = &rel.meta {
                        let doc_id = DocumentId::from_str(&meta.document_id)
                            .or_else(|e| Err(Status::invalid_argument(e.to_string())))?;
                        return Ok(OperationValue::Relation(Relation::new(doc_id)));
                    }
                    Err(Status::invalid_argument("No document id provided"))
                }
                _ => Err(Status::invalid_argument("Unsupported")),
            },
            None => Err(Status::invalid_argument("No value provided")),
        }
    }
}
