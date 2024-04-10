use std::num::NonZeroU64;
use std::str::FromStr;

use p2panda_rs::{document::DocumentViewId, schema::Schema};

use crate::aquadoggo_rpc::{self, CollectionRequest};
use crate::db::query::{Direction, Field, Filter, MetaField, Order, Pagination, Select};
use crate::db::stores::{OperationCursor, PaginationCursor, Query};

impl CollectionRequest {
    pub fn to_query(&self, schema: &Schema) -> Query<PaginationCursor> {
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

        if let Some(ord) = &self.order {
            if let Some(aquadoggo_rpc::order::Field::FieldName(name)) = &ord.field {
                let order_by = match name.as_str() {
                    "OWNER" => Field::Meta(MetaField::Owner),
                    "DOCUMENT_ID" => Field::Meta(MetaField::DocumentId),
                    "DOCUMENT_VIEW_ID" => Field::Meta(MetaField::DocumentViewId),
                    field_name => Field::new(field_name),
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
            let field = Field::Field(field_name.clone());
            select.add(&field);
        }

        Query {
            pagination,
            order,
            filter,
            select,
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