use std::num::NonZeroU64;

use p2panda_rs::schema::Schema;

use crate::{aquadoggo_rpc::{self, CollectionRequest}, db::{query::{Field, Filter, Order, Pagination, Select}, stores::{PaginationCursor, Query}}};

impl CollectionRequest {
    pub fn to_query(&self, schema: &Schema) -> Query<PaginationCursor> {
        let mut pagination = Pagination::<PaginationCursor>::default();
        let mut order = Order::default();
        let mut filter = Filter::default();
        let mut select = Select::default();

        if let Some(pag) = &self.pagination {
            pagination.first = NonZeroU64::new(pag.first).unwrap();
            if let Some(after) = &pag.after {

            }
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