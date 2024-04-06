use std::num::NonZeroU64;

use crate::{aquadoggo_rpc::{self, CollectionRequest}, db::{query::{Filter, Order, Pagination, Select}, stores::{PaginationCursor, Query}}};

impl From<CollectionRequest> for Query<PaginationCursor> {
    fn from(value: CollectionRequest) -> Self {
        let mut pagination = Pagination::<PaginationCursor>::default();
        let mut order = Order::default();
        let mut filter = Filter::default();
        let select = Select::all();

        if let Some(pag) = value.pagination {
            pagination.first = NonZeroU64::new(pag.first).unwrap();
            if let Some(after) = pag.after {

            }
        }

        Self {
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