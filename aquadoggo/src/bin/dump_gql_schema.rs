// SPDX-License-Identifier: AGPL-3.0-or-later

use aquadoggo::db::{connection_pool, provider::SqlStorage};
use aquadoggo::graphql::{build_root_schema, Context};

#[tokio::main]
async fn main() {
    let pool = connection_pool("sqlite::memory:", 1).await.unwrap();
    let context = Context::new(SqlStorage::new(pool));
    let schema = build_root_schema(context);
    let sdl = schema.sdl();

    println!("{sdl}");
}
