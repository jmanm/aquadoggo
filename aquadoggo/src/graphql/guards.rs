use async_graphql::{Guard, Result, context::Context};

use crate::api::Token;

pub struct OwnerGuard {
    owner_id: String,
}

#[async_trait::async_trait]
impl Guard for OwnerGuard {
    async fn check(&self, ctx: &Context<'_>) -> Result<()> {
        match ctx.data::<Token>() {
            Ok(token) => validate_token(&self.owner_id, token),
            Err(_) => Err("No token provided".into())
        }
    }
}

fn validate_token(owner_id: &String, token: &Token) -> Result<()> {
    if *owner_id == token.0 {
        Ok(())  
    } else {
        Err("Forbidden".into())
    }
}