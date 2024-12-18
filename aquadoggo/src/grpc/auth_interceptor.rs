use tonic::{service::Interceptor, Request, Status};

#[derive(Clone)]
pub struct AuthInterceptor {
    // Add any fields you need
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        // Get metadata from request
        let metadata = request.metadata();

        // Example: Check for authorization header
        if let Some(auth_header) = metadata.get("authorization") {
            if !auth_header.is_empty() {
                // if validate_token(auth_header.to_str().unwrap()) {
                return Ok(request);
                // }
            }
        }
        Err(Status::unauthenticated("No authorization header"))
    }
}
