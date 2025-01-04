use ed25519_dalek::pkcs8::EncodePublicKey;
use ed25519_dalek::{VerifyingKey, PUBLIC_KEY_LENGTH};
use josekit::{jws::EdDSA, jwt};
use log::{debug, warn};
use tonic::{service::Interceptor, Request, Status};

#[derive(Clone)]
pub struct AuthInterceptor {}

impl AuthInterceptor {
    fn validate_token(pub_key: &str, token: &str) -> bool {
        debug!("Received pub key {} and token {}", pub_key, token);
        if let Ok(key_bytes) = hex::decode(pub_key) {
            if key_bytes.len() == PUBLIC_KEY_LENGTH {
                let verifying_key_bytes: [u8; PUBLIC_KEY_LENGTH] = key_bytes.try_into().unwrap();
                if let Ok(verifiying_key) = VerifyingKey::from_bytes(&verifying_key_bytes) {
                    if let Ok(der) = verifiying_key.to_public_key_der() {
                        if let Ok(verifier) = EdDSA.verifier_from_der(der) {
                            if let Ok((payload, _header)) =
                                jwt::decode_with_verifier(token, &verifier)
                            {
                                debug!("Payload: {}", &payload);
                                return true;
                            }
                        }
                    }
                }
            }
        }
        warn!(
            "Invalid auth headers; pub key: {}, token: {}",
            pub_key, token
        );
        false
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let metadata = request.metadata();
        let key_header = metadata.get("pubKey");
        let token_header = metadata.get("token");

        match (key_header, token_header) {
            (Some(key_cookie), Some(token_cookie)) => {
                let key_str = key_cookie.to_str();
                let token_str = token_cookie.to_str();
                match (key_str, token_str) {
                    (Ok(pub_key), Ok(token)) => {
                        if Self::validate_token(pub_key, token) {
                            Ok(request)
                        } else {
                            Err(Status::unauthenticated("Incorrect signature"))
                        }
                    }
                    _ => Err(Status::unauthenticated("Invalid header values")),
                }
            }
            _ => Err(Status::unauthenticated("Missing required headers")),
        }
    }
}
