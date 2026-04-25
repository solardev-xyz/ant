use k256::ecdsa;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("invalid secp256k1 secret key")]
    InvalidSecretKey(#[from] k256::elliptic_curve::Error),
    #[error("signing failed: {0}")]
    Sign(#[from] ecdsa::Error),
    #[error("bad signature")]
    BadSignature,
    #[error("recovered overlay does not match declared overlay")]
    OverlayMismatch,
}
