pub mod brokeredmessage;
pub mod queue;
pub mod subscription;

use crate::core::error::AzureRequestError;
use eyre::eyre;
use hyper::StatusCode;

// Here's one function that interprets what all of the error codes mean for consistency.
// This might even get elevated out of this module, but preferabbly not.
pub fn interpret_results(status: StatusCode) -> Result<(), AzureRequestError> {
    use crate::core::error::AzureRequestError::*;
    match status {
        StatusCode::UNAUTHORIZED => Err(AuthorizationFailure),
        StatusCode::INTERNAL_SERVER_ERROR => Err(InternalError),
        StatusCode::BAD_REQUEST => Err(BadRequest),
        StatusCode::FORBIDDEN => Err(ResourceFailure),
        StatusCode::GONE => Err(ResourceNotFound),
        // These are the successful cases.
        StatusCode::CREATED => Ok(()),
        StatusCode::OK => Ok(()),
        e => Err(UnknownError(eyre!("{:?}", e))),
    }
}
