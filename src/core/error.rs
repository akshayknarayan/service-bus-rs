use eyre::Report;
use std::convert::From;
use std::error::Error;
use std::fmt::{Display, Formatter, Result};

#[derive(Debug)]
pub enum AzureRequestError {
    BadRequest,               // StatusCode 400
    AuthorizationFailure,     // StatusCode 401
    ResourceFailure,          // StatusCode 403
    ResourceNotFound,         // StatusCode 410
    InternalError,            // StatusCode 500
    UnknownError(Report),     // Catch All
    HyperError(hyper::Error), // Hyper threw an error sending the request.
    LocalMessage,             // The message doesn't exist on the server. You can't change it...
    EmptyBus,                 // There was nothing in the bus to receive.
    NonSerializedBody,
}

impl Error for AzureRequestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        use self::AzureRequestError::*;
        match self {
            &UnknownError(ref e) => Some(e.as_ref()),
            &HyperError(ref e) => Some(e),
            _ => None,
        }
    }
}

impl Display for AzureRequestError {
    fn fmt(&self, f: &mut Formatter) -> Result {
        use self::AzureRequestError::*;
        let s = match self {
            &BadRequest => "Remote returned code 400.",
            &AuthorizationFailure => "Remote returned 401. Check your connection string.",
            &ResourceFailure => {
                "Message failed to send. The message may be too large or the queue is full."
            }
            &ResourceNotFound => "The requested queue does not exist or could not be found.",
            &InternalError => "Remote returned 500 - Internal server error",
            &UnknownError(_) => "Something unexpected happened",
            &HyperError(_) => "Hyper had an issue making a web request",
            &LocalMessage => {
                "The message doesn't exist on the server. This happens when you try and \
                 delete/lock a message you created locally."
            }
            &EmptyBus => {
                "Service Bus Queue/Subscription didn't have any messages before receive timed out."
            }
            &NonSerializedBody => {
                "Parsing the body failed. This happens if the message sender doesn't serialize the \
                 message. Call message.get_body_raw() to extract the body."
            }
        };

        f.write_str(s)
    }
}

impl From<hyper::Error> for AzureRequestError {
    fn from(err: hyper::Error) -> Self {
        AzureRequestError::HyperError(err)
    }
}
