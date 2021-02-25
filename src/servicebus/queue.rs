use super::brokeredmessage::*;
use crate::core::{error::AzureRequestError, generate_sas};
use eyre::{eyre, Report};
use hyper::header::{HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use hyper::{Request, Uri};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

const CONTENT_TYPE_VAL: &'static str = "application/atom+xml;type=entry;charset=utf-8";
const SAS_BUFFER_TIME: usize = 15;

/// Client for Service Bus Queues/Topics.
///
/// Queues are useful in a number of situations. Queues are simpler than topics. All producers and
/// consumers read/write from the same queue. This can be used to create load balancing in a server with
/// multiple message consumers all reading messages as they come in. It can also be used in situations
/// when there is asyncronous processing that needs to be done. Producers can add messages to the queue
/// and not need to be concerned with whether there is a process running to consume the message immediately.
///
/// Topics and subscriptions work together hand in hand. Together they provide similar
/// functionality to queues. Producers send message to the topic. Consumers then create a
/// subscription to the topic to receive every messages. Each subscription functions as an
/// individual queue. This is useful when the same message will be read multiple times for
/// different reasons. An example might be adding logging to the Service bus. One subscription
/// might be used to provide load balancing for servers as described in the Queue page. Another
/// subscription will log every message as they come in in a different subscription. This way
/// different processes can consume the message and not interfere with each other or have to worry
/// about losing messages.
///
/// ```
/// use std::thread;
/// let queue = Arc::new(QueueClient::with_conn_and_queue(conn,queue_name));
/// for _ in 0..10 {
///     let q = queue.clone();
///     thread::spawn(move || {
///         q.send(BrokeredMessage::with_body("Sending a concurrent message"));
///     });
/// }
/// ```
#[derive(Clone)]
pub struct QueueClient {
    endpoint: Uri,
    queue_name: String,
    connection_string: String,
    sas_info: Arc<Mutex<(String, usize)>>,
}

impl QueueClient {
    pub fn with_conn_and_queue(connection_string: &str, queue: &str) -> Result<Self, Report> {
        let duration = Duration::from_secs(60 * 6);
        let mut endpoint = None;
        for param in connection_string.split(";") {
            let idx = param.find("=").unwrap_or(0);
            let (mut k, mut value) = param.split_at(idx);
            k = k.trim();
            value = value.trim();
            // cut out the equal sign if there was one.
            if value.len() > 0 {
                value = &value[1..]
            }
            match k {
                "Endpoint" => endpoint = Some(value),
                _ => {}
            };
        }

        let endpoint = endpoint
            .map(|e| e.replace("sb://", "https://"))
            .ok_or(eyre!("Endpoint not in connection string."))?;
        let url = endpoint.parse()?;
        let (sas_key, expiry) = generate_sas(connection_string, duration);

        Ok(QueueClient {
            connection_string: connection_string.to_string(),
            queue_name: queue.to_string(),
            endpoint: url,
            sas_info: Arc::new(Mutex::new((sas_key, expiry - SAS_BUFFER_TIME))),
        })
    }

    pub fn queue(&self) -> &str {
        &self.queue_name
    }

    pub fn endpoint(&self) -> &Uri {
        &self.endpoint
    }

    /// Send a message to the queue. Consumes the message. If the serve returned an error
    /// Then this function will return an error. The default timeout is 30 seconds.
    ///
    /// ```
    /// use servicebus::brokeredmessage::BrokeredMessage;
    ///
    /// let message = BrokeredMessage::with_body("This is a message");
    /// match my_queue.send(message) {
    ///     Ok(_) => println!("The message sent successfully"),
    ///     Err(e) => println!("The error was: {:?}", e);
    /// }
    /// ```
    pub fn send(&self, message: BrokeredMessage) -> Result<Request<String>, Report> {
        let timeout = Duration::from_secs(30);
        self.send_with_timeout(message, timeout)
    }

    /// Receive a message from the queue. Returns either the deserialized message or an error
    /// detailing what went wrong. The message will not be deleted on the server until
    /// `queue_client.complete_message(message)` is called. This is ideal for applications that
    /// can't afford to miss a message.
    pub fn receive(&self) -> Result<Request<()>, Report> {
        let timeout = Duration::from_secs(30);
        self.receive_with_timeout(timeout)
    }

    /// Receive a message from the queue. Returns the deserialized message or an error.
    /// The message is deleted from the queue when it is received. If the application crashes,
    /// the contents of the message can be lost.
    pub fn receive_and_delete(&self) -> Result<Request<()>, Report> {
        let timeout = Duration::from_secs(30);
        self.receive_and_delete_with_timeout(timeout)
    }

    /// Sends a message to the Service Bus Queue with a designated timeout.
    pub fn send_with_timeout(
        &self,
        message: BrokeredMessage,
        timeout: Duration,
    ) -> Result<Request<String>, Report> {
        let sas = self.refresh_sas();
        let mut parts = self.endpoint().clone().into_parts();
        parts.path_and_query =
            Some(format!("{}/messages?timeout={}", self.queue(), timeout.as_secs()).parse()?);
        let uri = Uri::from_parts(parts)?;

        Ok(Request::post(uri)
            .header(AUTHORIZATION, sas)
            .header(
                CONTENT_TYPE,
                HeaderValue::from_str(CONTENT_TYPE_VAL).unwrap(),
            )
            .header(
                BROKER_PROPERTIES_HEADER,
                HeaderValue::from_str(&message.props_as_json()).unwrap(),
            )
            .body(message.into_body())?)
    }

    /// Receive a message from the queue. Returns the deserialized message or an error.
    /// The message is deleted from the queue when it is received. If the application crashes,
    /// the contents of the message can be lost.
    pub fn receive_and_delete_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Request<()>, Report> {
        let sas = self.refresh_sas();
        let mut parts = self.endpoint().clone().into_parts();
        parts.path_and_query = Some(
            format!(
                "{}/messages/head?timeout={}",
                self.queue(),
                timeout.as_secs()
            )
            .parse()?,
        );
        let uri = Uri::from_parts(parts)?;
        Ok(Request::delete(uri).header(AUTHORIZATION, sas).body(())?)
    }

    /// Receive a message from the queue. Returns either the deserialized message or an error
    /// detailing what went wrong. The message will not be deleted on the server until
    /// `queue_client.complete_message(message)` is called. This is ideal for applications that
    /// can't afford to miss a message. Allows a timeout to be specified for greater control.
    pub fn receive_with_timeout(&self, timeout: Duration) -> Result<Request<()>, Report> {
        let sas = self.refresh_sas();

        let mut parts = self.endpoint().clone().into_parts();
        parts.path_and_query = Some(
            format!(
                "{}/messages/head?timeout={}",
                self.queue(),
                timeout.as_secs()
            )
            .parse()?,
        );
        let uri = Uri::from_parts(parts)?;
        Ok(Request::post(uri).header(AUTHORIZATION, sas).body(())?)
    }

    /// Completes a message that has been received from the Service Bus. This will fail
    /// if the message was created locally. Once a message is created, it cannot be restored
    ///
    /// ```
    /// let message = my_queue.receive().unwrap();
    /// // Do lots of processing with the message. Send it to another database.
    /// my_queue.complete_message(message);
    /// ```
    pub fn complete_message(&self, message: BrokeredMessage) -> Result<Request<()>, Report> {
        let sas = self.refresh_sas();

        // Take either the Sequence number or the Message ID
        // Then add the lock token and finally join it into the targer
        let target = self.get_message_update_path(&message)?;

        Ok(Request::delete(target)
            .header(AUTHORIZATION, sas)
            .body(())?)
    }

    /// Releases the lock on a message and puts it back into the queue.
    /// This method generally indicates that the message could not be
    /// handled properly and should be attempted at a later time.
    pub fn abandon_message(&self, message: BrokeredMessage) -> Result<Request<()>, Report> {
        let sas = self.refresh_sas();
        let target = self.get_message_update_path(&message)?;
        Ok(Request::put(target).header(AUTHORIZATION, sas).body(())?)
    }

    /// Renews the lock on a message. If a message is received by calling
    /// `queue.receive()` or `queue.receive_with_timeout()` then the message is locked
    /// but not deleted on the Service Bus. This method allows the lock to be renewed
    /// if additional time is needed to finish processing the message.
    ///
    /// ```
    /// use std::thread::sleep;
    ///
    /// let message = queue.receive();
    /// sleep(2*60*1000);
    /// //Renew the lock on the message so that we can keep processing it.
    /// queue.renew_message(message);
    /// sleep(2*60*1000);
    /// queue.complete_message(message);
    /// ```
    pub fn renew_message(&self, message: &BrokeredMessage) -> Result<Request<()>, Report> {
        let sas = self.refresh_sas();
        let target = self.get_message_update_path(&message)?;
        Ok(Request::post(target).header(AUTHORIZATION, sas).body(())?)
    }

    // Complete, Abandon, Renew all make calls to the same Uri so here's a quick function
    // for generating it.
    fn get_message_update_path(&self, message: &BrokeredMessage) -> Result<Uri, AzureRequestError> {
        // Take either the Sequence number or the Message ID
        // Then add the lock token and finally join it into the targer
        let target = message
            .props
            .SequenceNumber
            .map(|seq| seq.to_string())
            .or(message.props.MessageId.clone())
            .and_then(|id| message.props.LockToken.as_ref().map(|lock| (id, lock)))
            .map(|(id, lock)| format!("/{}/messages/{}/{}", self.queue(), id, lock))
            .and_then(|path| {
                let mut parts = self.endpoint().clone().into_parts();
                parts.path_and_query = Some(path.parse().ok()?);
                Uri::from_parts(parts).ok()
            })
            .ok_or(AzureRequestError::LocalMessage);
        target
    }

    fn refresh_sas(&self) -> HeaderValue {
        let curr_time = std::time::SystemTime::UNIX_EPOCH
            .elapsed()
            .expect("unix epoch time comparison")
            .as_secs();
        let mut sas_tuple = match self.sas_info.lock() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };
        if curr_time > (sas_tuple.1 as _) {
            let duration = Duration::from_secs(60 * 6);
            let (key, expiry) = generate_sas(&*self.connection_string, duration);
            sas_tuple.1 = expiry;
            sas_tuple.0 = key;
        }

        HeaderValue::from_str(&sas_tuple.0).unwrap()
    }
}

/*
#[cfg(test)]
mod tests {
    use super::*;
    use crate::servicebus::brokeredmessage::BrokeredMessage;

    #[test]
    fn queue_send_message() {
        let queue = QueueClient::with_conn_and_queue(&CONNECTION_STRING, "test1").unwrap();
        let message = BrokeredMessage::with_body("Cats and Dogs");
        match queue.send(message) {
            Err(e) => {
                println!("{:?}", e);
                panic!("Failed to send message.")
            }
            _ => assert!(true),
        }
    }

    #[test]
    fn queue_receive_message() {
        let queue = QueueClient::with_conn_and_queue(&CONNECTION_STRING, "test1").unwrap();
        match queue.receive_and_delete() {
            Err(e) => {
                println!("{:?}", e);
                panic!("Failed to receive message.")
            }
            Ok(_) => {}
        }
    }

    #[test]
    fn queue_complete_message() {
        let queue = QueueClient::with_conn_and_queue(&CONNECTION_STRING, "test1").unwrap();
        queue
            .send(BrokeredMessage::with_body("Complete this message"))
            .unwrap();
        match queue.receive() {
            Err(e) => {
                println!("{:?}", e);
                panic!("Failed to receive message.")
            }
            Ok(message) => match queue.complete_message(message.clone()) {
                Err(e) => {
                    println!("{:?}", e);
                    println!("{:?}", message);
                    panic!("Failed to complete the message");
                }
                _ => {}
            },
        }
    }

    #[test]
    fn queue_abandon_message() {
        let queue = QueueClient::with_conn_and_queue(&CONNECTION_STRING, "test1").unwrap();
        queue
            .send(BrokeredMessage::with_body("Complete this message"))
            .unwrap();
        match queue.receive() {
            Err(e) => {
                println!("{:?}", e);
                panic!("Failed to receive message.")
            }
            Ok(message) => match queue.abandon_message(message.clone()) {
                Err(e) => {
                    println!("{:?}", e);
                    println!("{:?}", message);
                    panic!("Failed to abandon the message");
                }
                _ => {}
            },
        }
    }

    #[test]
    fn queue_renew_message() {
        let queue = QueueClient::with_conn_and_queue(&CONNECTION_STRING, "test1").unwrap();
        queue
            .send(BrokeredMessage::with_body("Complete this message"))
            .unwrap();
        match queue.receive() {
            Err(e) => {
                println!("{:?}", e);
                panic!("Failed to receive message.")
            }
            Ok(message) => match queue.renew_message(message.clone()) {
                Err(e) => {
                    println!("{:?}", e);
                    println!("{:?}", message);
                    panic!("Failed to renew the message");
                }
                _ => {}
            },
        }
    }

    #[test]
    fn conncurrent_queue_send_message() {
        let queue = QueueClient::with_conn_and_queue(&CONNECTION_STRING, "test1").unwrap();
        let message = BrokeredMessage::with_body("Cats and Dogs");
        match queue.send(message) {
            Err(e) => {
                println!("{:?}", e);
                panic!("Failed to send message.");
            }
            _ => {}
        }
    }

    #[test]
    fn concurrent_queue_receive_message() {
        let queue = QueueClient::with_conn_and_queue(&CONNECTION_STRING, "test1").unwrap();
        match queue.receive_and_delete() {
            Err(e) => {
                println!("{:?}", e);
                panic!("Failed to receive a message");
            }
            Ok(_) => {}
        }
    }
}
*/
