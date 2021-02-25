use super::brokeredmessage::*;
use crate::core::error::AzureRequestError;
use crate::core::generate_sas;
use eyre::Report;
use hyper::header::*;
use hyper::{Request, Uri};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

const SAS_BUFFER_TIME: usize = 15;

/// Client for sending and receiving messages from a Service Bus Subscription in Azure.
/// This cient is `!Sync` because it internally uses a RefCell to keep track of
/// its authorization token, but it is still ideal for single threaded use.
pub struct SubscriptionClient {
    connection_string: String,
    topic_name: String,
    subscription_name: String,
    endpoint: Uri,
    sas_info: Arc<Mutex<(String, usize)>>,
}

/// The Subscription Trait is an abstraction over different types of Subscription that
/// can be used when communicating with an Azure Service Bus Queue.
///
/// Topics and subscriptions work together hand in hand. Together they provide similar functionality
/// to queues. Producers send message to the topic. Consumers then create a subscription to the
/// topic to receive every messages. Each subscription functions as an individual queue. This is useful
/// when the same message will be read multiple times for different reasons. An example might be
/// adding logging to the Service bus. One subscription might be used to provide load balancing for
/// servers as described in the Queue page. Another subscription will log every message as they come in
/// in a different subscription. This way different processes can consume the message and not interfere
/// with each other or have to worry about losing messages.
impl SubscriptionClient {
    /// Create a new subscription with a connection string, the name of a
    /// topic, and the name of a subscription.
    /// The connection string can be copied and pasted from the azure portal.
    /// The subscription name should be the name of an existing subscription.
    pub fn with_conn_topic_and_subscr(
        connection_string: &str,
        topic: &str,
        subscription: &str,
    ) -> Result<SubscriptionClient, Report> {
        let duration = Duration::from_secs(60 * 6);
        let mut endpoint = String::new();
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
                "Endpoint" => endpoint = value.to_string(),
                _ => {}
            };
        }
        endpoint = String::new() + "https" + endpoint.split_at(endpoint.find(":").unwrap_or(0)).1;
        let url = endpoint.parse()?;

        let (sas_key, expiry) = generate_sas(connection_string, duration);
        let conn_string = connection_string.to_string();

        Ok(SubscriptionClient {
            connection_string: conn_string,
            subscription_name: subscription.to_string(),
            topic_name: topic.to_string(),
            endpoint: url,
            sas_info: Arc::new(Mutex::new((sas_key, expiry - SAS_BUFFER_TIME))),
        })
    }

    pub fn subscription(&self) -> &str {
        &self.subscription_name
    }

    pub fn topic(&self) -> &str {
        &self.topic_name
    }

    /// The endpoint for the Queue. `http://{namespace}.servicebus.net/`
    pub fn endpoint(&self) -> &Uri {
        &self.endpoint
    }

    /// Receive a message from the subscription. Returns either the deserialized message or an error
    /// detailing what went wrong. The message will not be deleted on the server until
    /// `queue_client.complete_message(message)` is called. This is ideal for applications that
    /// can't afford to miss a message.
    pub fn receive(&self) -> Result<Request<()>, Report> {
        let timeout = Duration::from_secs(30);
        self.receive_with_timeout(timeout)
    }

    /// Receive a message from the subscription. Returns the deserialized message or an error.
    /// The message is deleted from the subscription when it is received. If the application crashes,
    /// the contents of the message can be lost.
    pub fn receive_and_delete(&self) -> Result<Request<()>, Report> {
        let timeout = Duration::from_secs(30);
        self.receive_and_delete_with_timeout(timeout)
    }

    /// Receive a message from the subscription. Returns the deserialized message or an error.
    /// The message is deleted from the subscription when it is received. If the application crashes,
    /// the contents of the message can be lost.
    pub fn receive_and_delete_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Request<()>, Report> {
        let sas = self.refresh_sas();

        let mut parts = self.endpoint().clone().into_parts();
        parts.path_and_query = Some(
            format!(
                "{}/subscriptions/{}/messages/head?timeout={}",
                self.topic(),
                self.subscription(),
                timeout.as_secs()
            )
            .parse()?,
        );
        let uri = Uri::from_parts(parts)?;

        Ok(Request::delete(uri).header(AUTHORIZATION, sas).body(())?)
    }

    /// Receive a message from the subscription. Returns either the deserialized message or an error
    /// detailing what went wrong. The message will not be deleted on the server until
    /// `subscription_client.complete_message(message)` is called. This is ideal for applications that
    /// can't afford to miss a message. Allows a timeout to be specified for greater control.
    pub fn receive_with_timeout(&self, timeout: Duration) -> Result<Request<()>, Report> {
        let sas = self.refresh_sas();
        let mut parts = self.endpoint().clone().into_parts();
        parts.path_and_query = Some(
            format!(
                "{}/subscriptions/{}/messages/head?timeout={}",
                self.topic(),
                self.subscription(),
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
    /// let message = my_subscription.receive().unwrap();
    /// // Do lots of processing with the message. Send it to another database.
    /// my_subscription.complete_message(message);
    /// ```
    pub fn complete_message(&self, message: BrokeredMessage) -> Result<Request<()>, Report> {
        let sas = self.refresh_sas();
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
    /// `subscription.receive()` or `subscription.receive_with_timeout()` then the message is locked
    /// but not deleted on the Service Bus. This method allows the lock to be renewed
    /// if additional time is needed to finish processing the message.
    ///
    /// ```
    /// use std::thread::sleep;
    ///
    /// let message = subscription.receive();
    /// sleep(2*60*1000);
    /// //Renew the lock on the message so that we can keep processing it.
    /// subscription.renew_message(message);
    /// sleep(2*60*1000);
    /// subscription.complete_message(message);
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
        message
            .props
            .SequenceNumber
            .map(|seq| seq.to_string())
            .or(message.props.MessageId.clone())
            .and_then(|id| message.props.LockToken.as_ref().map(|lock| (id, lock)))
            .map(|(id, lock)| {
                format!(
                    "{}/subscriptions/{}/messages/{}/{}",
                    self.topic(),
                    self.subscription(),
                    id,
                    lock
                )
            })
            .and_then(|path| {
                let mut parts = self.endpoint().clone().into_parts();
                parts.path_and_query = Some(path.parse().ok()?);
                Uri::from_parts(parts).ok()
            })
            .ok_or(AzureRequestError::LocalMessage)
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
