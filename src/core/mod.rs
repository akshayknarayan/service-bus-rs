pub mod error;

use crypto::hmac::Hmac;
use crypto::mac::Mac;
use crypto::sha2::*;
use percent_encoding::utf8_percent_encode;

// space, double quote ("), hash (#), inequality qualifiers (<), (>), backtick (`), question mark (?),
// and curly brackets ({), (}), forward slash (/), colon (:), semi-colon (;), equality
// (=), at (@), backslash (\), square brackets ([), (]), caret (^), and pipe (|)
const USERINFO_ENCODE_SET: percent_encoding::AsciiSet = percent_encoding::CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'<')
    .add(b'>')
    .add(b'`')
    .add(b'?')
    .add(b'{')
    .add(b'}')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'=')
    .add(b'@')
    .add(b'\\')
    .add(b'[')
    .add(b']')
    .add(b'^')
    .add(b'|');

const CUSTOM_ENCODE_SET: percent_encoding::AsciiSet =
    percent_encoding::CONTROLS.add(b'+').add(b'=').add(b'/');

/// This function generates an SAS token for authenticating into azure
/// using the connection string provided on portal.azure.com.
/// It will not raise an error if there is a mistake in the connection string,
/// but the token will be invalid.
pub fn generate_sas(connection_string: &str, duration: std::time::Duration) -> (String, usize) {
    let mut key = "";
    let mut endpoint = "";
    let mut name = "";

    let params = connection_string.split(";");
    for param in params {
        let idx = param.find("=").unwrap_or(0);
        let (mut k, mut value) = param.split_at(idx);
        k = k.trim();
        value = value.trim();
        // cut out the equal sign if there was one.
        if value.len() > 0 {
            value = &value[1..]
        }
        match k {
            "Endpoint" => endpoint = value,
            "SharedAccessKey" => key = value,
            "SharedAccessKeyName" => name = value,
            _ => {}
        };
    }

    let mut h = Hmac::new(Sha256::new(), key.as_bytes());

    let encoded_url = utf8_percent_encode(endpoint, &USERINFO_ENCODE_SET).collect::<String>();
    let expiry = (std::time::SystemTime::now() + duration)
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap_or(std::time::Duration::from_secs(0))
        .as_secs();

    let message = format!("{}\n{}", encoded_url, expiry);
    h.input(message.as_bytes());

    let mut sig = base64::encode(h.result().code());
    sig = utf8_percent_encode(&sig, &CUSTOM_ENCODE_SET).collect::<String>();

    let sas = format!(
        "SharedAccessSignature sig={}&se={}&skn={}&sr={}",
        sig, expiry, name, encoded_url
    );

    (sas, expiry as usize)
}
