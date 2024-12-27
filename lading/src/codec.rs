use std::io::Read;

use bytes::{Buf, Bytes};
use flate2::read::{MultiGzDecoder, ZlibDecoder};
use hyper::{body::{Body as HyperBody, BoxBody}, StatusCode};

/// decode decodes a HTTP request body based on its Content-Encoding header.
/// Only identity, gzip, and deflate are currently supported content encodings.
///
/// It supports multiple content encodings joined by ,s. They are decoded in the
/// order provided.
///
/// See [RFC7231](https://httpwg.org/specs/rfc7231.html#header.content-encoding) for more details
/// on this header value.
///
/// # Errors
///
/// Will return an `Err` including a `hyper` response body if:
///
/// * The passed `content_encoding` is unknown
/// * The body cannot be decoded as the specified content type
///
/// This response body can be passed back as the HTTP response to the client
pub(crate) fn decode(
    content_encoding: Option<&hyper::header::HeaderValue>,
    mut body: Bytes,
) -> Result<Bytes, hyper::Response<BoxBody>> {
    if let Some(content_encoding) = content_encoding {
        let content_encoding = String::from_utf8_lossy(content_encoding.as_bytes());

        for encoding in content_encoding
            .rsplit(',')
            .map(str::trim)
            .map(str::to_lowercase)
        {
            body = match encoding.as_ref() {
                "identity" => body,
                "gzip" => {
                    let mut decoded = Vec::new();
                    MultiGzDecoder::new(body.reader())
                        .read_to_end(&mut decoded)
                        .map_err(|error| encoding_error_to_response(&encoding, error))?;
                    decoded.into()
                }
                "deflate" => {
                    let mut decoded = Vec::new();
                    ZlibDecoder::new(body.reader())
                        .read_to_end(&mut decoded)
                        .map_err(|error| encoding_error_to_response(&encoding, error))?;
                    decoded.into()
                }
                "zstd" => {
                    let mut decoded = Vec::new();
                    zstd::Decoder::new(body.reader())
                        .map_err(|error| encoding_error_to_response(&encoding, error))?
                        .read_to_end(&mut decoded)
                        .map_err(|error| encoding_error_to_response(&encoding, error))?;

                    decoded.into()
                }
                encoding => {
                    return Err(hyper::Response::builder()
                        .status(StatusCode::UNSUPPORTED_MEDIA_TYPE)
                        .body(BoxBody::from(format!(
                            "Unsupported encoding type: {encoding}"
                        )))
                        .expect("failed to build response"))
                }
            }
        }
    }

    Ok(body)
}

fn encoding_error_to_response(
    encoding: &str,
    error: impl std::error::Error,
) -> hyper::Response<BoxBody> {
    hyper::Response::builder()
        .status(StatusCode::UNSUPPORTED_MEDIA_TYPE)
        .body(BoxBody::from(format!(
            "failed to decode input as {encoding}: {error}"
        )))
        .expect("failed to build response")
}
