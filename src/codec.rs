use bytes::{Buf, Bytes};
use flate2::read::{MultiGzDecoder, ZlibDecoder};
use hyper::{Body, StatusCode};
use std::io::Read;

pub fn decode(
    content_type: Option<&hyper::header::HeaderValue>,
    mut body: Bytes,
) -> Result<Bytes, hyper::Response<hyper::Body>> {
    if let Some(content_type) = content_type {
        let content_type = String::from_utf8_lossy(content_type.as_bytes());

        for encoding in content_type.rsplit(',').map(str::trim) {
            body = match encoding {
                "identity" => body,
                "gzip" => {
                    let mut decoded = Vec::new();
                    MultiGzDecoder::new(body.reader())
                        .read_to_end(&mut decoded)
                        .map_err(|error| encoding_error_to_response(encoding, error))?;
                    decoded.into()
                }
                "deflate" => {
                    let mut decoded = Vec::new();
                    ZlibDecoder::new(body.reader())
                        .read_to_end(&mut decoded)
                        .map_err(|error| encoding_error_to_response(encoding, error))?;
                    decoded.into()
                }
                encoding => {
                    return Err(hyper::Response::builder()
                        .status(StatusCode::UNSUPPORTED_MEDIA_TYPE)
                        .body(hyper::Body::from(format!(
                            "Unsupported encoding type: {}",
                            encoding
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
) -> hyper::Response<Body> {
    hyper::Response::builder()
        .status(StatusCode::UNSUPPORTED_MEDIA_TYPE)
        .body(hyper::Body::from(format!(
            "failed to decode input as {}: {}",
            encoding, error
        )))
        .expect("failed to build response")
}
