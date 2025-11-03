//! Patterned payload generator for realistic log loads.
//!
//! This module provides a payload generator that creates logs with realistic
//! patterns including:
//! - Common entities (service names, user IDs, hostnames, etc.)
//! - Log templates with variable parts
//! - Temporal patterns and correlations
//!
//! This creates more realistic test loads compared to purely random data.

use std::io::Write;

use rand::{Rng, seq::IndexedRandom};
use serde::{Deserialize, Serialize};

use crate::Error;

/// Configuration for patterned log generation
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Number of unique service names to use
    #[serde(default = "default_service_count")]
    pub service_count: u16,

    /// Number of unique hostnames to use
    #[serde(default = "default_host_count")]
    pub host_count: u16,

    /// Number of unique user IDs to use
    #[serde(default = "default_user_count")]
    pub user_count: u16,

    /// Log format variant to use
    #[serde(default)]
    pub format: LogFormat,

    /// Whether to include trace/span IDs for correlation
    #[serde(default)]
    pub include_trace_ids: bool,
}

fn default_service_count() -> u16 {
    10
}
fn default_host_count() -> u16 {
    20
}
fn default_user_count() -> u16 {
    100
}

/// Log format variants
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    /// JSON structured logs
    #[default]
    Json,
    /// Logfmt key=value format
    Logfmt,
    /// Traditional syslog-style format
    Traditional,
}

/// Log level
#[derive(Debug, Clone, Copy)]
enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    fn as_str(self) -> &'static str {
        match self {
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO",
            LogLevel::Warn => "WARN",
            LogLevel::Error => "ERROR",
        }
    }

    fn random<R: Rng + ?Sized>(rng: &mut R) -> Self {
        // Weighted distribution: more info, fewer errors
        let val = rng.random_range(0..100);
        match val {
            0..=10 => LogLevel::Debug,
            11..=75 => LogLevel::Info,
            76..=93 => LogLevel::Warn,
            _ => LogLevel::Error,
        }
    }
}

/// Common log message templates
const MESSAGE_TEMPLATES: &[&str] = &[
    "Request completed",
    "Processing request",
    "Database query executed",
    "Cache hit",
    "Cache miss",
    "Authentication successful",
    "Authentication failed",
    "Connection established",
    "Connection closed",
    "File uploaded",
    "File downloaded",
    "User session started",
    "User session ended",
    "API call successful",
    "API call failed",
    "Configuration loaded",
    "Service started",
    "Service stopped",
    "Health check passed",
    "Health check failed",
    "Message queued",
    "Message processed",
    "Batch job started",
    "Batch job completed",
    "Retry attempt",
];

/// HTTP methods for realistic patterns
const HTTP_METHODS: &[&str] = &["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"];

/// HTTP status codes with realistic distribution
const HTTP_STATUS_CODES: &[(u16, u8)] = &[
    (200, 60), // 60% success
    (201, 10), // 10% created
    (204, 5),  // 5% no content
    (400, 8),  // 8% bad request
    (401, 5),  // 5% unauthorized
    (404, 7),  // 7% not found
    (500, 3),  // 3% server error
    (502, 1),  // 1% bad gateway
    (503, 1),  // 1% service unavailable
];

/// Patterned log payload generator
#[derive(Debug, Clone)]
pub struct Patterned {
    config: Config,
    services: Vec<String>,
    hosts: Vec<String>,
    users: Vec<String>,
    paths: Vec<String>,
    current_trace_id: Option<String>,
    trace_span_counter: u64,
}

impl Patterned {
    /// Create a new patterned payload generator
    pub fn new<R>(_rng: &mut R, config: Config) -> Self
    where
        R: rand::Rng + ?Sized,
    {
        // Generate common entities
        let services = (0..config.service_count)
            .map(|i| format!("service-{i}"))
            .collect();

        let hosts = (0..config.host_count)
            .map(|i| format!("host-{i}.example.com"))
            .collect();

        let users = (0..config.user_count)
            .map(|i| format!("user-{i:05}"))
            .collect();

        // Generate common API paths
        let paths = vec![
            "/api/v1/users".to_string(),
            "/api/v1/posts".to_string(),
            "/api/v1/comments".to_string(),
            "/api/v1/auth/login".to_string(),
            "/api/v1/auth/logout".to_string(),
            "/api/v1/orders".to_string(),
            "/api/v1/products".to_string(),
            "/api/v1/search".to_string(),
            "/api/v1/analytics".to_string(),
            "/health".to_string(),
            "/metrics".to_string(),
        ];

        Self {
            config,
            services,
            hosts,
            users,
            paths,
            current_trace_id: None,
            trace_span_counter: 0,
        }
    }

    /// Generate a trace ID
    fn generate_trace_id<R: Rng + ?Sized>(rng: &mut R) -> String {
        format!("{:032x}", rng.random::<u128>())
    }

    /// Generate a span ID
    fn generate_span_id<R: Rng + ?Sized>(rng: &mut R) -> String {
        format!("{:016x}", rng.random::<u64>())
    }

    /// Generate an HTTP status code with realistic distribution
    fn generate_status_code<R: Rng + ?Sized>(rng: &mut R) -> u16 {
        let total_weight: u32 = HTTP_STATUS_CODES.iter().map(|(_, w)| u32::from(*w)).sum();
        let mut value = rng.random_range(0..total_weight);

        for (code, weight) in HTTP_STATUS_CODES {
            if value < u32::from(*weight) {
                return *code;
            }
            value -= u32::from(*weight);
        }

        200 // fallback
    }

    /// Generate a single log line
    fn generate_log_line<R: Rng + ?Sized>(&mut self, rng: &mut R) -> Result<String, Error> {
        let level = LogLevel::random(rng);
        let service = self.services.choose(rng).ok_or(Error::StringGenerate)?;
        let host = self.hosts.choose(rng).ok_or(Error::StringGenerate)?;
        let user = self.users.choose(rng).ok_or(Error::StringGenerate)?;
        let message = MESSAGE_TEMPLATES.choose(rng).ok_or(Error::StringGenerate)?;
        let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

        // 20% chance to start a new trace
        if self.config.include_trace_ids
            && (self.current_trace_id.is_none() || rng.random_range(0..100) < 20)
        {
            self.current_trace_id = Some(Self::generate_trace_id(rng));
            self.trace_span_counter = 0;
        }

        let log = match self.config.format {
            LogFormat::Json => {
                let method = HTTP_METHODS.choose(rng).ok_or(Error::StringGenerate)?;
                let path = self.paths.choose(rng).ok_or(Error::StringGenerate)?;
                let status = Self::generate_status_code(rng);
                let duration_ms = rng.random_range(1..500);

                let mut json = serde_json::json!({
                    "timestamp": timestamp,
                    "level": level.as_str(),
                    "service": service,
                    "host": host,
                    "message": message,
                    "user_id": user,
                    "http": {
                        "method": method,
                        "path": path,
                        "status": status,
                        "duration_ms": duration_ms,
                    }
                });

                if let Some(ref trace_id) = self.current_trace_id {
                    self.trace_span_counter += 1;
                    if let Some(obj) = json.as_object_mut() {
                        obj.insert(
                            "trace_id".to_string(),
                            serde_json::Value::String(trace_id.clone()),
                        );
                        obj.insert(
                            "span_id".to_string(),
                            serde_json::Value::String(Self::generate_span_id(rng)),
                        );
                    }
                }

                serde_json::to_string(&json)?
            }
            LogFormat::Logfmt => {
                let method = HTTP_METHODS.choose(rng).ok_or(Error::StringGenerate)?;
                let path = self.paths.choose(rng).ok_or(Error::StringGenerate)?;
                let status = Self::generate_status_code(rng);
                let duration_ms = rng.random_range(1..500);

                let mut parts = vec![
                    format!("timestamp=\"{timestamp}\""),
                    format!("level={}", level.as_str()),
                    format!("service={service}"),
                    format!("host={host}"),
                    format!("message=\"{message}\""),
                    format!("user_id={user}"),
                    format!("method={method}"),
                    format!("path={path}"),
                    format!("status={status}"),
                    format!("duration_ms={duration_ms}"),
                ];

                if let Some(ref trace_id) = self.current_trace_id {
                    self.trace_span_counter += 1;
                    parts.push(format!("trace_id={trace_id}"));
                    parts.push(format!("span_id={}", Self::generate_span_id(rng)));
                }

                parts.join(" ")
            }
            LogFormat::Traditional => {
                format!(
                    "{} {} {}: {} (service={}, user={})",
                    timestamp,
                    host,
                    level.as_str(),
                    message,
                    service,
                    user
                )
            }
        };

        Ok(log)
    }
}

impl crate::Serialize for Patterned {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        R: Rng + Sized,
        W: Write,
    {
        let mut bytes_remaining = max_bytes;

        loop {
            let log_line = self.generate_log_line(&mut rng)?;
            let line_length = log_line.len() + 1; // add one for the newline

            match bytes_remaining.checked_sub(line_length) {
                Some(remainder) => {
                    writeln!(writer, "{log_line}")?;
                    bytes_remaining = remainder;
                }
                None => break,
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Serialize;
    use rand::{SeedableRng, rngs::SmallRng};

    #[test]
    fn test_patterned_generation() {
        let mut rng = SmallRng::seed_from_u64(42);
        let config = Config {
            service_count: 5,
            host_count: 3,
            user_count: 10,
            format: LogFormat::Json,
            include_trace_ids: true,
        };

        let mut patterned = Patterned::new(&mut rng, config);
        let mut buffer = Vec::new();

        patterned
            .to_bytes(rng, 1024, &mut buffer)
            .expect("should generate logs");
        assert!(!buffer.is_empty());
        assert!(buffer.len() <= 1024);
    }

    #[test]
    fn test_all_formats() {
        for format in [LogFormat::Json, LogFormat::Logfmt, LogFormat::Traditional] {
            let mut rng = SmallRng::seed_from_u64(42);
            let config = Config {
                service_count: 5,
                host_count: 3,
                user_count: 10,
                format,
                include_trace_ids: false,
            };

            let mut patterned = Patterned::new(&mut rng, config);
            let mut buffer = Vec::new();

            patterned
                .to_bytes(rng, 512, &mut buffer)
                .expect("should generate logs");
            assert!(!buffer.is_empty());
        }
    }
}
