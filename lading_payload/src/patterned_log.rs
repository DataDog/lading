//! Pattern-driven Datadog log payload.
//!
//! This payload focuses on producing realistic looking log lines by
//! constraining randomness to a handful of common patterns. Repeated
//! structures help downstream systems surface aggregation behaviour that can
//! be masked by fully random payloads.

use std::io::Write;

use rand::{Rng, seq::SliceRandom};
use serde::{Deserialize, Serialize};

use crate::{Error, Generator};

use crate::datadog_logs::{Member, Message};

const HOSTS: &[&str] = &[
    "ingest-api-01",
    "ingest-api-02",
    "logs-worker-01",
    "logs-worker-02",
    "edge-router-01",
    "edge-router-02",
];

const METHODS: &[&str] = &["GET", "POST", "PUT"];
const HTTP_PATHS: &[&str] = &[
    "/api/v1/logs",
    "/api/v1/validate",
    "/api/v2/input",
    "/v1/intake",
];
const ROUTES: &[&str] = &[
    "logs.bulk",
    "logs.single",
    "logs.replay",
    "logs.archive",
];
const USERS: &[&str] = &["alice", "bob", "carol", "dave", "erin", "mallory"];
const INTEGRATIONS: &[&str] = &["aws", "gcp", "azure", "kubernetes", "custom"];
const TABLES: &[&str] = &["events", "logs", "pipelines", "signals"];
const REGIONS: &[&str] = &["us-east-1", "us-west-2", "eu-central-1"];
const MODULES: &[&str] = &["ingest", "normalizer", "pipeline", "shipper"];
const ERROR_REASONS: &[&str] = &[
    "upstream_timeout",
    "db_deadlock",
    "parser_failure",
    "queue_full",
];

#[derive(Debug, Copy, Clone)]
enum Level {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Copy, Clone)]
struct Template {
    weight: u16,
    status: &'static str,
    service: &'static str,
    ddsource: &'static str,
    tags: &'static str,
    kind: TemplateKind,
}

#[derive(Debug, Copy, Clone)]
enum TemplateKind {
    HttpSuccess,
    PipelineDelivery,
    AuthSuccess,
    Heartbeat,
    SlowQuery,
    RetryQueueDepth,
    RateLimitApproach,
    HttpError,
    DownstreamTimeout,
    DroppedLog,
}

const INFO_TEMPLATES: &[Template] = &[
    Template {
        weight: 6,
        status: "info",
        service: "ingest-api",
        ddsource: "nginx",
        tags: "env:prod,team:edge,region:us-east-1",
        kind: TemplateKind::HttpSuccess,
    },
    Template {
        weight: 4,
        status: "info",
        service: "pipeline-worker",
        ddsource: "vector",
        tags: "env:prod,team:pipeline,region:us-east-1",
        kind: TemplateKind::PipelineDelivery,
    },
    Template {
        weight: 3,
        status: "info",
        service: "auth-proxy",
        ddsource: "auth",
        tags: "env:prod,team:edge,region:us-west-2",
        kind: TemplateKind::AuthSuccess,
    },
    Template {
        weight: 2,
        status: "info",
        service: "pipeline-scheduler",
        ddsource: "scheduler",
        tags: "env:prod,team:pipeline,region:us-east-1",
        kind: TemplateKind::Heartbeat,
    },
];

const WARN_TEMPLATES: &[Template] = &[
    Template {
        weight: 4,
        status: "warning",
        service: "pipeline-worker",
        ddsource: "vector",
        tags: "env:prod,team:pipeline,region:us-east-1",
        kind: TemplateKind::SlowQuery,
    },
    Template {
        weight: 3,
        status: "warning",
        service: "integration-poller",
        ddsource: "poller",
        tags: "env:prod,team:integrations,region:us-east-1",
        kind: TemplateKind::RetryQueueDepth,
    },
    Template {
        weight: 2,
        status: "warning",
        service: "edge-router",
        ddsource: "nginx",
        tags: "env:prod,team:edge,region:eu-central-1",
        kind: TemplateKind::RateLimitApproach,
    },
];

const ERROR_TEMPLATES: &[Template] = &[
    Template {
        weight: 3,
        status: "error",
        service: "ingest-api",
        ddsource: "nginx",
        tags: "env:prod,team:edge,region:us-east-1",
        kind: TemplateKind::HttpError,
    },
    Template {
        weight: 2,
        status: "error",
        service: "pipeline-worker",
        ddsource: "vector",
        tags: "env:prod,team:pipeline,region:us-east-1",
        kind: TemplateKind::DownstreamTimeout,
    },
    Template {
        weight: 1,
        status: "error",
        service: "logs-normalizer",
        ddsource: "normalizer",
        tags: "env:prod,team:pipeline,region:us-west-2",
        kind: TemplateKind::DroppedLog,
    },
];

fn choose_template<'a, R>(rng: &mut R, templates: &'a [Template]) -> &'a Template
where
    R: Rng + ?Sized,
{
    let total_weight: u32 = templates
        .iter()
        .map(|template| u32::from(template.weight))
        .sum();
    debug_assert!(total_weight > 0, "template weights must be positive");
    let mut ticket = rng.random_range(0..total_weight);
    for template in templates {
        let weight = u32::from(template.weight);
        if weight == 0 {
            continue;
        }
        if ticket < weight {
            return template;
        }
        ticket -= weight;
    }
    templates
        .last()
        .expect("template list must contain at least one entry")
}

fn choose_level<R>(rng: &mut R, weights: LevelWeights) -> Level
where
    R: Rng + ?Sized,
{
    let components = [
        (Level::Info, u32::from(weights.info)),
        (Level::Warn, u32::from(weights.warn)),
        (Level::Error, u32::from(weights.error)),
    ];

    let total_weight: u32 = components.iter().map(|(_, weight)| *weight).sum();
    debug_assert!(total_weight > 0, "level weights must be positive");

    let mut ticket = rng.random_range(0..total_weight);
    for (level, weight) in components {
        if weight == 0 {
            continue;
        }
        if ticket < weight {
            return level;
        }
        ticket -= weight;
    }
    Level::Error
}

fn render_message<R>(kind: TemplateKind, rng: &mut R) -> String
where
    R: Rng + ?Sized,
{
    match kind {
        TemplateKind::HttpSuccess => {
            let method = METHODS
                .choose(rng)
                .expect("methods collection must not be empty");
            let path = HTTP_PATHS
                .choose(rng)
                .expect("paths collection must not be empty");
            let route = ROUTES
                .choose(rng)
                .expect("routes collection must not be empty");
            let latency = rng.random_range(12..250);
            let body_bytes = rng.random_range(1_200..5_500);
            let request_id = rng.random_range(1_048_576..1_049_600);
            format!(
                "{method} {path} route={route} status=200 latency_ms={latency} body_bytes={body_bytes} request_id={request_id:08X}"
            )
        }
        TemplateKind::PipelineDelivery => {
            let shard = rng.random_range(1..=8);
            let batch = rng.random_range(64..=192);
            let ack_ms = rng.random_range(45..=180);
            let records = rng.random_range(900..=1_800);
            format!(
                "delivered batch shard={shard} batch_size={batch} records={records} ack_ms={ack_ms}"
            )
        }
        TemplateKind::AuthSuccess => {
            let user = USERS
                .choose(rng)
                .expect("users collection must not be empty");
            let method = METHODS
                .choose(rng)
                .expect("methods collection must not be empty");
            let path = "/session/login";
            let request_id = rng.random_range(2_097_152..2_097_600);
            format!(
                "authenticated user={user} method={method} path={path} request_id={request_id:08X}"
            )
        }
        TemplateKind::Heartbeat => {
            let module = MODULES
                .choose(rng)
                .expect("modules collection must not be empty");
            let queue_depth = rng.random_range(1_200..=2_400);
            let lag = rng.random_range(0..=12);
            format!(
                "heartbeat module={module} queue_depth={queue_depth} lag_seconds={lag}"
            )
        }
        TemplateKind::SlowQuery => {
            let table = TABLES
                .choose(rng)
                .expect("tables collection must not be empty");
            let duration = rng.random_range(350..=950);
            let scans = rng.random_range(4..=16);
            format!(
                "slow query table={table} duration_ms={duration} scans={scans}"
            )
        }
        TemplateKind::RetryQueueDepth => {
            let integration = INTEGRATIONS
                .choose(rng)
                .expect("integrations collection must not be empty");
            let queue = rng.random_range(4_500..=8_000);
            let retries = rng.random_range(2..=6);
            format!(
                "retry queue depth warning integration={integration} size={queue} retries={retries}"
            )
        }
        TemplateKind::RateLimitApproach => {
            let account = rng.random_range(1_000..=1_100);
            let percent = rng.random_range(82..=97);
            let region = REGIONS
                .choose(rng)
                .expect("regions collection must not be empty");
            format!(
                "rate limit nearing threshold account={account} percent_of_limit={percent} region={region}"
            )
        }
        TemplateKind::HttpError => {
            let status_code = [500_u16, 502_u16, 503_u16]
                .choose(rng)
                .copied()
                .expect("status code list must not be empty");
            let route = ROUTES
                .choose(rng)
                .expect("routes collection must not be empty");
            let request_id = rng.random_range(4_194_304..4_195_328);
            let latency = rng.random_range(250..=1_000);
            format!(
                "upstream failure status={status_code} route={route} latency_ms={latency} request_id={request_id:08X}"
            )
        }
        TemplateKind::DownstreamTimeout => {
            let integration = INTEGRATIONS
                .choose(rng)
                .expect("integrations collection must not be empty");
            let duration = rng.random_range(1_200..=2_400);
            let attempt = rng.random_range(2..=5);
            format!(
                "downstream timeout integration={integration} attempt={attempt} duration_ms={duration}"
            )
        }
        TemplateKind::DroppedLog => {
            let reason = ERROR_REASONS
                .choose(rng)
                .expect("error reason list must not be empty");
            let correlation = rng.random_range(8_388_608..8_389_120);
            let bytes = rng.random_range(6_144..=9_216);
            format!(
                "dropped log reason={reason} bytes={bytes} correlation_id={correlation:08X}"
            )
        }
    }
}

fn select_hostname<R>(rng: &mut R) -> &'static str
where
    R: Rng + ?Sized,
{
    HOSTS
        .choose(rng)
        .copied()
        .expect("host list must not be empty")
}

fn select_tags<R>(tags: &'static str, rng: &mut R) -> &'static str
where
    R: Rng + ?Sized,
{
    if rng.random_range(0..100) < 20 {
        "env:prod,team:edge,customer:beta"
    } else {
        tags
    }
}

fn timestamp_with_jitter<R>(config: Config, rng: &mut R) -> u32
where
    R: Rng + ?Sized,
{
    if config.timestamp_jitter_seconds == 0 {
        config.starting_timestamp
    } else {
        let jitter = rng.random_range(0..=u32::from(config.timestamp_jitter_seconds));
        config.starting_timestamp.saturating_add(jitter)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct LevelWeights {
    /// Relative frequency for informational messages.
    pub info: u16,
    /// Relative frequency for warning messages.
    pub warn: u16,
    /// Relative frequency for error messages.
    pub error: u16,
}

impl LevelWeights {
    const fn total(self) -> u32 {
        u32::from(self.info) + u32::from(self.warn) + u32::from(self.error)
    }
}

impl Default for LevelWeights {
    fn default() -> Self {
        Self {
            info: 85,
            warn: 10,
            error: 5,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Config {
    /// Severity mix to use when selecting templates.
    pub level_weights: LevelWeights,
    /// Starting timestamp used as the base for generated events.
    pub starting_timestamp: u32,
    /// Maximum jitter, in seconds, that is added to `starting_timestamp`.
    pub timestamp_jitter_seconds: u16,
}

impl Config {
    fn validate(self) -> Result<(), Error> {
        if self.level_weights.total() == 0 {
            return Err(Error::Validation(
                "`level_weights` must contain at least one non-zero entry".to_string(),
            ));
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            level_weights: LevelWeights::default(),
            starting_timestamp: 1_700_000_000,
            timestamp_jitter_seconds: 120,
        }
    }
}

#[derive(Debug)]
pub struct PatternedLog {
    config: Config,
}

impl PatternedLog {
    /// Create a new [`PatternedLog`] serializer.
    pub fn new(config: Config) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self { config })
    }
}

impl<'a> Generator<'a> for PatternedLog {
    type Output = Member<'a>;
    type Error = Error;

    fn generate<R>(&'a self, rng: &mut R) -> Result<Self::Output, Error>
    where
        R: Rng + ?Sized,
    {
        let level = choose_level(rng, self.config.level_weights);
        let template = match level {
            Level::Info => choose_template(rng, INFO_TEMPLATES),
            Level::Warn => choose_template(rng, WARN_TEMPLATES),
            Level::Error => choose_template(rng, ERROR_TEMPLATES),
        };

        let hostname = select_hostname(rng);
        let ddtags = select_tags(template.tags, rng);
        let message = render_message(template.kind, rng);
        let timestamp = timestamp_with_jitter(self.config, rng);

        Ok(Member {
            message: Message::Structured(message),
            status: template.status,
            timestamp,
            hostname,
            service: template.service,
            ddsource: template.ddsource,
            ddtags,
        })
    }
}

impl crate::Serialize for PatternedLog {
    fn to_bytes<W, R>(&mut self, mut rng: R, max_bytes: usize, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
        R: Rng + Sized,
    {
        const APPROX_MEMBER_ENCODED_SIZE: usize = 256;

        if max_bytes < APPROX_MEMBER_ENCODED_SIZE {
            return Ok(());
        }

        let cap = (max_bytes / APPROX_MEMBER_ENCODED_SIZE) + 64;
        let mut members: Vec<Member> = Vec::with_capacity(cap);
        for _ in 0..cap {
            members.push(self.generate(&mut rng)?);
        }

        let mut high = members.len();
        while high != 0 {
            let encoding = serde_json::to_string(&members[0..high])?;
            if encoding.len() > max_bytes {
                high /= 2;
            } else {
                writer.write_all(encoding.as_bytes())?;
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::SmallRng};

    use super::*;

    proptest! {
        #[test]
        fn payload_not_exceed_max_bytes(seed: u64, max_bytes: u16) {
            let max_bytes = max_bytes as usize;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut serializer = PatternedLog::new(Config::default()).expect("valid config");

            let mut bytes = Vec::with_capacity(max_bytes);
            serializer.to_bytes(rng, max_bytes, &mut bytes).expect("failed to serialize");
            prop_assert!(bytes.len() <= max_bytes);
        }
    }

    proptest! {
        #[test]
        fn every_payload_deserializes(seed: u64, max_bytes: u16) {
            let max_bytes = (max_bytes as usize).max(1);
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut serializer = PatternedLog::new(Config::default()).expect("valid config");

            let mut bytes: Vec<u8> = Vec::with_capacity(max_bytes);
            serializer.to_bytes(rng, max_bytes, &mut bytes).expect("failed to serialize");

            if bytes.is_empty() {
                return Ok(());
            }

            let payload = std::str::from_utf8(&bytes).expect("payload is utf-8");
            let entries: Vec<Member> = serde_json::from_str(payload).expect("json deserializes");
            prop_assert!(entries.len() <= max_bytes / 64);
        }
    }
}