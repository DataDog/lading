use serde::Serialize;
use serde_yaml::Value;

use crate::variants::VariantKind;

// ---------------------------------------------------------------------------
// Blackhole types
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BlackholeKind {
    Http,
    Tcp,
    Udp,
}

impl BlackholeKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
    pub fn next(self) -> Self {
        match self {
            Self::Http => Self::Tcp,
            Self::Tcp => Self::Udp,
            Self::Udp => Self::Http,
        }
    }
    pub fn prev(self) -> Self {
        match self {
            Self::Http => Self::Udp,
            Self::Tcp => Self::Http,
            Self::Udp => Self::Tcp,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BlackholeEntry {
    pub kind: BlackholeKind,
    pub addr: String,
}

// ---------------------------------------------------------------------------
// Mirror types — match lading's YAML structure without importing private types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct LadingConfig {
    pub generator: Vec<GeneratorEntry>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub blackhole: Vec<Value>,
}

#[derive(Serialize)]
pub struct GeneratorEntry {
    pub file_gen: FileGenEntry,
}

#[derive(Serialize)]
pub struct FileGenEntry {
    pub logrotate_fs: LogrotateFsConfig,
}

#[derive(Serialize)]
pub struct LogrotateFsConfig {
    pub seed: Vec<u8>,
    pub concurrent_logs: u16,
    pub maximum_bytes_per_log: String,
    pub total_rotations: u8,
    pub max_depth: u8,
    pub variant: Value,
    pub maximum_prebuild_cache_size_bytes: String,
    pub maximum_block_size: String,
    pub mount_point: String,
    pub load_profile: Value,
}

// ---------------------------------------------------------------------------
// Load profile kind
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LoadProfileKind {
    Constant,
    Linear,
}

// ---------------------------------------------------------------------------
// Value builders
// ---------------------------------------------------------------------------

pub fn build_variant_value(
    kind: VariantKind,
    splunk_enc: usize, // 0 = text, 1 = json
    static_path: &str,
    template_path: &str,
) -> Value {
    match kind {
        VariantKind::Ascii => Value::String("ascii".into()),
        VariantKind::Json => Value::String("json".into()),
        VariantKind::ApacheCommon => Value::String("apache_common".into()),
        VariantKind::Syslog5424 => Value::String("syslog5424".into()),
        VariantKind::DatadogLog => Value::String("datadog_log".into()),
        VariantKind::SplunkHec => {
            let enc = if splunk_enc == 0 { "text" } else { "json" };
            let mut inner = serde_yaml::Mapping::new();
            inner.insert("encoding".into(), enc.into());
            let mut outer = serde_yaml::Mapping::new();
            outer.insert("splunk_hec".into(), Value::Mapping(inner));
            Value::Mapping(outer)
        }
        VariantKind::TemplatedJson => {
            let mut inner = serde_yaml::Mapping::new();
            inner.insert("template_path".into(), template_path.into());
            let mut outer = serde_yaml::Mapping::new();
            outer.insert("templated_json".into(), Value::Mapping(inner));
            Value::Mapping(outer)
        }
        VariantKind::Static => {
            let mut inner = serde_yaml::Mapping::new();
            inner.insert("static_path".into(), static_path.into());
            let mut outer = serde_yaml::Mapping::new();
            outer.insert("static".into(), Value::Mapping(inner));
            Value::Mapping(outer)
        }
        VariantKind::StaticChunks => {
            let mut inner = serde_yaml::Mapping::new();
            inner.insert("static_path".into(), static_path.into());
            let mut outer = serde_yaml::Mapping::new();
            outer.insert("static_chunks".into(), Value::Mapping(inner));
            Value::Mapping(outer)
        }
    }
}

pub fn build_load_profile_value(
    kind: LoadProfileKind,
    constant_rate: &str,
    linear_initial: &str,
    linear_rate: &str,
) -> Value {
    let mut map = serde_yaml::Mapping::new();
    match kind {
        LoadProfileKind::Constant => {
            map.insert("constant".into(), constant_rate.into());
        }
        LoadProfileKind::Linear => {
            let mut inner = serde_yaml::Mapping::new();
            inner.insert("initial".into(), linear_initial.into());
            inner.insert("rate".into(), linear_rate.into());
            map.insert("linear".into(), Value::Mapping(inner));
        }
    }
    Value::Mapping(map)
}

pub fn build_blackhole_entry_value(entry: &BlackholeEntry) -> Value {
    let mut inner = serde_yaml::Mapping::new();
    inner.insert("binding_addr".into(), entry.addr.clone().into());
    let mut outer = serde_yaml::Mapping::new();
    outer.insert(entry.kind.label().into(), Value::Mapping(inner));
    Value::Mapping(outer)
}

pub fn build_yaml(
    seed: &[u8; 32],
    concurrent_logs: u16,
    max_bytes_per_log: &str,
    total_rotations: u8,
    max_depth: u8,
    mount_point: &str,
    max_prebuild_cache: &str,
    max_block_size: &str,
    variant: Value,
    load_profile: Value,
    blackhole_entries: &[BlackholeEntry],
) -> String {
    let logrotate_fs = LogrotateFsConfig {
        seed: seed.to_vec(),
        concurrent_logs,
        maximum_bytes_per_log: max_bytes_per_log.to_string(),
        total_rotations,
        max_depth,
        variant,
        maximum_prebuild_cache_size_bytes: max_prebuild_cache.to_string(),
        maximum_block_size: max_block_size.to_string(),
        mount_point: mount_point.to_string(),
        load_profile,
    };
    let blackhole: Vec<Value> = blackhole_entries
        .iter()
        .map(build_blackhole_entry_value)
        .collect();
    let config = LadingConfig {
        generator: vec![GeneratorEntry {
            file_gen: FileGenEntry { logrotate_fs },
        }],
        blackhole,
    };
    let raw = serde_yaml::to_string(&config).unwrap_or_else(|e| format!("# YAML error: {e}"));
    // serde_yaml serialises Vec<u8> as a block sequence by default; rewrite to flow style.
    seed_to_flow_style(&raw, seed)
}

// ---------------------------------------------------------------------------
// Import — parse a lading YAML back into editable fields
// ---------------------------------------------------------------------------

pub struct ImportedFields {
    pub seed: Option<[u8; 32]>,
    pub concurrent_logs: Option<u16>,
    pub max_bytes_per_log: Option<String>,
    pub total_rotations: Option<u8>,
    pub max_depth: Option<u8>,
    pub mount_point: Option<String>,
    pub variant_kind: Option<VariantKind>,
    pub splunk_enc: Option<usize>, // 0 = text, 1 = json
    pub static_path: Option<String>,
    pub template_path: Option<String>,
    pub load_profile_kind: Option<LoadProfileKind>,
    pub constant_rate: Option<String>,
    pub linear_initial: Option<String>,
    pub linear_rate: Option<String>,
    pub max_prebuild_cache: Option<String>,
    pub max_block_size: Option<String>,
    pub blackhole_entries: Option<Vec<BlackholeEntry>>,
}

/// Parse a lading YAML string into `ImportedFields`.
/// Missing fields produce `None`; parse errors return `Err(description)`.
pub fn parse_config(yaml: &str) -> Result<ImportedFields, String> {
    let root: Value = serde_yaml::from_str(yaml).map_err(|e| format!("YAML parse error: {e}"))?;

    // Navigate: generator[0].file_gen.logrotate_fs
    let lfs = root
        .get("generator")
        .and_then(|v| v.get(0))
        .and_then(|v| v.get("file_gen"))
        .and_then(|v| v.get("logrotate_fs"))
        .ok_or_else(|| "Missing generator[0].file_gen.logrotate_fs".to_string())?;

    // seed: either a flow sequence [n, …] or block sequence
    let seed = lfs.get("seed").and_then(|v| {
        let items: Vec<u64> = match v {
            Value::Sequence(seq) => seq.iter().filter_map(|x| x.as_u64()).collect(),
            _ => return None,
        };
        if items.len() == 32 {
            let mut arr = [0u8; 32];
            for (i, &val) in items.iter().enumerate() {
                arr[i] = val as u8;
            }
            Some(arr)
        } else {
            None
        }
    });

    let concurrent_logs = lfs
        .get("concurrent_logs")
        .and_then(|v| v.as_u64())
        .and_then(|n| u16::try_from(n).ok());

    let max_bytes_per_log = lfs
        .get("maximum_bytes_per_log")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let total_rotations = lfs
        .get("total_rotations")
        .and_then(|v| v.as_u64())
        .and_then(|n| u8::try_from(n).ok());

    let max_depth = lfs
        .get("max_depth")
        .and_then(|v| v.as_u64())
        .and_then(|n| u8::try_from(n).ok());

    let mount_point = lfs
        .get("mount_point")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let max_prebuild_cache = lfs
        .get("maximum_prebuild_cache_size_bytes")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let max_block_size = lfs
        .get("maximum_block_size")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // variant: either a bare string or a mapping {splunk_hec: {encoding}, static: {static_path}, …}
    let variant_val = lfs.get("variant");
    let mut variant_kind: Option<VariantKind> = None;
    let mut splunk_enc: Option<usize> = None;
    let mut static_path: Option<String> = None;
    let mut template_path: Option<String> = None;

    if let Some(v) = variant_val {
        match v {
            Value::String(s) => {
                variant_kind = match s.as_str() {
                    "ascii" => Some(VariantKind::Ascii),
                    "json" => Some(VariantKind::Json),
                    "apache_common" => Some(VariantKind::ApacheCommon),
                    "syslog5424" => Some(VariantKind::Syslog5424),
                    "datadog_log" => Some(VariantKind::DatadogLog),
                    _ => None,
                };
            }
            Value::Mapping(m) => {
                if let Some(inner) = m.get("splunk_hec") {
                    variant_kind = Some(VariantKind::SplunkHec);
                    if let Some(enc) = inner.get("encoding").and_then(|e| e.as_str()) {
                        splunk_enc = Some(if enc == "text" { 0 } else { 1 });
                    }
                } else if let Some(inner) = m.get("static") {
                    variant_kind = Some(VariantKind::Static);
                    static_path = inner
                        .get("static_path")
                        .and_then(|p| p.as_str())
                        .map(|s| s.to_string());
                } else if let Some(inner) = m.get("static_chunks") {
                    variant_kind = Some(VariantKind::StaticChunks);
                    static_path = inner
                        .get("static_path")
                        .and_then(|p| p.as_str())
                        .map(|s| s.to_string());
                } else if let Some(inner) = m.get("templated_json") {
                    variant_kind = Some(VariantKind::TemplatedJson);
                    template_path = inner
                        .get("template_path")
                        .and_then(|p| p.as_str())
                        .map(|s| s.to_string());
                }
            }
            _ => {}
        }
    }

    // load_profile: {constant: "1MiB"} or {linear: {initial: "1MiB", rate: "100KiB"}}
    let lp_val = lfs.get("load_profile");
    let mut load_profile_kind: Option<LoadProfileKind> = None;
    let mut constant_rate: Option<String> = None;
    let mut linear_initial: Option<String> = None;
    let mut linear_rate: Option<String> = None;

    if let Some(Value::Mapping(m)) = lp_val {
        if let Some(rate) = m.get("constant") {
            load_profile_kind = Some(LoadProfileKind::Constant);
            constant_rate = rate.as_str().map(|s| s.to_string());
        } else if let Some(inner) = m.get("linear") {
            load_profile_kind = Some(LoadProfileKind::Linear);
            linear_initial = inner
                .get("initial")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            linear_rate = inner
                .get("rate")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
    }

    // blackhole: sequence of {http: {binding_addr}}, {tcp: {binding_addr}}, …
    let blackhole_entries = root
        .get("blackhole")
        .and_then(|v| v.as_sequence())
        .map(|seq| {
            seq.iter()
                .filter_map(|item| {
                    let m = item.as_mapping()?;
                    let (key, val) = m.iter().next()?;
                    let kind = match key.as_str()? {
                        "http" => BlackholeKind::Http,
                        "tcp" => BlackholeKind::Tcp,
                        "udp" => BlackholeKind::Udp,
                        _ => return None,
                    };
                    let addr = val
                        .get("binding_addr")
                        .and_then(|v| v.as_str())
                        .unwrap_or("127.0.0.1:9091")
                        .to_string();
                    Some(BlackholeEntry { kind, addr })
                })
                .collect::<Vec<_>>()
        });

    Ok(ImportedFields {
        seed,
        concurrent_logs,
        max_bytes_per_log,
        total_rotations,
        max_depth,
        mount_point,
        variant_kind,
        splunk_enc,
        static_path,
        template_path,
        load_profile_kind,
        constant_rate,
        linear_initial,
        linear_rate,
        max_prebuild_cache,
        max_block_size,
        blackhole_entries,
    })
}

/// Replace the first block-sequence `seed:` field with a flow-style array.
///
/// serde_yaml produces:
/// ```yaml
///       seed:
///       - 2
///       - 3
///       ...
/// ```
/// We want:
/// ```yaml
///       seed: [2, 3, ...]
/// ```
fn seed_to_flow_style(yaml: &str, seed: &[u8; 32]) -> String {
    let flow = format!(
        "[{}]",
        seed.iter()
            .map(|b| b.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let mut out = String::with_capacity(yaml.len() + 128);
    let mut iter = yaml.lines().peekable();

    while let Some(line) = iter.next() {
        // Match a line whose non-whitespace content is exactly "seed:"
        let trimmed = line.trim_start();
        if trimmed == "seed:" {
            // Check that the next line looks like a block sequence item
            let next_is_item = iter
                .peek()
                .map(|l| l.trim_start().starts_with("- "))
                .unwrap_or(false);

            if next_is_item {
                let indent = &line[..line.len() - trimmed.len()];
                out.push_str(indent);
                out.push_str("seed: ");
                out.push_str(&flow);
                out.push('\n');
                // Consume all "- N" item lines
                while let Some(&peek) = iter.peek() {
                    if peek.trim_start().starts_with("- ") {
                        iter.next();
                    } else {
                        break;
                    }
                }
                continue;
            }
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

/// Rewrite a `seed:` block-sequence in arbitrary YAML to a single-line flow sequence,
/// reading the values directly from the YAML rather than requiring the seed to be passed.
/// Used when displaying a user-supplied config (where we may not have the seed in memory).
/// A seed already on one line (flow style) is left unchanged.
pub fn compact_seed_in_yaml(yaml: &str) -> String {
    let mut out = String::with_capacity(yaml.len());
    let mut iter = yaml.lines().peekable();

    while let Some(line) = iter.next() {
        let trimmed = line.trim_start();
        if trimmed == "seed:" {
            let next_is_item = iter
                .peek()
                .map(|l| l.trim_start().starts_with("- "))
                .unwrap_or(false);
            if next_is_item {
                let indent = &line[..line.len() - trimmed.len()];
                let mut items: Vec<String> = Vec::new();
                while let Some(&peek) = iter.peek() {
                    if let Some(val) = peek.trim_start().strip_prefix("- ") {
                        items.push(val.to_string());
                        iter.next();
                    } else {
                        break;
                    }
                }
                out.push_str(indent);
                out.push_str("seed: [");
                out.push_str(&items.join(", "));
                out.push_str("]\n");
                continue;
            }
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}
