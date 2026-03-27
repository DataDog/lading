/// Every payload variant the wizard supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VariantKind {
    Ascii,
    Json,
    ApacheCommon,
    Syslog5424,
    DatadogLog,
    SplunkHec,
    TemplatedJson,
    Static,
    StaticChunks,
}

pub const ALL_VARIANTS: &[VariantKind] = &[
    VariantKind::Ascii,
    VariantKind::Json,
    VariantKind::ApacheCommon,
    VariantKind::Syslog5424,
    VariantKind::DatadogLog,
    VariantKind::SplunkHec,
    VariantKind::TemplatedJson,
    VariantKind::Static,
    VariantKind::StaticChunks,
];

pub struct VariantMeta {
    pub label: &'static str,
    pub description: &'static str,
    pub example_line: &'static str,
}

pub fn variant_meta(kind: VariantKind) -> VariantMeta {
    match kind {
        VariantKind::Ascii => VariantMeta {
            label: "ascii",
            description: "Generates lines of random printable ASCII characters (up to 6 KiB each).\nFast and simple — good default for throughput tests with minimal CPU overhead.",
            example_line: "k8Qr2mX9pL3fB7nT4vW0jZ5cR1eY6uI8oP2dFgHsA...",
        },
        VariantKind::Json => VariantMeta {
            label: "json",
            description: "Generates JSON-encoded log lines with numeric and array fields.\nUseful for testing JSON-aware consumers and parsers.",
            example_line: r#"{"id":12345678,"name":98765432,"seed":4242,"byte_parade":[104,101,108]}"#,
        },
        VariantKind::ApacheCommon => VariantMeta {
            label: "apache_common",
            description: "Generates Apache Common Log Format access log lines.\nRealistic HTTP access logs — ideal for testing log parsers and shippers.",
            example_line: r#"192.168.1.42 - frank [21/Mar/2024:13:55:36 -0700] "GET /index.html HTTP/1.1" 200 1234"#,
        },
        VariantKind::Syslog5424 => VariantMeta {
            label: "syslog5424",
            description: "Generates RFC 5424 structured syslog messages.\nStandard syslog format used by many Unix daemons and network devices.",
            example_line: "<34>1 2024-03-21T13:55:36Z webserver myapp 1234 ID47 - Application started",
        },
        VariantKind::DatadogLog => VariantMeta {
            label: "datadog_log",
            description: "Generates Datadog JSON log messages with standard agent fields.\nIdeal for testing the Datadog Logs intake pipeline end-to-end.",
            example_line: r#"{"message":"conn ok","status":"info","timestamp":"...","hostname":"web-01","service":"api"}"#,
        },
        VariantKind::SplunkHec => VariantMeta {
            label: "splunk_hec",
            description: "Generates Splunk HTTP Event Collector messages.\nRequires an encoding sub-field: text (raw event) or json (structured event object).",
            example_line: r#"{"time":1710029736,"host":"web-01","source":"app","event":"request processed"}"#,
        },
        VariantKind::TemplatedJson => VariantMeta {
            label: "templated_json",
            description: "Generates JSON records from a user-supplied YAML template file.\nFull control over schema and field value distributions.",
            example_line: "(generated from your template file)",
        },
        VariantKind::Static => VariantMeta {
            label: "static",
            description: "Streams content from a user-supplied static file.\nContent is read as-is — useful for replay of captured production logs.",
            example_line: "(content streamed from: <path>)",
        },
        VariantKind::StaticChunks => VariantMeta {
            label: "static_chunks",
            description: "Streams line-by-line chunks from a user-supplied file.\nChunks lines to fill blocks up to maximum_block_size — more efficient for large files.",
            example_line: "(line-by-line chunks from: <path>)",
        },
    }
}
