use criterion::{Criterion, black_box, criterion_group, criterion_main};

const SAMPLE_METRICS: &str = r#"
# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{method="post",code="200"} 1027 1395066363000
http_requests_total{method="post",code="400"}    3 1395066363000

# Escaping in label values:
msdos_file_access_time_seconds{path="C:\\DIR\\FILE.TXT",error="Cannot find file:\n\"FILE.TXT\""} 1.458255915e9

# Minimalistic line:
metric_without_timestamp_and_labels 12.47

# A weird metric from before the epoch:
something_weird{problem="division by zero"} +Inf -3982045

# A histogram, which has a pretty complex representation in the text format:
# HELP http_request_duration_seconds A histogram of the request duration.
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.05"} 24054
http_request_duration_seconds_bucket{le="0.1"} 33444
http_request_duration_seconds_bucket{le="0.2"} 100392
http_request_duration_seconds_bucket{le="0.5"} 129389
http_request_duration_seconds_bucket{le="1"} 133988
http_request_duration_seconds_bucket{le="+Inf"} 144320
http_request_duration_seconds_sum 53423
http_request_duration_seconds_count 144320

# TYPE rpc_duration_seconds summary
rpc_duration_seconds{quantile="0.01"} 3102
rpc_duration_seconds{quantile="0.05"} 3272
rpc_duration_seconds{quantile="0.5"} 4773
rpc_duration_seconds{quantile="0.9"} 9001
rpc_duration_seconds{quantile="0.99"} 76656
rpc_duration_seconds_sum 1.7560473e+07
rpc_duration_seconds_count 2693
"#;

fn benchmark_parser(c: &mut Criterion) {
    use lading::target_metrics::prometheus::parser::PrometheusParser;

    c.bench_function("prometheus_parser", |b| {
        b.iter(|| {
            let mut parser = PrometheusParser::new();
            let results = parser.parse_text(black_box(SAMPLE_METRICS));

            // Ensure we actually process the results
            let count = results.iter().filter(|r| r.is_ok()).count();
            black_box(count);
        });
    });
}

fn benchmark_large_metrics(c: &mut Criterion) {
    use lading::target_metrics::prometheus::parser::PrometheusParser;

    // Generate a large metrics payload
    let mut large_metrics = String::with_capacity(1_000_000);

    // Add TYPE declarations
    large_metrics.push_str("# TYPE http_requests_total counter\n");
    large_metrics.push_str("# TYPE memory_usage gauge\n");

    // Add many metric lines
    for i in 0..10000 {
        large_metrics.push_str(&format!(
            "http_requests_total{{method=\"GET\",code=\"200\",path=\"/api/v{}/users\"}} {} 1395066363000\n",
            i % 10, i * 10
        ));

        if i % 100 == 0 {
            large_metrics.push_str(&format!(
                "memory_usage{{instance=\"node{}\",job=\"monitoring\"}} {}\n",
                i % 20,
                1024 * 1024 * (i % 1000)
            ));
        }
    }

    let large_metrics = large_metrics;

    c.bench_function("prometheus_parser_large", |b| {
        b.iter(|| {
            let mut parser = PrometheusParser::new();
            let results = parser.parse_text(black_box(&large_metrics));
            let count = results.iter().filter(|r| r.is_ok()).count();
            black_box(count);
        });
    });
}

fn benchmark_escaped_labels(c: &mut Criterion) {
    use lading::target_metrics::prometheus::parser::PrometheusParser;

    // Metrics with lots of escaped labels that require allocation
    let escaped_metrics = r#"
# TYPE escaped_metric gauge
escaped_metric{path="C:\\Program Files\\App\\data.txt",message="Error: \"File not found\"\nPlease check path"} 1.0
escaped_metric{path="D:\\Users\\John\\Documents\\report.pdf",message="Success\nProcessed in 100ms"} 2.0
escaped_metric{json="{\"key\": \"value\", \"array\": [1, 2, 3]}"} 3.0
"#.repeat(1000);

    c.bench_function("prometheus_parser_escaped", |b| {
        b.iter(|| {
            let mut parser = PrometheusParser::new();
            let results = parser.parse_text(black_box(&escaped_metrics));
            let count = results.iter().filter(|r| r.is_ok()).count();
            black_box(count);
        });
    });
}

criterion_group!(
    benches,
    benchmark_parser,
    benchmark_large_metrics,
    benchmark_escaped_labels
);
criterion_main!(benches);
