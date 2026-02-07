//! Benchmarks for OpenTelemetry trace payload generation.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
use lading_payload::opentelemetry::trace::{
    AttributeConfig, AttributeValueConfig, Config, DatabaseServiceConfig, GrpcServiceConfig,
    OperationConfig, ServiceConfig, ServiceType, SuboperationConfig,
};
use lading_payload::{OpentelemetryTraces, Serialize};
use rand::{SeedableRng, rngs::SmallRng};
use std::time::Duration;

const MIB: usize = 1_048_576;

/// A minimal but representative topology for benchmarking.
fn bench_config() -> Config {
    Config {
        services: vec![
            ServiceConfig {
                name: "api-gateway".into(),
                service_type: Some(ServiceType::Http),
                grpc: None,
                database: None,
                operations: vec![OperationConfig {
                    id: "get-users".into(),
                    span_name: None,
                    method: Some("GET".into()),
                    route: Some("/users/{id}".into()),
                    table: None,
                    query: None,
                    suboperations: vec![SuboperationConfig {
                        to: "user-service/get-user".into(),
                        rate: 1.0,
                        max_repeat: 1,
                        attributes: vec![],
                    }],
                    attributes: vec![],
                    root: None,
                }],
                resource_attributes: vec![AttributeConfig::new(
                    "deployment.environment",
                    AttributeValueConfig::Static("production".into()),
                )],
                scope_name: Some("com.example.gateway".into()),
                scope_version: "1.0.0".into(),
            },
            ServiceConfig {
                name: "user-service".into(),
                service_type: Some(ServiceType::Grpc),
                grpc: Some(GrpcServiceConfig {
                    service: "UserService".into(),
                }),
                database: None,
                operations: vec![OperationConfig {
                    id: "get-user".into(),
                    span_name: None,
                    method: Some("GetUser".into()),
                    route: None,
                    table: None,
                    query: None,
                    suboperations: vec![SuboperationConfig {
                        to: "postgres/select-users".into(),
                        rate: 1.0,
                        max_repeat: 1,
                        attributes: vec![],
                    }],
                    attributes: vec![],
                    root: None,
                }],
                resource_attributes: vec![],
                scope_name: Some("com.example.users".into()),
                scope_version: "1.0.0".into(),
            },
            ServiceConfig {
                name: "postgres".into(),
                service_type: Some(ServiceType::Database),
                grpc: None,
                database: Some(DatabaseServiceConfig {
                    system: "postgresql".into(),
                    name: Some("mydb".into()),
                }),
                operations: vec![OperationConfig {
                    id: "select-users".into(),
                    span_name: None,
                    method: None,
                    route: None,
                    table: Some("users".into()),
                    query: Some("SELECT * FROM users WHERE id = $1".into()),
                    suboperations: vec![],
                    attributes: vec![],
                    root: None,
                }],
                resource_attributes: vec![],
                scope_name: None,
                scope_version: "1.0.0".into(),
            },
        ],
        error_rate: 0.01,
    }
}

fn opentelemetry_traces_setup(c: &mut Criterion) {
    let config = bench_config();
    c.bench_function("opentelemetry_traces_setup", |b| {
        b.iter(|| {
            let mut rng = SmallRng::seed_from_u64(19_690_716);
            let _ot = OpentelemetryTraces::with_config(&config, &mut rng)
                .expect("failed to create trace generator");
        })
    });
}

fn opentelemetry_traces_all(c: &mut Criterion) {
    let config = bench_config();
    let mut group = c.benchmark_group("opentelemetry_traces_all");
    for size in &[MIB, 10 * MIB, 100 * MIB, 1_000 * MIB] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut rng = SmallRng::seed_from_u64(19_690_716);
                let mut ot = OpentelemetryTraces::with_config(&config, &mut rng)
                    .expect("failed to create trace generator");
                let mut writer = Vec::with_capacity(size);

                ot.to_bytes(rng, size, &mut writer)
                    .expect("failed to convert to bytes");
            });
        });
    }
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(90));
    targets = opentelemetry_traces_setup, opentelemetry_traces_all
);
