//! The OTLP protocol speaking generator.

use super::General;
use hyper::Uri;
use opentelemetry::global;
use opentelemetry::KeyValue;
use opentelemetry_otlp;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::{runtime, Resource};
use serde::{Deserialize, Serialize};
//use opentelemetry_sdk::metrics::data::Temporality;
use opentelemetry_sdk::metrics::reader::DefaultTemporalitySelector;
use std::vec;

/// Config for [`OTLP`]
#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
/// Configuration of this generator.
pub struct Config {
    /// The seed for random operations against this target
    pub seed: [u8; 32],
    /// The URI for the target, must be a valid URI
    #[serde(with = "http_serde::uri")]
    pub target_uri: Uri,
    /// The bytes per second to send or receive from the target
    pub bytes_per_second: byte_unit::Byte,
    /// The maximum size in bytes of the largest block in the prebuild cache.
    #[serde(default = "lading_payload::block::default_maximum_block_size")]
    pub maximum_block_size: byte_unit::Byte,
    /// The total number of parallel connections to maintain
    pub parallel_connections: u16,
    /// The load throttle configuration
    #[serde(default)]
    pub throttle: lading_throttle::Config,
}

#[allow(missing_docs)]
#[derive(thiserror::Error, Copy, Clone, Debug)]
/// Errors produced by [`Otlp`].
pub enum Error {
    // Generic error for OTLP
    #[error("OTLP Error")]
    Generic,
}

/// The OTLP generator.
///
/// This generator is reposnsible for connecting to the target via OTLP.
#[derive(Clone, Debug)]
pub struct Otlp {
    target_uri: Uri,
    shutdown: lading_signal::Watcher,
}

fn init_meter_provider(endpoint: &str) -> opentelemetry_sdk::metrics::SdkMeterProvider {
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic() // create GRPC layer
        .with_endpoint(endpoint)
        .build_metrics_exporter(Box::new(DefaultTemporalitySelector::new()))
        .unwrap();

    let reader = PeriodicReader::builder(exporter, runtime::Tokio).build();
    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(Resource::new([KeyValue::new(
            "service.name",
            "metrics-basic-example",
        )]))
        .build();

    global::set_meter_provider(provider.clone());
    provider
}

impl Otlp {
    /// Create a new [`Otlp`] instance
    ///
    /// # Errors
    ///
    /// Creation will fail if the underlying governor capacity exceeds u32.
    ///
    /// # Panics
    ///
    /// Function will panic if user has passed non-zero values for any byte
    /// values. Sharp corners.
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(
        _general: General,
        config: Config,
        shutdown: lading_signal::Watcher,
    ) -> Result<Self, Error> {
        Ok(Self {
            target_uri: config.target_uri,
            shutdown,
        })
    }

    /// Run [`Otlp`] to completion or until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// TODO
    ///
    pub async fn spin(self) -> Result<(), Error> {
        let meter_provider = init_meter_provider(&self.target_uri.to_string());

        // Create a meter from the above MeterProvider.
        let meter = global::meter("mylibraryname");

        // Create a Counter Instrument.
        let counter = meter.u64_counter("my_counter").init();

        // Record measurements using the Counter instrument.
        counter.add(
            10,
            &[
                KeyValue::new("mykey1", "myvalue1"),
                KeyValue::new("mykey2", "myvalue2"),
            ],
        );

        // Create a ObservableCounter instrument and register a callback that reports the measurement.
        let _observable_counter = meter
            .u64_observable_counter("my_observable_counter")
            .with_description("My observable counter example description")
            .with_unit("myunit")
            .with_callback(|observer| {
                observer.observe(
                    100,
                    &[
                        KeyValue::new("mykey1", "myvalue1"),
                        KeyValue::new("mykey2", "myvalue2"),
                    ],
                )
            })
            .init();

        // Create a UpCounter Instrument.
        let updown_counter = meter.i64_up_down_counter("my_updown_counter").init();

        // Record measurements using the UpCounter instrument.
        updown_counter.add(
            -10,
            &[
                KeyValue::new("mykey1", "myvalue1"),
                KeyValue::new("mykey2", "myvalue2"),
            ],
        );

        // Create a Observable UpDownCounter instrument and register a callback that reports the measurement.
        let _observable_up_down_counter = meter
            .i64_observable_up_down_counter("my_observable_updown_counter")
            .with_description("My observable updown counter example description")
            .with_unit("myunit")
            .with_callback(|observer| {
                observer.observe(
                    100,
                    &[
                        KeyValue::new("mykey1", "myvalue1"),
                        KeyValue::new("mykey2", "myvalue2"),
                    ],
                )
            })
            .init();

        // Create a Histogram Instrument.
        let histogram = meter
            .f64_histogram("my_histogram")
            .with_description("My histogram example description")
            // Setting boundaries is optional. By default, the boundaries are set to
            // [0.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 5000.0, 7500.0, 10000.0]
            .with_boundaries(vec![0.0, 5.0, 10.0, 15.0, 20.0, 25.0])
            .init();

        // Record measurements using the histogram instrument.
        histogram.record(
            10.5,
            &[
                KeyValue::new("mykey1", "myvalue1"),
                KeyValue::new("mykey2", "myvalue2"),
            ],
        );

        // Note that there is no ObservableHistogram instrument.

        // Create a Gauge Instrument.
        let gauge = meter
            .f64_gauge("my_gauge")
            .with_description("A gauge set to 1.0")
            .with_unit("myunit")
            .init();

        gauge.record(
            1.0,
            &[
                KeyValue::new("mykey1", "myvalue1"),
                KeyValue::new("mykey2", "myvalue2"),
            ],
        );

        // Create a ObservableGauge instrument and register a callback that reports the measurement.
        let _observable_gauge = meter
            .f64_observable_gauge("my_observable_gauge")
            .with_description("An observable gauge set to 1.0")
            .with_unit("myunit")
            .with_callback(|observer| {
                observer.observe(
                    1.0,
                    &[
                        KeyValue::new("mykey1", "myvalue1"),
                        KeyValue::new("mykey2", "myvalue2"),
                    ],
                )
            })
            .init();

        // Metrics are exported by default every 30 seconds when using stdout exporter,
        // however shutting down the MeterProvider here instantly flushes
        // the metrics, instead of waiting for the 30 sec interval.
        match meter_provider.shutdown() {
            Ok(_) => return Ok(()),
            Err(_err) => return Err(Error::Generic),
        }
    }
}
