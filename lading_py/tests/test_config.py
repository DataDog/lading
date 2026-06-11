"""Tests for config parsing."""
import pytest
import yaml
from pydantic import ValidationError
from lading_py.config import (
    RootConfig, DogStatsDConfig, ConfRange, InclusiveRange,
    parse_bytes, TelemetryConfig,
)


class TestParseBytes:
    def test_mib(self):
        assert parse_bytes("1 MiB") == 1024 ** 2

    def test_gib(self):
        assert parse_bytes("2 GiB") == 2 * 1024 ** 3

    def test_mb(self):
        assert parse_bytes("100 MB") == 100 * 1000 ** 2

    def test_b(self):
        assert parse_bytes("8192 B") == 8192

    def test_fractional(self):
        assert parse_bytes("0.5 GiB") == int(0.5 * 1024 ** 3)

    def test_plain_int(self):
        assert parse_bytes(12345) == 12345

    def test_case_insensitive(self):
        assert parse_bytes("4 mib") == 4 * 1024 ** 2

    def test_500_mib(self):
        assert parse_bytes("500 MiB") == 500 * 1024 ** 2


class TestConfRange:
    def test_inclusive_lo_hi(self):
        r = ConfRange(inclusive=InclusiveRange(min=3, max=7))
        assert r.lo == 3
        assert r.hi == 7

    def test_sample_int_in_range(self):
        import random
        rng = random.Random(42)
        r = ConfRange(inclusive=InclusiveRange(min=2, max=5))
        for _ in range(100):
            v = r.sample_int(rng)
            assert 2 <= v <= 5

    def test_sample_float_in_range(self):
        import random
        rng = random.Random(42)
        r = ConfRange(inclusive=InclusiveRange(min=0.1, max=1.0))
        for _ in range(100):
            v = r.sample(rng)
            assert 0.1 <= v <= 1.0


class TestDogStatsDConfig:
    def test_defaults(self):
        cfg = DogStatsDConfig()
        assert cfg.multivalue_pack_probability == 0.08
        assert cfg.sampling_probability == 0.5
        assert cfg.length_prefix_framed is False

    def test_length_prefix_framed_rejected(self):
        with pytest.raises(ValidationError, match="length_prefix_framed"):
            DogStatsDConfig(length_prefix_framed=True)

    def test_metric_names_default(self):
        cfg = DogStatsDConfig()
        assert cfg.metric_names == ["metric{{0-9}}"]


class TestTelemetryConfig:
    def test_short_form_path(self):
        tel = TelemetryConfig(path="nong")
        assert tel.output_path == "nong"
        assert tel.format == "jsonl"
        assert tel.flush_seconds == 60

    def test_long_form_log(self):
        tel = TelemetryConfig(log={"path": "out", "format": {"jsonl": {"flush_seconds": 30}}})
        assert tel.output_path == "out"
        assert tel.format == "jsonl"
        assert tel.flush_seconds == 30

    def test_parquet_format(self):
        tel = TelemetryConfig(log={"path": "out", "format": {"parquet": {"flush_seconds": 120}}})
        assert tel.format == "parquet"

    def test_prometheus_addr(self):
        tel = TelemetryConfig(prometheus={"addr": "0.0.0.0:9000"})
        assert tel.prometheus_addr == "0.0.0.0:9000"
        assert tel.output_path is None


class TestRootConfigFromYaml:
    def test_parse_lading_yaml(self):
        with open("/home/stephenwakely/src/lading/lading.yaml") as f:
            raw = yaml.safe_load(f)
        config = RootConfig.model_validate(raw)

        assert len(config.generator) == 1
        gen = config.generator[0]
        assert gen.unix_datagram is not None
        assert gen.unix_datagram.path == "/tmp/dsd.socket"
        assert gen.unix_datagram.bytes_per_second_int == 1024 ** 2

        dsd = gen.unix_datagram.dogstatsd
        assert dsd.contexts.lo == 50
        assert dsd.contexts.hi == 50
        assert dsd.metric_weights.distribution == 5
        assert dsd.metric_names == ["name{{0-2}}"]
        assert dsd.tag_names == ["tag1", "tag2", "tag3"]

        assert len(config.target_metrics) == 3
        assert config.target_metrics[0].prometheus is not None
        assert config.target_metrics[2].expvar is not None

    def test_minimal_config(self):
        raw = {
            "generator": [{
                "unix_datagram": {
                    "seed": list(range(32)),
                    "path": "/tmp/test.socket",
                    "variant": {"dogstatsd": {}},
                }
            }],
        }
        config = RootConfig.model_validate(raw)
        assert config.experiment_duration_secs == 60
        assert config.sample_period_milliseconds == 1000

    def test_empty_config(self):
        config = RootConfig.model_validate({})
        assert config.generator == []
        assert config.blackhole == []
        assert config.target_metrics == []
