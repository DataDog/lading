import re
from typing import Any
from pydantic import BaseModel, model_validator


def parse_bytes(s: str | int) -> int:
    if isinstance(s, int):
        return s
    units = {
        "b": 1, "kb": 1000, "mb": 1000**2, "gb": 1000**3,
        "kib": 1024, "mib": 1024**2, "gib": 1024**3,
    }
    m = re.match(r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]+)$", str(s).strip())
    if not m:
        return int(s)
    n, unit = float(m.group(1)), m.group(2).lower()
    return int(n * units.get(unit, 1))


class InclusiveRange(BaseModel):
    min: float
    max: float


class ExclusiveRange(BaseModel):
    min: float
    max: float


class ConfRange(BaseModel):
    inclusive: InclusiveRange | None = None
    exclusive: ExclusiveRange | None = None

    @property
    def lo(self) -> float:
        if self.inclusive:
            return self.inclusive.min
        return self.exclusive.min + 1

    @property
    def hi(self) -> float:
        if self.inclusive:
            return self.inclusive.max
        return self.exclusive.max - 1

    def sample(self, rng) -> float:
        return rng.uniform(self.lo, self.hi)

    def sample_int(self, rng) -> int:
        return rng.randint(int(self.lo), int(self.hi))


class KindWeights(BaseModel):
    metric: int = 90
    event: int = 0
    service_check: int = 0


class MetricWeights(BaseModel):
    count: int = 0
    gauge: int = 0
    timer: int = 0
    distribution: int = 5
    set: int = 0
    histogram: int = 0


_DEFAULT_CONTEXTS = ConfRange(inclusive=InclusiveRange(min=50, max=50))
_DEFAULT_TAGS_PER_MSG = ConfRange(inclusive=InclusiveRange(min=3, max=3))
_DEFAULT_MULTIVALUE_COUNT = ConfRange(inclusive=InclusiveRange(min=2, max=32))
_DEFAULT_SAMPLING_RANGE = ConfRange(inclusive=InclusiveRange(min=0.1, max=1.0))


class DogStatsDConfig(BaseModel):
    contexts: ConfRange = _DEFAULT_CONTEXTS
    tags_per_msg: ConfRange = _DEFAULT_TAGS_PER_MSG
    multivalue_count: ConfRange = _DEFAULT_MULTIVALUE_COUNT
    multivalue_pack_probability: float = 0.08
    kind_weights: KindWeights = KindWeights()
    metric_weights: MetricWeights = MetricWeights()
    metric_names: list[str] = ["metric{{0-9}}"]
    tag_names: list[str] = ["tag1", "tag2", "tag3"]
    tag_values: list[str] = ["value{{0-9}}"]
    sampling_range: ConfRange = _DEFAULT_SAMPLING_RANGE
    sampling_probability: float = 0.5
    unique_tag_ratio: float = 0.11
    length_prefix_framed: bool = False
    container_ids: list[str] = []
    external_data: list[str] = []
    cardinality: list[str] = []

    @model_validator(mode="after")
    def reject_length_prefix_framed(self):
        if self.length_prefix_framed:
            raise ValueError(
                "length_prefix_framed=true is unsupported: dogstatsd-py does not "
                "expose length-prefix framing. Set length_prefix_framed: false."
            )
        return self


class UnixDatagramConfig(BaseModel):
    seed: list[int]
    path: str
    bytes_per_second: Any = "1 MiB"
    maximum_prebuild_cache_size_bytes: Any = "500 MiB"
    maximum_block_size: Any = "8192 B"
    parallel_connections: int = 1
    variant: dict[str, Any] = {}

    @property
    def bytes_per_second_int(self) -> int:
        return parse_bytes(self.bytes_per_second)

    @property
    def dogstatsd(self) -> DogStatsDConfig:
        raw = self.variant.get("dogstatsd", {})
        return DogStatsDConfig(**raw)


class GeneratorConfig(BaseModel):
    id: str | None = None
    unix_datagram: UnixDatagramConfig | None = None


class HttpBlackholeConfig(BaseModel):
    binding_addr: str


class BlackholeConfig(BaseModel):
    http: HttpBlackholeConfig | None = None


class PrometheusTargetConfig(BaseModel):
    uri: str
    tags: dict[str, str] = {}
    metrics: list[str] | None = None


class ExpvarTargetConfig(BaseModel):
    uri: str
    vars: list[str] = []
    tags: dict[str, str] = {}


class TargetMetricsEntry(BaseModel):
    prometheus: PrometheusTargetConfig | None = None
    expvar: ExpvarTargetConfig | None = None


class TelemetryConfig(BaseModel):
    # Short form: telemetry: {path: "nong"}
    path: str | None = None
    # Long form: telemetry: {log: {path: ..., format: ...}}
    log: dict[str, Any] | None = None
    prometheus: dict[str, Any] | None = None
    prometheus_socket: dict[str, Any] | None = None
    global_labels: dict[str, str] = {}

    @property
    def output_path(self) -> str | None:
        if self.path:
            return self.path
        if self.log:
            return self.log.get("path")
        return None

    @property
    def format(self) -> str:
        if self.log:
            fmt = self.log.get("format", {})
            if isinstance(fmt, dict):
                if "parquet" in fmt:
                    return "parquet"
                if "multi" in fmt:
                    return "multi"
        return "jsonl"

    @property
    def flush_seconds(self) -> int:
        if self.log:
            fmt = self.log.get("format", {})
            if isinstance(fmt, dict):
                for k in ("jsonl", "parquet", "multi"):
                    if k in fmt and isinstance(fmt[k], dict):
                        return fmt[k].get("flush_seconds", 60)
        return 60

    @property
    def prometheus_addr(self) -> str | None:
        if self.prometheus:
            return self.prometheus.get("addr")
        return None

    @property
    def prometheus_socket_path(self) -> str | None:
        if self.prometheus_socket:
            return self.prometheus_socket.get("path")
        return None


class ObserverConfig(BaseModel):
    enable_smaps: bool = False
    enable_smaps_rollup: bool = True


class RootConfig(BaseModel):
    generator: list[GeneratorConfig] = []
    blackhole: list[BlackholeConfig] = []
    target_metrics: list[TargetMetricsEntry] = []
    telemetry: TelemetryConfig | None = None
    observer: ObserverConfig | None = None
    sample_period_milliseconds: int = 1000
    warmup_duration_secs: int = 0
    experiment_duration_secs: int = 60

    model_config = {"extra": "allow"}
