import time
from dataclasses import dataclass, field
from enum import Enum


class MetricKind(str, Enum):
    Counter = "counter"
    Gauge = "gauge"
    Histogram = "histogram"


@dataclass
class Line:
    run_id: str
    time: int           # milliseconds since epoch
    fetch_index: int
    metric_name: str
    metric_kind: str    # MetricKind value
    value: float
    labels: dict[str, str] = field(default_factory=dict)
    value_histogram: bytes = b""

    def to_dict(self) -> dict:
        import base64
        d = {
            "run_id": self.run_id,
            "time": self.time,
            "fetch_index": self.fetch_index,
            "metric_name": self.metric_name,
            "metric_kind": self.metric_kind,
            "value": self.value,
            "labels": self.labels,
        }
        if self.value_histogram:
            d["value_histogram"] = base64.b64encode(self.value_histogram).decode()
        return d
