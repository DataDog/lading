"""
DogStatsD payload generation.

Produces Block objects (call descriptors) consumed by the generator.
All serialization is deferred to dogstatsd-py at send time.
"""
import re
import random
from dataclasses import dataclass, field
from typing import Union

from lading_py.config import DogStatsDConfig


# ---------------------------------------------------------------------------
# Template expansion
# ---------------------------------------------------------------------------

_TEMPLATE_RE = re.compile(r"\{\{(\d+)-(\d+)\}\}")


def expand_template(tmpl: str) -> list[str]:
    """Expand 'name{{0-2}}' → ['name0', 'name1', 'name2']."""
    m = _TEMPLATE_RE.search(tmpl)
    if not m:
        return [tmpl]
    lo, hi = int(m.group(1)), int(m.group(2))
    prefix = tmpl[: m.start()]
    suffix = tmpl[m.end() :]
    results = []
    for i in range(lo, hi + 1):
        for s in expand_template(prefix + str(i) + suffix):
            results.append(s)
    return results


def expand_list(templates: list[str]) -> list[str]:
    out = []
    for t in templates:
        out.extend(expand_template(t))
    return out


# ---------------------------------------------------------------------------
# Call descriptors
# ---------------------------------------------------------------------------

@dataclass
class MetricCall:
    name: str
    value: float
    metric_type: str  # "gauge"|"count"|"histogram"|"distribution"|"timing"|"set"
    tags: list[str]
    sample_rate: float | None = None


@dataclass
class EventCall:
    title: str
    text: str
    tags: list[str]
    alert_type: str | None = None
    priority: str | None = None


@dataclass
class ServiceCheckCall:
    name: str
    status: int  # 0=OK 1=WARNING 2=CRITICAL 3=UNKNOWN
    tags: list[str]
    message: str | None = None


# A single metric/event/service_check OR a batch of metrics (multi-value)
Block = Union[MetricCall, EventCall, ServiceCheckCall, list[MetricCall]]


# ---------------------------------------------------------------------------
# Context pool
# ---------------------------------------------------------------------------

@dataclass
class Context:
    name: str
    base_tags: list[str]


def _weighted_choice(rng: random.Random, weights: dict[str, int]) -> str:
    keys = [k for k, w in weights.items() if w > 0]
    ws = [weights[k] for k in keys]
    return rng.choices(keys, weights=ws, k=1)[0]


def build_context_pool(cfg: DogStatsDConfig, rng: random.Random) -> list[Context]:
    names = expand_list(cfg.metric_names)
    tag_names = expand_list(cfg.tag_names)
    tag_values = expand_list(cfg.tag_values)

    n = int(cfg.contexts.hi)
    contexts = []
    for _ in range(n):
        name = rng.choice(names)
        n_tags = cfg.tags_per_msg.sample_int(rng)
        tags = [
            f"{rng.choice(tag_names)}:{rng.choice(tag_values)}"
            for _ in range(n_tags)
        ]
        contexts.append(Context(name=name, base_tags=tags))
    return contexts


# ---------------------------------------------------------------------------
# Block generation
# ---------------------------------------------------------------------------

_METRIC_TYPE_MAP = {
    "count": "count",
    "gauge": "gauge",
    "timer": "timing",
    "distribution": "distribution",
    "set": "set",
    "histogram": "histogram",
}

_ALERT_TYPES = ["error", "warning", "info", "success"]
_PRIORITIES = ["normal", "low"]
_SC_STATUSES = [0, 1, 2, 3]


def _sample_metric_value(rng: random.Random, metric_type: str) -> float:
    if metric_type == "count":
        return float(rng.randint(1, 100))
    if metric_type == "set":
        return float(rng.randint(0, 10000))
    if metric_type == "timing":
        return round(rng.uniform(0.1, 5000.0), 3)
    return round(rng.uniform(0.0, 1000.0), 4)


def _maybe_sample_rate(rng: random.Random, cfg: DogStatsDConfig) -> float | None:
    if rng.random() < cfg.sampling_probability:
        return round(cfg.sampling_range.sample(rng), 4)
    return None


def _gen_metric_call(
    rng: random.Random, cfg: DogStatsDConfig, contexts: list[Context]
) -> MetricCall:
    ctx = rng.choice(contexts)
    kind_weights = {k: v for k, v in cfg.metric_weights.model_dump().items()}
    raw_type = _weighted_choice(rng, kind_weights)
    metric_type = _METRIC_TYPE_MAP[raw_type]
    value = _sample_metric_value(rng, metric_type)
    sample_rate = _maybe_sample_rate(rng, cfg)
    return MetricCall(
        name=ctx.name,
        value=value,
        metric_type=metric_type,
        tags=list(ctx.base_tags),
        sample_rate=sample_rate,
    )


def _gen_event_call(rng: random.Random) -> EventCall:
    title_len = rng.randint(8, 32)
    text_len = rng.randint(16, 128)
    title = "".join(rng.choices("abcdefghijklmnopqrstuvwxyz_", k=title_len))
    text = "".join(rng.choices("abcdefghijklmnopqrstuvwxyz_ ", k=text_len))
    alert_type = rng.choice(_ALERT_TYPES) if rng.random() < 0.5 else None
    priority = rng.choice(_PRIORITIES) if rng.random() < 0.5 else None
    return EventCall(title=title, text=text, tags=[], alert_type=alert_type, priority=priority)


def _gen_service_check_call(rng: random.Random) -> ServiceCheckCall:
    name_len = rng.randint(8, 32)
    name = "".join(rng.choices("abcdefghijklmnopqrstuvwxyz_.", k=name_len))
    status = rng.choice(_SC_STATUSES)
    return ServiceCheckCall(name=name, status=status, tags=[])


def generate_block(
    rng: random.Random, cfg: DogStatsDConfig, contexts: list[Context]
) -> Block:
    kind_weights = cfg.kind_weights.model_dump()
    kind = _weighted_choice(rng, kind_weights)

    if kind == "metric":
        if rng.random() < cfg.multivalue_pack_probability:
            count = cfg.multivalue_count.sample_int(rng)
            return [_gen_metric_call(rng, cfg, contexts) for _ in range(count)]
        return _gen_metric_call(rng, cfg, contexts)
    elif kind == "event":
        return _gen_event_call(rng)
    else:
        return _gen_service_check_call(rng)


# ---------------------------------------------------------------------------
# Block cache
# ---------------------------------------------------------------------------

def _estimate_block_bytes(block: Block) -> int:
    """Rough wire-size estimate for rate limiting."""
    if isinstance(block, list):
        return sum(_estimate_block_bytes(m) for m in block)
    if isinstance(block, MetricCall):
        return len(block.name) + sum(len(t) for t in block.tags) + 30
    if isinstance(block, EventCall):
        return len(block.title) + len(block.text) + 20
    if isinstance(block, ServiceCheckCall):
        return len(block.name) + 20
    return 50


class BlockCache:
    def __init__(self, cfg: DogStatsDConfig, seed: list[int], max_count: int = 10_000):
        seed_int = int.from_bytes(bytes(seed[:32]), "little")
        rng = random.Random(seed_int)
        contexts = build_context_pool(cfg, rng)
        self._blocks: list[Block] = [
            generate_block(rng, cfg, contexts) for _ in range(max_count)
        ]
        self._idx = 0

    def next(self) -> Block:
        block = self._blocks[self._idx]
        self._idx = (self._idx + 1) % len(self._blocks)
        return block
