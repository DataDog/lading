"""Tests for DogStatsD payload generation."""
import random
import pytest
from lading_py.config import (
    DogStatsDConfig, ConfRange, InclusiveRange, KindWeights, MetricWeights,
)
from lading_py.payload.dogstatsd import (
    expand_template, expand_list,
    build_context_pool, generate_block,
    BlockCache, MetricCall, EventCall, ServiceCheckCall,
    _estimate_block_bytes,
)


# ---------------------------------------------------------------------------
# Template expansion
# ---------------------------------------------------------------------------

class TestExpandTemplate:
    def test_no_template(self):
        assert expand_template("metric.name") == ["metric.name"]

    def test_simple_range(self):
        assert expand_template("name{{0-2}}") == ["name0", "name1", "name2"]

    def test_suffix(self):
        assert expand_template("m{{1-3}}.count") == ["m1.count", "m2.count", "m3.count"]

    def test_single_value(self):
        assert expand_template("x{{5-5}}") == ["x5"]

    def test_expand_list_multiple(self):
        result = expand_list(["a{{0-1}}", "b{{0-1}}"])
        assert result == ["a0", "a1", "b0", "b1"]

    def test_expand_list_no_templates(self):
        assert expand_list(["tag1", "tag2"]) == ["tag1", "tag2"]

    def test_ten_values(self):
        result = expand_template("value{{0-9}}")
        assert len(result) == 10
        assert result[0] == "value0"
        assert result[9] == "value9"


# ---------------------------------------------------------------------------
# Context pool
# ---------------------------------------------------------------------------

def _make_cfg(**kwargs) -> DogStatsDConfig:
    base = dict(
        contexts=ConfRange(inclusive=InclusiveRange(min=10, max=10)),
        tags_per_msg=ConfRange(inclusive=InclusiveRange(min=2, max=2)),
        metric_names=["metric{{0-2}}"],
        tag_names=["env", "host"],
        tag_values=["prod{{0-1}}"],
    )
    base.update(kwargs)
    return DogStatsDConfig(**base)


class TestContextPool:
    def test_correct_count(self):
        cfg = _make_cfg()
        rng = random.Random(1)
        pool = build_context_pool(cfg, rng)
        assert len(pool) == 10

    def test_names_from_expanded_templates(self):
        cfg = _make_cfg()
        rng = random.Random(1)
        pool = build_context_pool(cfg, rng)
        valid_names = {"metric0", "metric1", "metric2"}
        for ctx in pool:
            assert ctx.name in valid_names

    def test_tags_are_key_value_pairs(self):
        cfg = _make_cfg()
        rng = random.Random(1)
        pool = build_context_pool(cfg, rng)
        for ctx in pool:
            assert len(ctx.base_tags) == 2
            for tag in ctx.base_tags:
                assert ":" in tag

    def test_tag_names_from_config(self):
        cfg = _make_cfg()
        rng = random.Random(1)
        pool = build_context_pool(cfg, rng)
        valid_tag_names = {"env", "host"}
        valid_tag_values = {"prod0", "prod1"}
        for ctx in pool:
            for tag in ctx.base_tags:
                k, v = tag.split(":", 1)
                assert k in valid_tag_names
                assert v in valid_tag_values


# ---------------------------------------------------------------------------
# Block generation
# ---------------------------------------------------------------------------

class TestGenerateBlock:
    def _metric_only_cfg(self) -> DogStatsDConfig:
        return _make_cfg(
            kind_weights=KindWeights(metric=1, event=0, service_check=0),
            metric_weights=MetricWeights(distribution=1, gauge=0, count=0, timer=0, set=0, histogram=0),
            multivalue_pack_probability=0.0,
        )

    def test_single_metric_call(self):
        cfg = self._metric_only_cfg()
        rng = random.Random(42)
        contexts = build_context_pool(cfg, rng)
        block = generate_block(rng, cfg, contexts)
        assert isinstance(block, MetricCall)
        assert block.metric_type == "distribution"

    def test_all_metric_types_reachable(self):
        cfg = _make_cfg(
            kind_weights=KindWeights(metric=1, event=0, service_check=0),
            metric_weights=MetricWeights(count=1, gauge=1, timer=1, distribution=1, set=1, histogram=1),
            multivalue_pack_probability=0.0,
        )
        rng = random.Random(0)
        contexts = build_context_pool(cfg, rng)
        seen_types = set()
        for _ in range(500):
            b = generate_block(rng, cfg, contexts)
            if isinstance(b, MetricCall):
                seen_types.add(b.metric_type)
        # All six types should appear across 500 samples
        assert seen_types >= {"count", "gauge", "timing", "distribution", "set", "histogram"}

    def test_event_call_generated(self):
        cfg = _make_cfg(
            kind_weights=KindWeights(metric=0, event=1, service_check=0),
        )
        rng = random.Random(1)
        contexts = build_context_pool(cfg, rng)
        block = generate_block(rng, cfg, contexts)
        assert isinstance(block, EventCall)
        assert len(block.title) > 0
        assert len(block.text) > 0

    def test_service_check_generated(self):
        cfg = _make_cfg(
            kind_weights=KindWeights(metric=0, event=0, service_check=1),
        )
        rng = random.Random(1)
        contexts = build_context_pool(cfg, rng)
        block = generate_block(rng, cfg, contexts)
        assert isinstance(block, ServiceCheckCall)
        assert block.status in (0, 1, 2, 3)

    def test_multivalue_batch(self):
        cfg = _make_cfg(
            kind_weights=KindWeights(metric=1, event=0, service_check=0),
            multivalue_pack_probability=1.0,
            multivalue_count=ConfRange(inclusive=InclusiveRange(min=5, max=5)),
        )
        rng = random.Random(7)
        contexts = build_context_pool(cfg, rng)
        block = generate_block(rng, cfg, contexts)
        assert isinstance(block, list)
        assert len(block) == 5
        assert all(isinstance(m, MetricCall) for m in block)

    def test_sample_rate_present_and_absent(self):
        cfg = _make_cfg(
            kind_weights=KindWeights(metric=1, event=0, service_check=0),
            sampling_probability=0.5,
        )
        rng = random.Random(0)
        contexts = build_context_pool(cfg, rng)
        with_rate = without_rate = 0
        for _ in range(200):
            b = generate_block(rng, cfg, contexts)
            if isinstance(b, MetricCall):
                if b.sample_rate is not None:
                    with_rate += 1
                else:
                    without_rate += 1
        assert with_rate > 0
        assert without_rate > 0

    def test_sample_rate_in_range(self):
        cfg = _make_cfg(
            kind_weights=KindWeights(metric=1, event=0, service_check=0),
            sampling_probability=1.0,
            sampling_range=ConfRange(inclusive=InclusiveRange(min=0.1, max=0.5)),
        )
        rng = random.Random(0)
        contexts = build_context_pool(cfg, rng)
        for _ in range(50):
            b = generate_block(rng, cfg, contexts)
            if isinstance(b, MetricCall):
                assert b.sample_rate is not None
                assert 0.1 <= b.sample_rate <= 0.5


# ---------------------------------------------------------------------------
# Block cache
# ---------------------------------------------------------------------------

class TestBlockCache:
    def test_deterministic_with_same_seed(self):
        cfg = _make_cfg()
        seed = list(range(32))
        cache1 = BlockCache(cfg, seed, max_count=20)
        cache2 = BlockCache(cfg, seed, max_count=20)
        for _ in range(20):
            b1, b2 = cache1.next(), cache2.next()
            assert type(b1) == type(b2)
            if isinstance(b1, MetricCall) and isinstance(b2, MetricCall):
                assert b1.name == b2.name
                assert b1.metric_type == b2.metric_type

    def test_different_seeds_differ(self):
        cfg = _make_cfg()
        cache1 = BlockCache(cfg, list(range(32)), max_count=50)
        cache2 = BlockCache(cfg, list(reversed(range(32))), max_count=50)
        blocks1 = [cache1.next() for _ in range(50)]
        blocks2 = [cache2.next() for _ in range(50)]
        # Very unlikely all would match with different seeds
        assert any(
            (isinstance(b1, MetricCall) and isinstance(b2, MetricCall) and b1.name != b2.name)
            for b1, b2 in zip(blocks1, blocks2)
        )

    def test_wraps_around(self):
        cfg = _make_cfg()
        cache = BlockCache(cfg, list(range(32)), max_count=3)
        b0a = cache.next()
        b1a = cache.next()
        b2a = cache.next()
        b0b = cache.next()  # wraps
        if isinstance(b0a, MetricCall) and isinstance(b0b, MetricCall):
            assert b0a.name == b0b.name
            assert b0a.metric_type == b0b.metric_type

    def test_count_respected(self):
        cfg = _make_cfg()
        cache = BlockCache(cfg, list(range(32)), max_count=17)
        assert len(cache._blocks) == 17


# ---------------------------------------------------------------------------
# Byte estimation
# ---------------------------------------------------------------------------

class TestEstimateBlockBytes:
    def test_single_metric(self):
        m = MetricCall("my.metric", 1.0, "gauge", ["env:prod", "host:foo"], None)
        est = _estimate_block_bytes(m)
        assert est > 0

    def test_batch_larger_than_single(self):
        m = MetricCall("x", 1.0, "gauge", [], None)
        single = _estimate_block_bytes(m)
        batch = _estimate_block_bytes([m, m, m])
        assert batch == single * 3

    def test_event(self):
        e = EventCall("title", "text", [], None, None)
        assert _estimate_block_bytes(e) > 0

    def test_service_check(self):
        sc = ServiceCheckCall("check.name", 0, [])
        assert _estimate_block_bytes(sc) > 0
