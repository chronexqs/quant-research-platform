"""Unit tests for adp.features.strategies — all 8 feature strategies and the registry."""

from __future__ import annotations

import math

import polars as pl
import pytest
from polars.testing import assert_series_equal

from adp.features.strategies import (
    STRATEGY_REGISTRY,
    EWMAStrategy,
    FeatureStrategy,
    LogReturnsStrategy,
    MovingAverageStrategy,
    ReturnsStrategy,
    RollingMaxStrategy,
    RollingMinStrategy,
    RollingStdStrategy,
    VWAPStrategy,
)


# ── Helpers ──────────────────────────────────────────────────

def _prices_lf(prices: list[float]) -> pl.LazyFrame:
    return pl.LazyFrame({"price": prices})


def _ohlcv_lf() -> pl.LazyFrame:
    return pl.LazyFrame({
        "price": [100.0, 102.0, 101.0, 105.0, 103.0],
        "volume": [10.0, 20.0, 15.0, 25.0, 30.0],
    })


# ── Registry ─────────────────────────────────────────────────

@pytest.mark.unit
class TestStrategyRegistry:
    def test_all_eight_registered(self) -> None:
        expected = {
            "rolling_std", "moving_average", "ewma",
            "rolling_min", "rolling_max",
            "vwap", "returns", "log_returns",
        }
        assert set(STRATEGY_REGISTRY.keys()) == expected

    def test_all_conform_to_protocol(self) -> None:
        for name, strategy in STRATEGY_REGISTRY.items():
            assert isinstance(strategy, FeatureStrategy), f"{name} does not conform"


# ── MovingAverageStrategy ────────────────────────────────────

@pytest.mark.unit
class TestMovingAverageStrategy:
    def test_compute(self) -> None:
        lf = _prices_lf([1.0, 2.0, 3.0, 4.0, 5.0])
        strategy = MovingAverageStrategy()
        result = strategy.compute(lf, "sma_3", {"column": "price", "window": 3}).collect()
        sma = result["sma_3"]
        # First two are null (window=3), third is mean(1,2,3)=2.0
        assert sma[0] is None
        assert sma[1] is None
        assert sma[2] == pytest.approx(2.0)
        assert sma[3] == pytest.approx(3.0)
        assert sma[4] == pytest.approx(4.0)


# ── RollingStdStrategy ───────────────────────────────────────

@pytest.mark.unit
class TestRollingStdStrategy:
    def test_compute(self) -> None:
        lf = _prices_lf([10.0, 10.0, 10.0, 20.0, 20.0])
        strategy = RollingStdStrategy()
        result = strategy.compute(lf, "rstd_3", {"column": "price", "window": 3}).collect()
        rstd = result["rstd_3"]
        # Window of [10, 10, 10] has std=0
        assert rstd[2] == pytest.approx(0.0)
        # Window of [10, 10, 20] has non-zero std
        assert rstd[3] is not None and rstd[3] > 0


# ── EWMAStrategy ─────────────────────────────────────────────

@pytest.mark.unit
class TestEWMAStrategy:
    def test_compute(self) -> None:
        lf = _prices_lf([1.0, 2.0, 3.0, 4.0, 5.0])
        strategy = EWMAStrategy()
        result = strategy.compute(lf, "ewma_3", {"column": "price", "span": 3}).collect()
        ewma = result["ewma_3"]
        # EWMA should be monotonically increasing for monotonically increasing input
        for i in range(1, len(ewma)):
            assert ewma[i] >= ewma[i - 1]
        # Last value should be between input min and max
        assert 1.0 <= ewma[4] <= 5.0


# ── RollingMinStrategy ───────────────────────────────────────

@pytest.mark.unit
class TestRollingMinStrategy:
    def test_compute(self) -> None:
        lf = _prices_lf([5.0, 3.0, 8.0, 1.0, 9.0])
        strategy = RollingMinStrategy()
        result = strategy.compute(lf, "rmin_3", {"column": "price", "window": 3}).collect()
        rmin = result["rmin_3"]
        # At index 2, window is [5,3,8] -> min=3
        assert rmin[2] == pytest.approx(3.0)
        # At index 3, window is [3,8,1] -> min=1
        assert rmin[3] == pytest.approx(1.0)


# ── RollingMaxStrategy ───────────────────────────────────────

@pytest.mark.unit
class TestRollingMaxStrategy:
    def test_compute(self) -> None:
        lf = _prices_lf([5.0, 3.0, 8.0, 1.0, 9.0])
        strategy = RollingMaxStrategy()
        result = strategy.compute(lf, "rmax_3", {"column": "price", "window": 3}).collect()
        rmax = result["rmax_3"]
        # At index 2, window is [5,3,8] -> max=8
        assert rmax[2] == pytest.approx(8.0)
        # At index 4, window is [8,1,9] -> max=9
        assert rmax[4] == pytest.approx(9.0)


# ── VWAPStrategy ─────────────────────────────────────────────

@pytest.mark.unit
class TestVWAPStrategy:
    def test_compute(self) -> None:
        lf = _ohlcv_lf()
        strategy = VWAPStrategy()
        result = strategy.compute(
            lf, "vwap", {"price_column": "price", "volume_column": "volume"}
        ).collect()
        vwap = result["vwap"]
        # First row: cumsum(price*vol) / cumsum(vol) = (100*10)/(10) = 100
        assert vwap[0] == pytest.approx(100.0)
        # Second row: (100*10 + 102*20) / (10+20) = 3040/30 = 101.333...
        assert vwap[1] == pytest.approx(3040.0 / 30.0)


# ── ReturnsStrategy ──────────────────────────────────────────

@pytest.mark.unit
class TestReturnsStrategy:
    def test_compute(self) -> None:
        lf = _prices_lf([100.0, 110.0, 99.0])
        strategy = ReturnsStrategy()
        result = strategy.compute(lf, "ret", {"column": "price"}).collect()
        ret = result["ret"]
        assert ret[0] is None  # No previous value
        assert ret[1] == pytest.approx(0.1)  # (110-100)/100
        assert ret[2] == pytest.approx((99.0 - 110.0) / 110.0)


# ── LogReturnsStrategy ───────────────────────────────────────

@pytest.mark.unit
class TestLogReturnsStrategy:
    def test_compute(self) -> None:
        lf = _prices_lf([100.0, 110.0, 99.0])
        strategy = LogReturnsStrategy()
        result = strategy.compute(lf, "logret", {"column": "price"}).collect()
        logret = result["logret"]
        assert logret[0] is None  # No previous value
        assert logret[1] == pytest.approx(math.log(110.0 / 100.0))
        assert logret[2] == pytest.approx(math.log(99.0 / 110.0))
