"""Unit tests for adp.features.strategies — all 11 feature strategies and the registry."""

from __future__ import annotations

import math

import polars as pl
import pytest

from adp.features.strategies import (
    STRATEGY_REGISTRY,
    CrossSectionalRankStrategy,
    EWMAStrategy,
    FeatureStrategy,
    LogReturnsStrategy,
    MovingAverageStrategy,
    RealizedVolatilityStrategy,
    ReturnsStrategy,
    RollingMaxStrategy,
    RollingMinStrategy,
    RollingStdStrategy,
    VWAPStrategy,
    ZScoreStrategy,
)

# ── Helpers ──────────────────────────────────────────────────


def _prices_lf(prices: list[float]) -> pl.LazyFrame:
    """Create a single-column ``LazyFrame`` with the given price series.

    Args:
        prices: List of float values for the ``"price"`` column.

    Returns:
        A ``pl.LazyFrame`` with one column named ``"price"``.
    """
    return pl.LazyFrame({"price": prices})


def _ohlcv_lf() -> pl.LazyFrame:
    """Create a small price-and-volume ``LazyFrame`` for VWAP-style tests.

    Returns a five-row ``LazyFrame`` with ``"price"`` and ``"volume"``
    columns containing deterministic values.

    Returns:
        A ``pl.LazyFrame`` with ``"price"`` and ``"volume"`` columns.
    """
    return pl.LazyFrame(
        {
            "price": [100.0, 102.0, 101.0, 105.0, 103.0],
            "volume": [10.0, 20.0, 15.0, 25.0, 30.0],
        }
    )


# ── Registry ─────────────────────────────────────────────────


@pytest.mark.unit
class TestStrategyRegistry:
    def test_all_strategies_registered(self) -> None:
        expected = {
            "rolling_std",
            "moving_average",
            "ewma",
            "rolling_min",
            "rolling_max",
            "vwap",
            "returns",
            "log_returns",
            "z_score",
            "realized_volatility",
            "cross_sectional_rank",
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

    def test_zero_price_produces_null(self) -> None:
        """Zero previous price should produce null, not inf."""
        lf = _prices_lf([100.0, 0.0, 50.0])
        strategy = ReturnsStrategy()
        result = strategy.compute(lf, "ret", {"column": "price"}).collect()
        ret = result["ret"]
        # (0-100)/100 = -1.0 (valid — prev=100 is non-zero)
        assert ret[1] == pytest.approx(-1.0)
        # (50-0)/0 would be inf → prev=0, guard produces null
        assert ret[2] is None


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

    def test_zero_price_produces_null(self) -> None:
        """Zero price should produce null, not -inf or inf."""
        lf = _prices_lf([100.0, 0.0, 50.0])
        strategy = LogReturnsStrategy()
        result = strategy.compute(lf, "logret", {"column": "price"}).collect()
        logret = result["logret"]
        # 0/100 = 0, ratio not > 0 → null
        assert logret[1] is None
        # 50/0: prev=0, abs(0) < 1e-15, so guard produces null (prevents inf)
        assert logret[2] is None

    def test_negative_price_produces_null(self) -> None:
        """Negative price ratio should produce null."""
        lf = _prices_lf([100.0, -50.0, 100.0])
        strategy = LogReturnsStrategy()
        result = strategy.compute(lf, "logret", {"column": "price"}).collect()
        logret = result["logret"]
        # -50/100 = -0.5, ratio < 0 → null
        assert logret[1] is None


# ── ZScoreStrategy ──────────────────────────────────────────


@pytest.mark.unit
class TestZScoreStrategy:
    def test_compute(self) -> None:
        lf = _prices_lf([1.0, 2.0, 3.0, 4.0, 5.0])
        strategy = ZScoreStrategy()
        result = strategy.compute(lf, "zscore", {"column": "price", "window": 3}).collect()
        zscore = result["zscore"]
        # First two are null (window=3)
        assert zscore[0] is None
        assert zscore[1] is None
        # At index 2: mean([1,2,3])=2.0, std([1,2,3])=1.0, z=(3-2)/1=1.0
        assert zscore[2] == pytest.approx(1.0)

    def test_constant_values_produce_null(self) -> None:
        """When all values in window are identical, std=0 → null (not NaN/inf)."""
        lf = _prices_lf([5.0, 5.0, 5.0, 5.0, 5.0])
        strategy = ZScoreStrategy()
        result = strategy.compute(lf, "zscore", {"column": "price", "window": 3}).collect()
        zscore = result["zscore"]
        # std=0 for constant values → null guard should kick in
        for i in range(2, 5):
            assert zscore[i] is None


# ── RealizedVolatilityStrategy ──────────────────────────────


@pytest.mark.unit
class TestRealizedVolatilityStrategy:
    def test_compute(self) -> None:
        lf = _prices_lf([100.0, 102.0, 101.0, 105.0, 103.0])
        strategy = RealizedVolatilityStrategy()
        result = strategy.compute(lf, "rvol", {"column": "price", "window": 3}).collect()
        rvol = result["rvol"]
        # First values are null due to pct_change + rolling window
        assert rvol[0] is None
        # At index 3+, we should have valid values
        assert rvol[4] is not None and rvol[4] > 0

    def test_constant_prices_zero_vol(self) -> None:
        """Constant prices → zero returns → zero realized vol."""
        lf = _prices_lf([100.0, 100.0, 100.0, 100.0, 100.0])
        strategy = RealizedVolatilityStrategy()
        result = strategy.compute(lf, "rvol", {"column": "price", "window": 3}).collect()
        rvol = result["rvol"]
        # pct_change of constant = 0, so sum of squared returns = 0, sqrt(0) = 0
        assert rvol[4] == pytest.approx(0.0)

    def test_zero_price_produces_null_not_inf(self) -> None:
        """Zero price should produce null returns, not inf realized vol."""
        lf = _prices_lf([100.0, 0.0, 50.0, 60.0, 70.0])
        strategy = RealizedVolatilityStrategy()
        result = strategy.compute(lf, "rvol", {"column": "price", "window": 3}).collect()
        rvol = result["rvol"]
        # No value should be inf
        for val in rvol.to_list():
            if val is not None:
                assert not math.isinf(val), f"Got inf in realized vol: {rvol.to_list()}"


# ── CrossSectionalRankStrategy ──────────────────────────────


@pytest.mark.unit
class TestCrossSectionalRankStrategy:
    def test_compute(self) -> None:
        lf = pl.LazyFrame(
            {
                "symbol": ["A", "A", "A", "B", "B", "B"],
                "price": [10.0, 30.0, 20.0, 5.0, 15.0, 25.0],
            }
        )
        strategy = CrossSectionalRankStrategy()
        result = strategy.compute(lf, "rank", {"column": "price", "group_by": "symbol"}).collect()
        rank = result["rank"]
        # Within group A: 10→rank 1, 30→rank 3, 20→rank 2
        assert rank[0] == pytest.approx(1.0)
        assert rank[1] == pytest.approx(3.0)
        assert rank[2] == pytest.approx(2.0)

    def test_tied_values_average_rank(self) -> None:
        """Tied values should get average rank (method='average')."""
        lf = pl.LazyFrame(
            {
                "symbol": ["A", "A", "A"],
                "price": [10.0, 10.0, 20.0],
            }
        )
        strategy = CrossSectionalRankStrategy()
        result = strategy.compute(lf, "rank", {"column": "price", "group_by": "symbol"}).collect()
        rank = result["rank"]
        # Two tied at 10.0 → average of ranks 1,2 → 1.5 each
        assert rank[0] == pytest.approx(1.5)
        assert rank[1] == pytest.approx(1.5)
        assert rank[2] == pytest.approx(3.0)


# ── VWAPStrategy edge cases ─────────────────────────────────


@pytest.mark.unit
class TestVWAPStrategyEdgeCases:
    def test_zero_volume_produces_null(self) -> None:
        """Zero volume should produce null VWAP, not div-by-zero."""
        lf = pl.LazyFrame(
            {
                "price": [100.0, 200.0, 300.0],
                "volume": [0.0, 0.0, 0.0],
            }
        )
        strategy = VWAPStrategy()
        result = strategy.compute(
            lf, "vwap", {"price_column": "price", "volume_column": "volume"}
        ).collect()
        vwap = result["vwap"]
        for i in range(3):
            assert vwap[i] is None

    def test_windowed_zero_volume_produces_null(self) -> None:
        """Zero volume within a rolling window should produce null."""
        lf = pl.LazyFrame(
            {
                "price": [100.0, 200.0, 300.0, 400.0],
                "volume": [0.0, 0.0, 0.0, 10.0],
            }
        )
        strategy = VWAPStrategy()
        result = strategy.compute(
            lf, "vwap", {"price_column": "price", "volume_column": "volume", "window": 3}
        ).collect()
        vwap = result["vwap"]
        # First 3 rows: all zero volume → null
        assert vwap[0] is None
        assert vwap[1] is None
        assert vwap[2] is None
