"""Feature computation strategies — 11 implementations of the FeatureStrategy protocol."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

import polars as pl


@runtime_checkable
class FeatureStrategy(Protocol):
    """Protocol for feature computation strategies."""

    def compute(
        self, lf: pl.LazyFrame, feature_name: str, params: dict[str, Any]
    ) -> pl.LazyFrame: ...


class RollingStdStrategy:
    def compute(
        self, lf: pl.LazyFrame, feature_name: str, params: dict[str, Any]
    ) -> pl.LazyFrame:
        return lf.with_columns(
            pl.col(params["column"])
            .rolling_std(window_size=params["window"])
            .alias(feature_name)
        )


class MovingAverageStrategy:
    def compute(
        self, lf: pl.LazyFrame, feature_name: str, params: dict[str, Any]
    ) -> pl.LazyFrame:
        return lf.with_columns(
            pl.col(params["column"])
            .rolling_mean(window_size=params["window"])
            .alias(feature_name)
        )


class EWMAStrategy:
    def compute(
        self, lf: pl.LazyFrame, feature_name: str, params: dict[str, Any]
    ) -> pl.LazyFrame:
        return lf.with_columns(
            pl.col(params["column"]).ewm_mean(span=params["span"]).alias(feature_name)
        )


class RollingMinStrategy:
    def compute(
        self, lf: pl.LazyFrame, feature_name: str, params: dict[str, Any]
    ) -> pl.LazyFrame:
        return lf.with_columns(
            pl.col(params["column"])
            .rolling_min(window_size=params["window"])
            .alias(feature_name)
        )


class RollingMaxStrategy:
    def compute(
        self, lf: pl.LazyFrame, feature_name: str, params: dict[str, Any]
    ) -> pl.LazyFrame:
        return lf.with_columns(
            pl.col(params["column"])
            .rolling_max(window_size=params["window"])
            .alias(feature_name)
        )


class VWAPStrategy:
    def compute(
        self, lf: pl.LazyFrame, feature_name: str, params: dict[str, Any]
    ) -> pl.LazyFrame:
        price_col = params["price_column"]
        vol_col = params["volume_column"]
        window = params.get("window")
        pv = pl.col(price_col) * pl.col(vol_col)
        if window:
            vol_sum = pl.col(vol_col).rolling_sum(window_size=window)
            vwap = pl.when(vol_sum.abs() > 1e-15).then(
                pv.rolling_sum(window_size=window) / vol_sum
            ).otherwise(pl.lit(None, dtype=pl.Float64))
        else:
            vol_cum = pl.col(vol_col).cum_sum()
            vwap = pl.when(vol_cum.abs() > 1e-15).then(
                pv.cum_sum() / vol_cum
            ).otherwise(pl.lit(None, dtype=pl.Float64))
        return lf.with_columns(vwap.alias(feature_name))


class ReturnsStrategy:
    def compute(
        self, lf: pl.LazyFrame, feature_name: str, params: dict[str, Any]
    ) -> pl.LazyFrame:
        col = params["column"]
        prev = pl.col(col).shift(1)
        ret = pl.when(prev.abs() > 1e-15).then(
            (pl.col(col) - prev) / prev
        ).otherwise(pl.lit(None, dtype=pl.Float64))
        return lf.with_columns(ret.alias(feature_name))


class LogReturnsStrategy:
    def compute(
        self, lf: pl.LazyFrame, feature_name: str, params: dict[str, Any]
    ) -> pl.LazyFrame:
        col = params["column"]
        prev = pl.col(col).shift(1)
        ratio = pl.col(col) / prev
        safe_log = pl.when((prev.abs() > 1e-15) & (ratio > 0)).then(
            ratio.log()
        ).otherwise(pl.lit(None, dtype=pl.Float64))
        return lf.with_columns(safe_log.alias(feature_name))


class ZScoreStrategy:
    def compute(
        self, lf: pl.LazyFrame, feature_name: str, params: dict[str, Any]
    ) -> pl.LazyFrame:
        col = params["column"]
        window = params["window"]
        mean = pl.col(col).rolling_mean(window_size=window)
        std = pl.col(col).rolling_std(window_size=window)
        z = pl.when(std > 1e-15).then(
            (pl.col(col) - mean) / std
        ).otherwise(pl.lit(None, dtype=pl.Float64))
        return lf.with_columns(z.alias(feature_name))


class RealizedVolatilityStrategy:
    def compute(
        self, lf: pl.LazyFrame, feature_name: str, params: dict[str, Any]
    ) -> pl.LazyFrame:
        col = params["column"]
        window = params["window"]
        prev = pl.col(col).shift(1)
        returns = pl.when(prev.abs() > 1e-15).then(
            (pl.col(col) - prev) / prev
        ).otherwise(pl.lit(None, dtype=pl.Float64))
        return lf.with_columns(
            (returns.pow(2).rolling_sum(window_size=window)).sqrt().alias(
                feature_name
            )
        )


class CrossSectionalRankStrategy:
    def compute(
        self, lf: pl.LazyFrame, feature_name: str, params: dict[str, Any]
    ) -> pl.LazyFrame:
        col = params["column"]
        group_col = params["group_by"]
        return lf.with_columns(
            pl.col(col).rank(method="average").over(group_col).alias(feature_name)
        )


STRATEGY_REGISTRY: dict[str, FeatureStrategy] = {
    "rolling_std": RollingStdStrategy(),
    "moving_average": MovingAverageStrategy(),
    "ewma": EWMAStrategy(),
    "rolling_min": RollingMinStrategy(),
    "rolling_max": RollingMaxStrategy(),
    "vwap": VWAPStrategy(),
    "returns": ReturnsStrategy(),
    "log_returns": LogReturnsStrategy(),
    "z_score": ZScoreStrategy(),
    "realized_volatility": RealizedVolatilityStrategy(),
    "cross_sectional_rank": CrossSectionalRankStrategy(),
}
