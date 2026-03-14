"""Qlib-based multi-factor computation engine."""

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from config.settings import QLIB_DATA_DIR

logger = logging.getLogger(__name__)

# Default factor weights for composite scoring
DEFAULT_WEIGHTS = {
    "value": 0.25,      # PE/PB
    "quality": 0.20,     # ROE
    "momentum": 0.20,    # Price momentum
    "volatility": 0.15,  # Inverse volatility
    "liquidity": 0.10,   # Turnover
    "size": 0.10,        # Inverse market cap
}


class FactorEngine:
    """Qlib-based multi-factor computation engine for A-shares."""

    def __init__(self, qlib_dir: Path | None = None):
        self.qlib_dir = qlib_dir or QLIB_DATA_DIR
        self._initialized = False

    def _ensure_qlib(self) -> None:
        """Initialize Qlib if not already done."""
        if self._initialized:
            return
        try:
            import qlib
            qlib.init(provider_uri=str(self.qlib_dir), region="cn")
            self._initialized = True
        except Exception as e:
            logger.error(f"Qlib initialization failed: {e}")
            raise

    def get_alpha158_features(
        self,
        instruments: str | list[str],
        start_date: str = "2020-01-01",
        end_date: str = "2026-03-14",
    ) -> pd.DataFrame:
        """Get Qlib's built-in Alpha158 factor set.

        Alpha158 includes 158 factors covering:
        - Price-volume features (KBAR, KLEN, KMID, etc.)
        - Rolling statistics (ROC, MA, STD, BETA, RSQR, RESI, etc.)
        - Volume-price correlation
        - Volatility measures

        Args:
            instruments: 'csi300', 'csi500', or list of Qlib codes ['SH600519', ...]
            start_date: start date YYYY-MM-DD
            end_date: end date YYYY-MM-DD

        Returns:
            MultiIndex DataFrame [instrument, datetime] x [158 features]
        """
        self._ensure_qlib()
        from qlib.contrib.data.handler import Alpha158

        handler = Alpha158(
            instruments=instruments,
            start_time=start_date,
            end_time=end_date,
            fit_start_time=start_date,
            fit_end_time=end_date,
        )
        return handler.fetch(col_set="feature")

    def get_factor_exposure(
        self,
        instruments: str | list[str],
        date: str,
        factors: list[str] | None = None,
    ) -> pd.DataFrame:
        """Get factor exposures for instruments on a specific date.

        Args:
            instruments: market name or list of codes
            date: target date YYYY-MM-DD
            factors: specific factors to compute, None for all common ones

        Returns:
            DataFrame indexed by instrument with factor columns
        """
        self._ensure_qlib()
        from qlib.data import D

        if factors is None:
            factors = [
                "$close/Ref($close,20)-1",     # 20d momentum
                "$close/Ref($close,60)-1",     # 60d momentum
                "Std($close/Ref($close,1)-1, 20)",  # 20d volatility
                "Mean($volume, 20)",           # 20d avg volume
                "$close",                      # price level
            ]

        factor_names = [
            "momentum_20d", "momentum_60d", "volatility_20d",
            "avg_volume_20d", "close",
        ]

        try:
            data = D.features(
                instruments if isinstance(instruments, list) else D.list_instruments(
                    D.instruments(market=instruments), as_list=True
                ),
                fields=factors,
                start_time=date,
                end_time=date,
            )
            if data.empty:
                return pd.DataFrame()

            data.columns = factor_names[:len(data.columns)]
            return data.droplevel("datetime") if "datetime" in data.index.names else data

        except Exception as e:
            logger.error(f"Factor exposure query failed: {e}")
            return pd.DataFrame()

    def get_custom_factors(
        self,
        symbols: list[str],
        financial_data: dict[str, dict],
    ) -> pd.DataFrame:
        """Compute custom fundamental factors from pre-fetched financial data.

        Args:
            symbols: list of stock codes
            financial_data: dict mapping code -> {pe_ttm, pb, dv_ttm, total_mv, ...}

        Returns:
            DataFrame indexed by symbol with factor columns
        """
        records = []
        for symbol in symbols:
            fd = financial_data.get(symbol, {})
            if not fd:
                continue
            records.append({
                "symbol": symbol,
                "pe_ttm": fd.get("pe_ttm"),
                "pb": fd.get("pb"),
                "ps_ttm": fd.get("ps_ttm"),
                "dv_ttm": fd.get("dv_ttm"),
                "total_mv": fd.get("total_mv"),
            })

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records).set_index("symbol")
        return df

    def composite_score(
        self,
        symbols: list[str],
        factor_data: pd.DataFrame,
        weights: dict[str, float] | None = None,
    ) -> pd.DataFrame:
        """Compute weighted composite factor score.

        Args:
            symbols: list of stock codes
            factor_data: DataFrame with columns including PE, PB, momentum, etc.
            weights: factor weight dict, defaults to DEFAULT_WEIGHTS

        Returns:
            DataFrame[symbol, score, rank, factor_details]
        """
        if weights is None:
            weights = DEFAULT_WEIGHTS

        if factor_data.empty:
            return pd.DataFrame()

        scores = pd.DataFrame(index=factor_data.index)

        # Value score: lower PE/PB = higher score
        if "pe_ttm" in factor_data.columns:
            pe = factor_data["pe_ttm"].clip(lower=0)
            pe_valid = pe[pe > 0]
            if not pe_valid.empty:
                scores["value"] = 100 - pe_valid.rank(pct=True) * 100
            else:
                scores["value"] = 50.0

        if "pb" in factor_data.columns and "value" in scores.columns:
            pb = factor_data["pb"].clip(lower=0)
            pb_valid = pb[pb > 0]
            if not pb_valid.empty:
                pb_score = 100 - pb_valid.rank(pct=True) * 100
                scores["value"] = (scores["value"] + pb_score) / 2
        elif "pb" in factor_data.columns:
            pb = factor_data["pb"].clip(lower=0)
            pb_valid = pb[pb > 0]
            if not pb_valid.empty:
                scores["value"] = 100 - pb_valid.rank(pct=True) * 100

        # Momentum score: higher momentum = higher score
        if "momentum_20d" in factor_data.columns:
            scores["momentum"] = factor_data["momentum_20d"].rank(pct=True) * 100
        elif "momentum_60d" in factor_data.columns:
            scores["momentum"] = factor_data["momentum_60d"].rank(pct=True) * 100

        # Volatility score: lower volatility = higher score
        if "volatility_20d" in factor_data.columns:
            scores["volatility"] = 100 - factor_data["volatility_20d"].rank(pct=True) * 100

        # Liquidity score: moderate liquidity preferred
        if "avg_volume_20d" in factor_data.columns:
            scores["liquidity"] = factor_data["avg_volume_20d"].rank(pct=True) * 100

        # Size score: smaller = higher score (small-cap premium)
        if "total_mv" in factor_data.columns:
            mv = factor_data["total_mv"]
            mv_valid = mv[mv > 0]
            if not mv_valid.empty:
                scores["size"] = 100 - mv_valid.rank(pct=True) * 100

        # Quality score from dividend yield
        if "dv_ttm" in factor_data.columns:
            scores["quality"] = factor_data["dv_ttm"].rank(pct=True) * 100

        # Compute weighted composite
        total_weight = 0.0
        composite = pd.Series(0.0, index=scores.index)
        for factor_name, weight in weights.items():
            if factor_name in scores.columns:
                valid = scores[factor_name].notna()
                composite[valid] += scores[factor_name][valid] * weight
                total_weight += weight

        if total_weight > 0:
            composite = composite / total_weight * (sum(weights.values()) / 1.0)

        result = pd.DataFrame({
            "score": composite.round(1),
        })

        # Add individual factor scores
        for col in scores.columns:
            result[f"factor_{col}"] = scores[col].round(1)

        result["rank"] = result["score"].rank(ascending=False).astype(int)
        result = result.sort_values("rank")

        return result
