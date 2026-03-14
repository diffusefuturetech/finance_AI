"""Multi-factor stock screening."""

import logging
from dataclasses import dataclass, field

import pandas as pd

from data.fetcher import StockDataFetcher
from analysis.factor import FactorEngine

logger = logging.getLogger(__name__)


@dataclass
class ScreenCriteria:
    """Stock screening criteria."""
    pe_max: float | None = None
    pe_min: float | None = None
    pb_max: float | None = None
    pb_min: float | None = None
    roe_min: float | None = None
    market_cap_min: float | None = None   # in 亿元
    market_cap_max: float | None = None
    momentum_days: int = 60
    momentum_min: float | None = None     # min return %
    industry: str | None = None
    top_n: int = 20


class StockScreener:
    """Multi-factor stock screening combining fundamental + quantitative factors."""

    def __init__(
        self,
        fetcher: StockDataFetcher | None = None,
        factor_engine: FactorEngine | None = None,
    ):
        self.fetcher = fetcher or StockDataFetcher()
        self.factor_engine = factor_engine or FactorEngine()

    def screen(
        self,
        criteria: ScreenCriteria,
        market: str = "csi300",
    ) -> pd.DataFrame:
        """Apply filters and rank stocks by composite factor score.

        Args:
            criteria: screening criteria
            market: 'csi300', 'csi500', or 'all'

        Returns:
            DataFrame[code, name, score, pe, pb, change_pct, market_cap, ...]
            sorted by composite score descending
        """
        # Step 1: Get candidate stocks with real-time data
        import akshare as ak
        spot_df = ak.stock_zh_a_spot_em()

        # Filter by market index components if needed
        if market != "all":
            components = self.fetcher.get_index_components(
                {"csi300": "000300", "csi500": "000905", "csi1000": "000852"}.get(market, "000300")
            )
            if components:
                spot_df = spot_df[spot_df["代码"].isin(components)]

        # Step 2: Build factor dataframe from spot data
        df = pd.DataFrame({
            "code": spot_df["代码"].astype(str),
            "name": spot_df["名称"].astype(str),
            "price": pd.to_numeric(spot_df.get("最新价", 0), errors="coerce"),
            "change_pct": pd.to_numeric(spot_df.get("涨跌幅", 0), errors="coerce"),
            "pe_ttm": pd.to_numeric(spot_df.get("市盈率-动态", 0), errors="coerce"),
            "pb": pd.to_numeric(spot_df.get("市净率", 0), errors="coerce"),
            "total_mv": pd.to_numeric(spot_df.get("总市值", 0), errors="coerce"),
            "turnover_rate": pd.to_numeric(spot_df.get("换手率", 0), errors="coerce"),
            "volume": pd.to_numeric(spot_df.get("成交量", 0), errors="coerce"),
            "amount": pd.to_numeric(spot_df.get("成交额", 0), errors="coerce"),
        }).set_index("code")

        # Convert market cap from yuan to 亿元
        df["market_cap_yi"] = df["total_mv"] / 1e8

        # Step 3: Apply filters
        mask = pd.Series(True, index=df.index)

        if criteria.pe_max is not None:
            mask &= (df["pe_ttm"] > 0) & (df["pe_ttm"] <= criteria.pe_max)
        if criteria.pe_min is not None:
            mask &= df["pe_ttm"] >= criteria.pe_min
        if criteria.pb_max is not None:
            mask &= (df["pb"] > 0) & (df["pb"] <= criteria.pb_max)
        if criteria.pb_min is not None:
            mask &= df["pb"] >= criteria.pb_min
        if criteria.market_cap_min is not None:
            mask &= df["market_cap_yi"] >= criteria.market_cap_min
        if criteria.market_cap_max is not None:
            mask &= df["market_cap_yi"] <= criteria.market_cap_max

        # Filter out ST stocks and abnormal data
        mask &= ~df["name"].str.contains("ST|退市", na=False)
        mask &= df["price"] > 0

        df = df[mask]

        if df.empty:
            logger.warning("No stocks match the criteria")
            return pd.DataFrame()

        # Step 4: Compute composite score
        factor_data = df[["pe_ttm", "pb", "total_mv"]].copy()
        if "dv_ttm" in df.columns:
            factor_data["dv_ttm"] = df["dv_ttm"]

        scores = self.factor_engine.composite_score(
            symbols=df.index.tolist(),
            factor_data=factor_data,
        )

        if scores.empty:
            # Fallback: rank by PE
            df["score"] = (100 - df["pe_ttm"].rank(pct=True) * 100).round(1)
            df["rank"] = df["score"].rank(ascending=False).astype(int)
        else:
            df = df.join(scores[["score", "rank"]])

        # Step 5: Sort and return top_n
        df = df.sort_values("score", ascending=False).head(criteria.top_n)
        df["rank"] = range(1, len(df) + 1)

        result_cols = ["name", "price", "change_pct", "pe_ttm", "pb",
                       "market_cap_yi", "turnover_rate", "score", "rank"]
        available = [c for c in result_cols if c in df.columns]
        return df[available].reset_index()

    def value_picks(self, top_n: int = 10, market: str = "csi300") -> pd.DataFrame:
        """Pre-configured screen: low PE/PB value stocks."""
        criteria = ScreenCriteria(
            pe_max=20,
            pe_min=1,
            pb_max=3,
            pb_min=0.1,
            market_cap_min=100,  # >= 100亿
            top_n=top_n,
        )
        return self.screen(criteria, market=market)

    def momentum_picks(self, top_n: int = 10, market: str = "csi300") -> pd.DataFrame:
        """Pre-configured screen: strong momentum stocks."""
        criteria = ScreenCriteria(
            pe_min=1,
            market_cap_min=50,
            top_n=top_n,
        )
        result = self.screen(criteria, market=market)
        # Re-sort by change_pct as momentum proxy
        if not result.empty and "change_pct" in result.columns:
            result = result.sort_values("change_pct", ascending=False).head(top_n)
            result["rank"] = range(1, len(result) + 1)
        return result

    def quality_picks(self, top_n: int = 10, market: str = "csi300") -> pd.DataFrame:
        """Pre-configured screen: high quality stocks (high ROE, reasonable PE)."""
        criteria = ScreenCriteria(
            pe_max=40,
            pe_min=5,
            pb_min=1,
            market_cap_min=200,  # >= 200亿
            top_n=top_n,
        )
        return self.screen(criteria, market=market)

    def growth_picks(self, top_n: int = 10, market: str = "csi300") -> pd.DataFrame:
        """Pre-configured screen: growth stocks (moderate PE, high market cap)."""
        criteria = ScreenCriteria(
            pe_min=20,
            pe_max=80,
            market_cap_min=100,
            top_n=top_n,
        )
        return self.screen(criteria, market=market)
