"""Unified data fetcher abstracting AKShare and BaoStock."""

import logging
from datetime import date, datetime

import akshare as ak
import baostock as bs
import pandas as pd

from config.settings import DEFAULT_START_DATE, symbol_to_qlib
from data.cache import DataCache

logger = logging.getLogger(__name__)


class StockDataFetcher:
    """Fetch A-share stock data from AKShare (primary) and BaoStock (backup)."""

    def __init__(self, cache: DataCache | None = None):
        self.cache = cache or DataCache()
        self._stock_list_cache: pd.DataFrame | None = None

    # --- Real-time Quote ---

    def get_realtime_quote(self, symbol: str) -> dict:
        """Get current price, change%, volume for a single stock.

        Uses Sina Finance API (lightweight, single-stock) with EastMoney as fallback.

        Args:
            symbol: Stock code like '000001' or '600519', or stock name like '贵州茅台'

        Returns:
            dict with keys: code, name, price, change_pct, change_amount,
                           volume, amount, open, high, low, prev_close, timestamp
        """
        code = self._resolve_symbol(symbol)
        try:
            return self._quote_sina(code)
        except Exception as e:
            logger.warning(f"Sina quote failed for {code}: {e}, trying EastMoney")
            return self._quote_em(code)

    def _quote_sina(self, code: str) -> dict:
        """Get realtime quote from Sina Finance API (fast, single-stock)."""
        import requests as req

        prefix = "sh" if code.startswith(("6", "9")) else "sz"
        url = f"https://hq.sinajs.cn/list={prefix}{code}"
        headers = {"Referer": "https://finance.sina.com.cn"}
        r = req.get(url, headers=headers, timeout=10)
        r.encoding = "gbk"

        # Parse: var hq_str_sz002795="name,today_open,prev_close,price,high,low,..."
        text = r.text.strip()
        if '=""' in text or not text:
            raise ValueError(f"Empty Sina response for {code}")

        data_str = text.split('"')[1]
        fields = data_str.split(",")
        if len(fields) < 32:
            raise ValueError(f"Unexpected Sina data format for {code}")

        name = fields[0]
        today_open = float(fields[1]) if fields[1] else 0
        prev_close = float(fields[2]) if fields[2] else 0
        price = float(fields[3]) if fields[3] else 0
        high = float(fields[4]) if fields[4] else 0
        low = float(fields[5]) if fields[5] else 0
        volume = float(fields[8]) if fields[8] else 0  # shares
        amount = float(fields[9]) if fields[9] else 0   # yuan

        change_amount = price - prev_close if prev_close > 0 else 0
        change_pct = (change_amount / prev_close * 100) if prev_close > 0 else 0

        # Get extra info (market cap, turnover) from EastMoney individual info
        pe, pb, total_mv, turnover_rate = None, None, 0, 0
        try:
            info_df = ak.stock_individual_info_em(symbol=code)
            info_dict = dict(zip(info_df["item"], info_df["value"]))
            total_mv = float(info_dict.get("总市值", 0))
            float_shares = float(info_dict.get("流通股", 0))
            if float_shares > 0 and volume > 0:
                turnover_rate = round(volume / float_shares * 100, 2)
        except Exception as e:
            logger.debug(f"Individual info failed for {code}: {e}")

        return {
            "code": code,
            "name": name,
            "price": price,
            "change_pct": round(change_pct, 2),
            "change_amount": round(change_amount, 2),
            "volume": volume,
            "amount": amount,
            "open": today_open,
            "high": high,
            "low": low,
            "prev_close": prev_close,
            "turnover_rate": turnover_rate,
            "pe": pe,
            "pb": pb,
            "total_market_cap": total_mv,
            "timestamp": datetime.now().isoformat(),
        }

    def _quote_em(self, code: str) -> dict:
        """Fallback: get realtime quote from EastMoney full market data."""
        df = ak.stock_zh_a_spot_em()
        row = df[df["代码"] == code]
        if row.empty:
            raise ValueError(f"Stock not found: {code}")
        r = row.iloc[0]
        return {
            "code": str(r["代码"]),
            "name": str(r["名称"]),
            "price": float(r.get("最新价", 0)),
            "change_pct": float(r.get("涨跌幅", 0)),
            "change_amount": float(r.get("涨跌额", 0)),
            "volume": float(r.get("成交量", 0)),
            "amount": float(r.get("成交额", 0)),
            "open": float(r.get("今开", 0)),
            "high": float(r.get("最高", 0)),
            "low": float(r.get("最低", 0)),
            "prev_close": float(r.get("昨收", 0)),
            "turnover_rate": float(r.get("换手率", 0)),
            "pe": float(r.get("市盈率-动态", 0)) if r.get("市盈率-动态") else None,
            "pb": float(r.get("市净率", 0)) if r.get("市净率") else None,
            "total_market_cap": float(r.get("总市值", 0)),
            "timestamp": datetime.now().isoformat(),
        }

    # --- Daily History ---

    def get_daily_history(
        self,
        symbol: str,
        start_date: str = DEFAULT_START_DATE,
        end_date: str | None = None,
        adjust: str = "qfq",
        source: str = "akshare",
    ) -> pd.DataFrame:
        """Fetch daily OHLCV data. Checks cache first, fetches delta only.

        Args:
            symbol: Stock code like '600519'
            start_date: Start date string 'YYYYMMDD'
            end_date: End date string 'YYYYMMDD', defaults to today
            adjust: 'qfq' (forward), 'hfq' (backward), '' (none)
            source: 'akshare' or 'baostock'

        Returns:
            DataFrame with columns: date, open, close, high, low, volume, amount
        """
        code = self._resolve_symbol(symbol)
        if end_date is None:
            end_date = date.today().strftime("%Y%m%d")

        # Normalize date format to YYYY-MM-DD for cache
        start_norm = self._normalize_date(start_date)
        end_norm = self._normalize_date(end_date)

        # Check cache
        cached = self.cache.get_daily(code, start_norm, end_norm)
        if cached is not None and len(cached) > 0:
            cached_range = self.cache.get_cached_range(code)
            if cached_range and cached_range[0] <= start_norm and cached_range[1] >= end_norm:
                logger.info(f"Cache hit for {code} [{start_norm}, {end_norm}]")
                return cached

        # Fetch from source (try AKShare first, fallback to BaoStock)
        df = None
        if source == "baostock":
            df = self._fetch_baostock(code, start_norm, end_norm, adjust)
        else:
            df = self._fetch_akshare(code, start_date, end_date, adjust)
            if df is None or df.empty:
                logger.info(f"AKShare failed for {code}, falling back to BaoStock")
                df = self._fetch_baostock(code, start_norm, end_norm, adjust)

        if df is not None and not df.empty:
            self.cache.store_daily(code, df)

        return df if df is not None else pd.DataFrame()

    def _fetch_akshare(
        self, code: str, start_date: str, end_date: str, adjust: str
    ) -> pd.DataFrame | None:
        """Fetch daily data from AKShare."""
        try:
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            )
            if df.empty:
                return None
            df = df.rename(columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "amount",
            })
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            return df[["date", "open", "close", "high", "low", "volume", "amount"]]
        except Exception as e:
            logger.error(f"AKShare fetch failed for {code}: {e}")
            return None

    def _fetch_baostock(
        self, code: str, start_date: str, end_date: str, adjust: str
    ) -> pd.DataFrame | None:
        """Fetch daily data from BaoStock."""
        try:
            bs.login()
            # BaoStock needs market prefix: sh.600519 or sz.000001
            bs_code = f"sh.{code}" if code.startswith(("6", "9")) else f"sz.{code}"
            adj_map = {"qfq": "2", "hfq": "1", "": "3"}
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,open,high,low,close,volume,amount",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag=adj_map.get(adjust, "3"),
            )
            rows = []
            while (rs.error_code == "0") and rs.next():
                rows.append(rs.get_row_data())
            bs.logout()

            if not rows:
                return None
            df = pd.DataFrame(rows, columns=rs.fields)
            for col in ["open", "high", "low", "close", "volume", "amount"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            return df
        except Exception as e:
            logger.error(f"BaoStock fetch failed for {code}: {e}")
            return None

    # --- Stock List ---

    def get_stock_list(self, refresh: bool = False) -> pd.DataFrame:
        """Get all A-share stock codes and names.

        Returns:
            DataFrame with columns: symbol, name, market, industry
        """
        if self._stock_list_cache is not None and not refresh:
            return self._stock_list_cache

        df = ak.stock_zh_a_spot_em()
        result = pd.DataFrame({
            "symbol": df["代码"].astype(str),
            "name": df["名称"].astype(str),
        })
        # Determine market
        result["market"] = result["symbol"].apply(
            lambda x: "SH" if x.startswith(("6", "9")) else "SZ"
        )
        result["industry"] = ""  # Can be enriched later
        self._stock_list_cache = result

        # Update cache
        self.cache.store_stock_info(result)
        return result

    def search_stock(self, query: str) -> list[dict]:
        """Fuzzy search by name or code fragment."""
        # Try cache first
        results = self.cache.search_stock(query)
        if results:
            return results

        # Fallback to live data
        stock_list = self.get_stock_list()
        mask = stock_list["symbol"].str.contains(query) | stock_list["name"].str.contains(query)
        matches = stock_list[mask].head(20)
        return matches.to_dict("records")

    # --- Index Components ---

    def get_index_components(self, index: str = "000300") -> list[str]:
        """Get constituent stock codes for CSI300/CSI500/etc."""
        index_map = {
            "000300": "沪深300",
            "000905": "中证500",
            "000852": "中证1000",
        }
        try:
            df = ak.index_stock_cons(symbol=index)
            return df["品种代码"].tolist()
        except Exception:
            logger.warning(f"Failed to get index components for {index}, trying alternative")
            try:
                df = ak.index_stock_cons_csindex(symbol=index)
                return df["成分券代码"].tolist()
            except Exception as e:
                logger.error(f"Index components fetch failed: {e}")
                return []

    # --- Financial Data ---

    def get_financial_data(self, symbol: str) -> dict:
        """Get fundamental data: PE, PB, ROE, market_cap, etc.

        Combines data from:
        - stock_individual_info_em: market cap, shares
        - stock_financial_analysis_indicator: ROE, profit margins
        - Computed PE/PB from price and financial data
        """
        code = self._resolve_symbol(symbol)
        result = {"code": code}

        # Get basic info (market cap, shares)
        try:
            info_df = ak.stock_individual_info_em(symbol=code)
            info_dict = dict(zip(info_df["item"], info_df["value"]))
            result["total_mv"] = float(info_dict.get("总市值", 0))
            result["float_mv"] = float(info_dict.get("流通市值", 0))
            result["industry"] = info_dict.get("行业", "")
        except Exception as e:
            logger.warning(f"Individual info failed for {code}: {e}")

        # Get financial indicators (ROE, profit margins, etc.)
        try:
            fin_df = ak.stock_financial_analysis_indicator(symbol=code, start_year="2024")
            if not fin_df.empty:
                latest = fin_df.iloc[-1]
                result["roe"] = self._safe_float(latest.get("净资产收益率(%)"))
                result["gross_margin"] = self._safe_float(latest.get("销售毛利率(%)"))
                result["net_margin"] = self._safe_float(latest.get("销售净利率(%)"))
                result["revenue_growth"] = self._safe_float(latest.get("主营业务收入增长率(%)"))
                result["profit_growth"] = self._safe_float(latest.get("净利润增长率(%)"))
                result["eps"] = self._safe_float(latest.get("摊薄每股收益(元)"))
                result["bps"] = self._safe_float(latest.get("每股净资产_调整后(元)"))
                result["debt_ratio"] = self._safe_float(latest.get("资产负债率(%)"))
                result["current_ratio"] = self._safe_float(latest.get("流动比率"))
                result["report_date"] = str(latest.get("日期", ""))

                # Compute PE and PB from price
                quote = self._quote_sina(code)
                price = quote.get("price", 0)
                if price > 0:
                    eps = result.get("eps")
                    bps = result.get("bps")
                    if eps and eps > 0:
                        result["pe_ttm"] = round(price / eps, 2)
                    if bps and bps > 0:
                        result["pb"] = round(price / bps, 2)
        except Exception as e:
            logger.warning(f"Financial analysis failed for {code}: {e}")

        return result

    @staticmethod
    def _safe_float(val) -> float | None:
        """Safely convert to float, returning None for invalid values."""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    # --- Helpers ---

    def _resolve_symbol(self, query: str) -> str:
        """Resolve a stock name or code to bare code (e.g. '600519')."""
        query = query.strip()
        # Already a code
        if query.isdigit() and len(query) == 6:
            return query
        # Strip prefix
        if query.startswith(("SH", "SZ", "sh", "sz")):
            return query[2:]
        # Try name search
        results = self.cache.search_stock(query)
        if results:
            return results[0]["symbol"]
        # Fallback: search live
        matches = self.search_stock(query)
        if matches:
            return matches[0]["symbol"]
        raise ValueError(f"Cannot resolve stock symbol: {query}")

    @staticmethod
    def _normalize_date(date_str: str) -> str:
        """Normalize date string to YYYY-MM-DD format."""
        date_str = date_str.replace("-", "").replace("/", "")
        if len(date_str) == 8:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        return date_str
