"""Technical indicators using TA-Lib."""

import numpy as np
import pandas as pd
import talib


class TechnicalAnalyzer:
    """Calculate technical indicators for a given stock's OHLCV data."""

    def __init__(self, df: pd.DataFrame):
        """Initialize with OHLCV DataFrame.

        Args:
            df: DataFrame with columns: date, open, high, low, close, volume
        """
        self.df = df.copy().sort_values("date").reset_index(drop=True)
        self.close = self.df["close"].values.astype(float)
        self.high = self.df["high"].values.astype(float)
        self.low = self.df["low"].values.astype(float)
        self.open = self.df["open"].values.astype(float)
        self.volume = self.df["volume"].values.astype(float)

    def macd(
        self, fastperiod: int = 12, slowperiod: int = 26, signalperiod: int = 9
    ) -> pd.DataFrame:
        """MACD indicator.

        Returns:
            DataFrame with columns: date, dif, dea, macd_hist
        """
        dif, dea, hist = talib.MACD(
            self.close,
            fastperiod=fastperiod,
            slowperiod=slowperiod,
            signalperiod=signalperiod,
        )
        return pd.DataFrame({
            "date": self.df["date"],
            "dif": dif,
            "dea": dea,
            "macd_hist": hist * 2,  # Chinese convention: MACD bar = 2 * (DIF - DEA)
        })

    def kdj(self, n: int = 9, m1: int = 3, m2: int = 3) -> pd.DataFrame:
        """KDJ indicator (Chinese-style).

        Returns:
            DataFrame with columns: date, k, d, j
        """
        slowk, slowd = talib.STOCH(
            self.high, self.low, self.close,
            fastk_period=n,
            slowk_period=m1,
            slowk_matype=0,
            slowd_period=m2,
            slowd_matype=0,
        )
        j = 3 * slowk - 2 * slowd
        return pd.DataFrame({
            "date": self.df["date"],
            "k": slowk,
            "d": slowd,
            "j": j,
        })

    def rsi(self, periods: list[int] | None = None) -> pd.DataFrame:
        """RSI indicator for multiple periods.

        Returns:
            DataFrame with columns: date, rsi_6, rsi_12, rsi_24
        """
        if periods is None:
            periods = [6, 12, 24]
        result = {"date": self.df["date"]}
        for p in periods:
            result[f"rsi_{p}"] = talib.RSI(self.close, timeperiod=p)
        return pd.DataFrame(result)

    def bollinger(
        self, period: int = 20, nbdevup: float = 2.0, nbdevdn: float = 2.0
    ) -> pd.DataFrame:
        """Bollinger Bands.

        Returns:
            DataFrame with columns: date, upper, middle, lower
        """
        upper, middle, lower = talib.BBANDS(
            self.close,
            timeperiod=period,
            nbdevup=nbdevup,
            nbdevdn=nbdevdn,
            matype=0,
        )
        return pd.DataFrame({
            "date": self.df["date"],
            "upper": upper,
            "middle": middle,
            "lower": lower,
        })

    def moving_averages(
        self, periods: list[int] | None = None
    ) -> pd.DataFrame:
        """Simple Moving Averages.

        Returns:
            DataFrame with MA columns: date, ma5, ma10, ma20, ma60, ma120, ma250
        """
        if periods is None:
            periods = [5, 10, 20, 60, 120, 250]
        result = {"date": self.df["date"], "close": self.close}
        for p in periods:
            if len(self.close) >= p:
                result[f"ma{p}"] = talib.SMA(self.close, timeperiod=p)
            else:
                result[f"ma{p}"] = np.nan
        return pd.DataFrame(result)

    def compute_all(self) -> dict[str, pd.DataFrame]:
        """Compute all indicators at once."""
        return {
            "macd": self.macd(),
            "kdj": self.kdj(),
            "rsi": self.rsi(),
            "bollinger": self.bollinger(),
            "ma": self.moving_averages(),
        }

    def generate_signals(self) -> dict:
        """Generate buy/sell/hold signals from indicator crossovers.

        Returns:
            dict with signal assessments and overall score (-100 to 100)
        """
        signals = {}
        score = 0.0

        # MACD signal
        macd_df = self.macd()
        if len(macd_df) >= 2:
            dif_now = macd_df["dif"].iloc[-1]
            dea_now = macd_df["dea"].iloc[-1]
            dif_prev = macd_df["dif"].iloc[-2]
            dea_prev = macd_df["dea"].iloc[-2]

            if not (np.isnan(dif_now) or np.isnan(dea_now)):
                if dif_prev <= dea_prev and dif_now > dea_now:
                    signals["macd_signal"] = "金叉（看多）"
                    score += 25
                elif dif_prev >= dea_prev and dif_now < dea_now:
                    signals["macd_signal"] = "死叉（看空）"
                    score -= 25
                elif dif_now > dea_now:
                    signals["macd_signal"] = "多头排列"
                    score += 10
                elif dif_now < dea_now:
                    signals["macd_signal"] = "空头排列"
                    score -= 10
                else:
                    signals["macd_signal"] = "中性"
            else:
                signals["macd_signal"] = "数据不足"
        else:
            signals["macd_signal"] = "数据不足"

        # RSI signal
        rsi_df = self.rsi([14])
        if not rsi_df.empty:
            rsi_val = rsi_df["rsi_14"].iloc[-1]
            if not np.isnan(rsi_val):
                if rsi_val > 80:
                    signals["rsi_signal"] = f"超买（{rsi_val:.1f}）"
                    score -= 20
                elif rsi_val > 70:
                    signals["rsi_signal"] = f"偏强（{rsi_val:.1f}）"
                    score -= 5
                elif rsi_val < 20:
                    signals["rsi_signal"] = f"超卖（{rsi_val:.1f}）"
                    score += 20
                elif rsi_val < 30:
                    signals["rsi_signal"] = f"偏弱（{rsi_val:.1f}）"
                    score += 5
                else:
                    signals["rsi_signal"] = f"中性（{rsi_val:.1f}）"
            else:
                signals["rsi_signal"] = "数据不足"
        else:
            signals["rsi_signal"] = "数据不足"

        # KDJ signal
        kdj_df = self.kdj()
        if len(kdj_df) >= 2:
            k_now = kdj_df["k"].iloc[-1]
            d_now = kdj_df["d"].iloc[-1]
            k_prev = kdj_df["k"].iloc[-2]
            d_prev = kdj_df["d"].iloc[-2]

            if not (np.isnan(k_now) or np.isnan(d_now)):
                if k_prev <= d_prev and k_now > d_now:
                    signals["kdj_signal"] = "金叉（看多）"
                    score += 20
                elif k_prev >= d_prev and k_now < d_now:
                    signals["kdj_signal"] = "死叉（看空）"
                    score -= 20
                elif k_now > d_now:
                    signals["kdj_signal"] = "多头"
                    score += 8
                else:
                    signals["kdj_signal"] = "空头"
                    score -= 8
            else:
                signals["kdj_signal"] = "数据不足"
        else:
            signals["kdj_signal"] = "数据不足"

        # Bollinger signal
        boll_df = self.bollinger()
        if not boll_df.empty:
            upper = boll_df["upper"].iloc[-1]
            lower = boll_df["lower"].iloc[-1]
            middle = boll_df["middle"].iloc[-1]
            price = self.close[-1]

            if not np.isnan(upper):
                if price > upper:
                    signals["boll_signal"] = "突破上轨（偏强/超买）"
                    score -= 5
                elif price < lower:
                    signals["boll_signal"] = "跌破下轨（偏弱/超卖）"
                    score += 5
                elif price > middle:
                    signals["boll_signal"] = "中轨上方运行"
                    score += 5
                else:
                    signals["boll_signal"] = "中轨下方运行"
                    score -= 5
            else:
                signals["boll_signal"] = "数据不足"
        else:
            signals["boll_signal"] = "数据不足"

        # MA alignment
        ma_df = self.moving_averages([5, 10, 20, 60])
        if not ma_df.empty:
            ma5 = ma_df["ma5"].iloc[-1]
            ma10 = ma_df["ma10"].iloc[-1]
            ma20 = ma_df["ma20"].iloc[-1]
            ma60 = ma_df["ma60"].iloc[-1]

            if not any(np.isnan(x) for x in [ma5, ma10, ma20, ma60]):
                if ma5 > ma10 > ma20 > ma60:
                    signals["ma_alignment"] = "多头排列"
                    score += 20
                elif ma5 < ma10 < ma20 < ma60:
                    signals["ma_alignment"] = "空头排列"
                    score -= 20
                elif ma5 > ma10 > ma20:
                    signals["ma_alignment"] = "短期多头"
                    score += 10
                elif ma5 < ma10 < ma20:
                    signals["ma_alignment"] = "短期空头"
                    score -= 10
                else:
                    signals["ma_alignment"] = "交叉震荡"
            else:
                signals["ma_alignment"] = "数据不足"
        else:
            signals["ma_alignment"] = "数据不足"

        # Overall assessment
        score = max(-100, min(100, score))
        if score >= 40:
            signals["overall"] = "强烈看多"
        elif score >= 15:
            signals["overall"] = "看多"
        elif score > -15:
            signals["overall"] = "中性"
        elif score > -40:
            signals["overall"] = "看空"
        else:
            signals["overall"] = "强烈看空"

        signals["score"] = round(score, 1)

        return signals
