"""Chart generation using matplotlib with Chinese font support."""

import platform
from pathlib import Path
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

from config.settings import CHART_OUTPUT_DIR

# Configure Chinese font support
_system = platform.system()
if _system == "Darwin":
    plt.rcParams["font.sans-serif"] = ["Arial Unicode MS", "PingFang SC", "SimHei"]
elif _system == "Linux":
    plt.rcParams["font.sans-serif"] = ["WenQuanYi Zen Hei", "SimHei", "DejaVu Sans"]
else:
    plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei"]
plt.rcParams["axes.unicode_minus"] = False


class ChartPlotter:
    """Generate chart images for stock analysis."""

    def __init__(self, output_dir: Path | None = None):
        self.output_dir = output_dir or CHART_OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _save_fig(self, fig: plt.Figure, name: str) -> str:
        """Save figure and return path."""
        filename = f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        path = self.output_dir / filename
        fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
        plt.close(fig)
        return str(path)

    def plot_technical_dashboard(
        self,
        df: pd.DataFrame,
        indicators: dict[str, pd.DataFrame],
        symbol: str,
        name: str,
        last_n: int = 120,
    ) -> str:
        """Multi-panel dashboard: candlestick+Bollinger + volume + MACD + KDJ + RSI.

        Args:
            df: OHLCV DataFrame
            indicators: dict from TechnicalAnalyzer.compute_all()
            symbol: stock code
            name: stock name
            last_n: number of recent days to show

        Returns: path to saved PNG
        """
        df = df.tail(last_n).copy().reset_index(drop=True)
        dates = pd.to_datetime(df["date"])
        x = np.arange(len(dates))

        fig, axes = plt.subplots(5, 1, figsize=(14, 15),
                                  gridspec_kw={"height_ratios": [3, 1, 1.2, 1.2, 1.2]},
                                  sharex=True)
        fig.suptitle(f"{name}（{symbol}）技术分析", fontsize=16, fontweight="bold")

        # Panel 1: Candlestick + MA + Bollinger Bands
        ax1 = axes[0]
        close = df["close"].values.astype(float)
        open_p = df["open"].values.astype(float)
        high = df["high"].values.astype(float)
        low = df["low"].values.astype(float)

        colors = ["#ef5350" if c >= o else "#26a69a" for c, o in zip(close, open_p)]

        # Candlestick bodies
        for i in range(len(x)):
            body_bottom = min(open_p[i], close[i])
            body_height = abs(close[i] - open_p[i])
            ax1.bar(x[i], body_height, bottom=body_bottom, width=0.6,
                    color=colors[i], edgecolor=colors[i])
            ax1.vlines(x[i], low[i], high[i], colors=colors[i], linewidth=0.8)

        # Moving averages overlay
        if "ma" in indicators:
            ma_df = indicators["ma"].tail(last_n).reset_index(drop=True)
            ma_colors = {"ma5": "#FF6D00", "ma10": "#2962FF", "ma20": "#AB47BC", "ma60": "#00897B"}
            for col, color in ma_colors.items():
                if col in ma_df.columns:
                    vals = ma_df[col].values
                    valid = ~np.isnan(vals)
                    ax1.plot(x[valid], vals[valid], color=color, linewidth=1, label=col.upper())

        # Bollinger Bands overlay
        if "bollinger" in indicators:
            boll_df = indicators["bollinger"].tail(last_n).reset_index(drop=True)
            for col in ["upper", "middle", "lower"]:
                if col in boll_df.columns:
                    vals = boll_df[col].values
                    valid = ~np.isnan(vals)
                    style = "--" if col != "middle" else ":"
                    ax1.plot(x[valid], vals[valid], color="#9E9E9E", linewidth=0.8,
                             linestyle=style, label=f"BOLL-{col[0].upper()}" if col != "middle" else "BOLL-M")
            # Fill between upper and lower
            if "upper" in boll_df.columns and "lower" in boll_df.columns:
                upper = boll_df["upper"].values
                lower = boll_df["lower"].values
                valid = ~(np.isnan(upper) | np.isnan(lower))
                ax1.fill_between(x[valid], upper[valid], lower[valid],
                                 color="#9E9E9E", alpha=0.08)

        ax1.legend(loc="upper left", fontsize=7, ncol=4)
        ax1.set_ylabel("价格")
        ax1.grid(True, alpha=0.3)

        # Panel 2: Volume
        ax2 = axes[1]
        volume = df["volume"].values.astype(float)
        ax2.bar(x, volume, color=colors, width=0.6)
        ax2.set_ylabel("成交量")
        ax2.grid(True, alpha=0.3)

        # Panel 3: MACD
        ax3 = axes[2]
        if "macd" in indicators:
            macd_df = indicators["macd"].tail(last_n).reset_index(drop=True)
            hist = macd_df["macd_hist"].values
            dif = macd_df["dif"].values
            dea = macd_df["dea"].values

            valid = ~np.isnan(hist)
            hist_colors = ["#ef5350" if h >= 0 else "#26a69a" for h in hist[valid]]
            ax3.bar(x[valid], hist[valid], color=hist_colors, width=0.6)

            valid_dif = ~np.isnan(dif)
            ax3.plot(x[valid_dif], dif[valid_dif], color="#2962FF", linewidth=1, label="DIF")
            valid_dea = ~np.isnan(dea)
            ax3.plot(x[valid_dea], dea[valid_dea], color="#FF6D00", linewidth=1, label="DEA")
            ax3.legend(loc="upper left", fontsize=8)

        ax3.set_ylabel("MACD")
        ax3.axhline(y=0, color="gray", linewidth=0.5)
        ax3.grid(True, alpha=0.3)

        # Panel 4: KDJ
        ax4 = axes[3]
        if "kdj" in indicators:
            kdj_df = indicators["kdj"].tail(last_n).reset_index(drop=True)
            for col, color, label in [
                ("k", "#2962FF", "K"),
                ("d", "#FF6D00", "D"),
                ("j", "#AB47BC", "J"),
            ]:
                vals = kdj_df[col].values
                valid = ~np.isnan(vals)
                ax4.plot(x[valid], vals[valid], color=color, linewidth=1, label=label)
            ax4.axhline(y=80, color="gray", linewidth=0.5, linestyle="--")
            ax4.axhline(y=20, color="gray", linewidth=0.5, linestyle="--")
            ax4.legend(loc="upper left", fontsize=8)

        ax4.set_ylabel("KDJ")
        ax4.grid(True, alpha=0.3)

        # Panel 5: RSI
        ax5 = axes[4]
        if "rsi" in indicators:
            rsi_df = indicators["rsi"].tail(last_n).reset_index(drop=True)
            rsi_styles = [
                ("rsi_6", "#2962FF", "RSI(6)"),
                ("rsi_12", "#FF6D00", "RSI(12)"),
                ("rsi_24", "#AB47BC", "RSI(24)"),
            ]
            for col, color, label in rsi_styles:
                if col in rsi_df.columns:
                    vals = rsi_df[col].values
                    valid = ~np.isnan(vals)
                    ax5.plot(x[valid], vals[valid], color=color, linewidth=1, label=label)
            ax5.axhline(y=70, color="#ef5350", linewidth=0.5, linestyle="--", alpha=0.7)
            ax5.axhline(y=30, color="#26a69a", linewidth=0.5, linestyle="--", alpha=0.7)
            ax5.axhline(y=50, color="gray", linewidth=0.3, linestyle=":")
            ax5.fill_between(x, 70, 100, color="#ef5350", alpha=0.05)
            ax5.fill_between(x, 0, 30, color="#26a69a", alpha=0.05)
            ax5.set_ylim(0, 100)
            ax5.legend(loc="upper left", fontsize=8)

        ax5.set_ylabel("RSI")
        ax5.grid(True, alpha=0.3)

        # X-axis date labels on bottom panel
        tick_step = max(1, len(x) // 10)
        ax5.set_xticks(x[::tick_step])
        ax5.set_xticklabels(
            [dates.iloc[i].strftime("%m-%d") for i in range(0, len(dates), tick_step)],
            rotation=45, fontsize=8,
        )

        fig.tight_layout()
        return self._save_fig(fig, f"technical_{symbol}")

    def plot_equity_curve(
        self,
        cumulative_returns: pd.Series,
        benchmark: pd.Series | None = None,
        strategy_name: str = "策略",
    ) -> str:
        """Backtest equity curve vs benchmark.

        Returns: path to saved PNG
        """
        fig, ax = plt.subplots(figsize=(12, 6))

        ax.plot(cumulative_returns.index, cumulative_returns.values,
                color="#2962FF", linewidth=1.5, label=strategy_name)

        if benchmark is not None:
            ax.plot(benchmark.index, benchmark.values,
                    color="#FF6D00", linewidth=1.2, label="基准", linestyle="--")

        ax.set_title(f"{strategy_name} 净值曲线", fontsize=14)
        ax.set_ylabel("累计收益")
        ax.legend(loc="upper left")
        ax.grid(True, alpha=0.3)
        ax.axhline(y=1.0 if cumulative_returns.iloc[0] >= 0.5 else 0,
                    color="gray", linewidth=0.5)

        fig.autofmt_xdate()
        fig.tight_layout()
        return self._save_fig(fig, f"equity_{strategy_name}")

    def plot_factor_radar(
        self, factor_scores: dict[str, float], symbol: str, name: str = ""
    ) -> str:
        """Radar/spider chart of factor scores.

        Args:
            factor_scores: dict like {'价值': 80, '质量': 65, '动量': 50, ...}

        Returns: path to saved PNG
        """
        labels = list(factor_scores.keys())
        values = list(factor_scores.values())

        # Close the radar
        angles = np.linspace(0, 2 * np.pi, len(labels), endpoint=False).tolist()
        values_plot = values + [values[0]]
        angles += [angles[0]]

        fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(polar=True))

        ax.fill(angles, values_plot, color="#2962FF", alpha=0.15)
        ax.plot(angles, values_plot, color="#2962FF", linewidth=2)
        ax.scatter(angles[:-1], values, color="#2962FF", s=50, zorder=5)

        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(labels, fontsize=12)
        ax.set_ylim(0, 100)
        ax.set_title(f"{name}（{symbol}）多因子评分", fontsize=14, pad=20)

        fig.tight_layout()
        return self._save_fig(fig, f"radar_{symbol}")

    def plot_valuation_history(
        self,
        val_df: pd.DataFrame,
        symbol: str,
        name: str = "",
    ) -> str:
        """Historical PE/PB valuation chart with percentile bands.

        Args:
            val_df: DataFrame with columns: date, pe, pb, ps
            symbol: stock code
            name: stock name

        Returns: path to saved PNG
        """
        if val_df.empty:
            return ""

        dates = pd.to_datetime(val_df["date"])

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
        fig.suptitle(f"{name}（{symbol}）历史估值分位", fontsize=14, fontweight="bold")

        for ax, col, label, color in [
            (ax1, "pe", "PE(TTM)", "#2962FF"),
            (ax2, "pb", "PB(MRQ)", "#FF6D00"),
        ]:
            if col not in val_df.columns:
                continue
            vals = val_df[col].dropna()
            if vals.empty:
                continue

            # Filter positive values for PE (negative PE is meaningless for bands)
            if col == "pe":
                plot_vals = vals[vals > 0]
            else:
                plot_vals = vals[vals > 0]

            if plot_vals.empty:
                ax.text(0.5, 0.5, f"{label}: 数据不足", ha="center", va="center",
                        transform=ax.transAxes, fontsize=12, color="gray")
                continue

            plot_dates = dates[plot_vals.index]
            mean_val = plot_vals.mean()
            std_val = plot_vals.std()
            current = plot_vals.iloc[-1]
            percentile = (plot_vals < current).sum() / len(plot_vals) * 100

            # Main line
            ax.plot(plot_dates, plot_vals, color=color, linewidth=1.2, label=label)

            # Mean line
            ax.axhline(y=mean_val, color="#666666", linewidth=1, linestyle="--",
                       label=f"均值 {mean_val:.1f}")

            # ±1σ bands
            ax.axhline(y=mean_val + std_val, color="#BDBDBD", linewidth=0.8, linestyle=":")
            ax.axhline(y=mean_val - std_val, color="#BDBDBD", linewidth=0.8, linestyle=":")
            ax.fill_between(plot_dates, mean_val - std_val, mean_val + std_val,
                            color=color, alpha=0.06)

            # Current value marker
            ax.scatter([plot_dates.iloc[-1]], [current], color="#ef5350", s=80, zorder=5,
                       edgecolors="white", linewidth=1.5)
            ax.annotate(f"当前 {current:.1f}\n({percentile:.0f}%分位)",
                        xy=(plot_dates.iloc[-1], current),
                        xytext=(10, 10), textcoords="offset points",
                        fontsize=9, color="#ef5350", fontweight="bold",
                        arrowprops=dict(arrowstyle="->", color="#ef5350", lw=1))

            ax.set_ylabel(label)
            ax.legend(loc="upper left", fontsize=8)
            ax.grid(True, alpha=0.3)

        fig.autofmt_xdate()
        fig.tight_layout()
        return self._save_fig(fig, f"valuation_{symbol}")
