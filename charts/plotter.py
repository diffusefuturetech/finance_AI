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
        """Multi-panel dashboard: candlestick + volume + MACD + KDJ.

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

        fig, axes = plt.subplots(4, 1, figsize=(14, 12),
                                  gridspec_kw={"height_ratios": [3, 1, 1.2, 1.2]},
                                  sharex=True)
        fig.suptitle(f"{name}（{symbol}）技术分析", fontsize=16, fontweight="bold")

        # Panel 1: Candlestick + MA
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
            # Wicks
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
            ax1.legend(loc="upper left", fontsize=8)

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

        # X-axis date labels
        tick_step = max(1, len(x) // 10)
        ax4.set_xticks(x[::tick_step])
        ax4.set_xticklabels(
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
