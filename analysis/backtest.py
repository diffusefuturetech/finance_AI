"""Qlib backtesting wrapper."""

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from config.settings import QLIB_DATA_DIR
from analysis.models import get_model_config, DATASET_CONFIG, BACKTEST_CONFIG

logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    """Backtesting result metrics."""
    total_return: float
    annual_return: float
    max_drawdown: float
    sharpe_ratio: float
    win_rate: float
    trade_count: int
    daily_returns: pd.Series
    cumulative_returns: pd.Series
    benchmark_returns: pd.Series | None = None
    positions: pd.DataFrame | None = None


class QlibBacktester:
    """Wrapper around Qlib's backtesting framework."""

    def __init__(self, qlib_dir: Path | None = None):
        self.qlib_dir = qlib_dir or QLIB_DATA_DIR
        self._initialized = False

    def _ensure_qlib(self) -> None:
        if self._initialized:
            return
        import qlib
        qlib.init(provider_uri=str(self.qlib_dir), region="cn")
        self._initialized = True

    def run_factor_backtest(
        self,
        model_name: str = "lgb",
        market: str = "csi300",
        train_start: str = "2015-01-01",
        train_end: str = "2022-12-31",
        valid_start: str = "2023-01-01",
        valid_end: str = "2024-06-30",
        test_start: str = "2024-07-01",
        test_end: str = "2026-03-14",
        top_k: int = 30,
        n_drop: int = 5,
        benchmark: str = "SH000300",
    ) -> BacktestResult:
        """Run full Qlib workflow: Alpha158 features -> model train -> backtest.

        Args:
            model_name: 'lgb' or 'linear'
            market: 'csi300', 'csi500', etc.
            train_start/end: training period
            valid_start/end: validation period
            test_start/end: backtesting period
            top_k: number of stocks to hold
            n_drop: number of stocks to drop per rebalance
            benchmark: benchmark index

        Returns:
            BacktestResult with all performance metrics
        """
        self._ensure_qlib()

        from qlib.contrib.data.handler import Alpha158
        from qlib.data.dataset import DatasetH
        from qlib.utils import init_instance_by_config

        # Build dataset
        handler_config = {
            "class": "Alpha158",
            "module_path": "qlib.contrib.data.handler",
            "kwargs": {
                "instruments": market,
                "start_time": train_start,
                "end_time": test_end,
                "fit_start_time": train_start,
                "fit_end_time": train_end,
            },
        }

        dataset_config = {
            "class": "DatasetH",
            "module_path": "qlib.data.dataset",
            "kwargs": {
                "handler": handler_config,
                "segments": {
                    "train": (train_start, train_end),
                    "valid": (valid_start, valid_end),
                    "test": (test_start, test_end),
                },
            },
        }

        logger.info("Building dataset...")
        dataset = init_instance_by_config(dataset_config)

        # Train model
        model_config = get_model_config(model_name)
        logger.info(f"Training {model_name} model...")
        model = init_instance_by_config(model_config)
        model.fit(dataset)

        # Predict
        logger.info("Generating predictions...")
        pred = model.predict(dataset)

        # Backtest
        logger.info("Running backtest...")
        from qlib.contrib.strategy import TopkDropoutStrategy
        from qlib.backtest import backtest as qlib_backtest
        from qlib.contrib.evaluate import risk_analysis

        strategy_config = {
            "class": "TopkDropoutStrategy",
            "module_path": "qlib.contrib.strategy",
            "kwargs": {
                "signal": pred,
                "topk": top_k,
                "n_drop": n_drop,
            },
        }

        executor_config = {
            "class": "SimulatorExecutor",
            "module_path": "qlib.backtest.executor",
            "kwargs": {
                "time_per_step": "day",
                "generate_portfolio_metrics": True,
            },
        }

        portfolio_metric_dict, indicator_dict = qlib_backtest(
            pred=pred,
            strategy=strategy_config,
            executor=executor_config,
            account=BACKTEST_CONFIG["account"],
            benchmark=benchmark,
            start_time=test_start,
            end_time=test_end,
        )

        # Extract results
        report_normal = portfolio_metric_dict.get("1day", [None, None])
        if isinstance(report_normal, (list, tuple)) and len(report_normal) >= 2:
            report_df = report_normal[0]
            positions_df = report_normal[1] if len(report_normal) > 1 else None
        else:
            report_df = report_normal
            positions_df = None

        # Risk analysis
        analysis = risk_analysis(report_df)

        # Build result
        daily_returns = report_df["return"] if "return" in report_df.columns else pd.Series()
        cumulative = (1 + daily_returns).cumprod()

        bench_returns = None
        if "bench" in report_df.columns:
            bench_returns = (1 + report_df["bench"]).cumprod()

        total_ret = float(cumulative.iloc[-1] - 1) if len(cumulative) > 0 else 0.0

        return BacktestResult(
            total_return=total_ret,
            annual_return=float(analysis.get("annualized_return", {}).get("risk", 0)),
            max_drawdown=float(analysis.get("max_drawdown", {}).get("risk", 0)),
            sharpe_ratio=float(analysis.get("information_ratio", {}).get("risk", 0)),
            win_rate=float((daily_returns > 0).mean()) if len(daily_returns) > 0 else 0.0,
            trade_count=len(daily_returns),
            daily_returns=daily_returns,
            cumulative_returns=cumulative,
            benchmark_returns=bench_returns,
            positions=positions_df,
        )

    def quick_backtest(
        self,
        market: str = "csi300",
        test_period: str = "6m",
    ) -> BacktestResult:
        """Quick backtest with sensible defaults for the specified period.

        Args:
            market: market to test
            test_period: '3m', '6m', '1y', '2y'
        """
        from datetime import datetime, timedelta

        end = datetime.now()
        period_map = {
            "3m": timedelta(days=90),
            "6m": timedelta(days=180),
            "1y": timedelta(days=365),
            "2y": timedelta(days=730),
        }
        delta = period_map.get(test_period, timedelta(days=180))
        test_start = (end - delta).strftime("%Y-%m-%d")
        test_end = end.strftime("%Y-%m-%d")

        # Training period: 5 years before test
        train_end = (end - delta - timedelta(days=1)).strftime("%Y-%m-%d")
        train_start = (end - delta - timedelta(days=1825)).strftime("%Y-%m-%d")
        valid_start = (end - delta - timedelta(days=365)).strftime("%Y-%m-%d")

        return self.run_factor_backtest(
            model_name="lgb",
            market=market,
            train_start=train_start,
            train_end=train_end,
            valid_start=valid_start,
            valid_end=train_end,
            test_start=test_start,
            test_end=test_end,
        )
