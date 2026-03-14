"""Qlib ML model configurations."""

# LightGBM model - main workhorse
LGB_MODEL_CONFIG = {
    "class": "LGBModel",
    "module_path": "qlib.contrib.model.gbdt",
    "kwargs": {
        "loss": "mse",
        "colsample_bytree": 0.8879,
        "learning_rate": 0.0421,
        "subsample": 0.8789,
        "lambda_l1": 205.6999,
        "lambda_l2": 580.9768,
        "max_depth": 8,
        "num_leaves": 210,
        "num_threads": 4,
    },
}

# Linear model - simple baseline
LINEAR_MODEL_CONFIG = {
    "class": "LinearModel",
    "module_path": "qlib.contrib.model.linear",
    "kwargs": {
        "estimator": "ridge",
        "alpha": 0.05,
    },
}

# Dataset configuration with Alpha158
DATASET_CONFIG = {
    "class": "DatasetH",
    "module_path": "qlib.data.dataset",
    "kwargs": {
        "handler": {
            "class": "Alpha158",
            "module_path": "qlib.contrib.data.handler",
        },
        "segments": {
            "train": ("2015-01-01", "2022-12-31"),
            "valid": ("2023-01-01", "2024-06-30"),
            "test": ("2024-07-01", "2026-03-14"),
        },
    },
}

# Backtest configuration
BACKTEST_CONFIG = {
    "strategy": {
        "class": "TopkDropoutStrategy",
        "module_path": "qlib.contrib.strategy",
        "kwargs": {
            "topk": 30,
            "n_drop": 5,
        },
    },
    "executor": {
        "class": "SimulatorExecutor",
        "module_path": "qlib.backtest.executor",
        "kwargs": {
            "time_per_step": "day",
        },
    },
    "account": 100_000_000,
    "benchmark": "SH000300",
}


def get_model_config(model_name: str = "lgb") -> dict:
    """Get model config by name."""
    configs = {
        "lgb": LGB_MODEL_CONFIG,
        "linear": LINEAR_MODEL_CONFIG,
    }
    if model_name not in configs:
        raise ValueError(f"Unknown model: {model_name}. Available: {list(configs.keys())}")
    return configs[model_name]
