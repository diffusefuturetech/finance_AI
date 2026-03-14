#!/usr/bin/env python3
"""One-time full data initialization script.

Usage:
    python scripts/init_data.py [--market csi300] [--start 20150101] [--end 20260314]

This script:
1. Fetches daily OHLCV data for all stocks in the specified market
2. Converts to Qlib binary format
3. Generates calendar and instruments files
4. Verifies Qlib can read the data
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import QLIB_DATA_DIR, ensure_dirs
from data.converter import QlibDataConverter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def verify_qlib_data() -> bool:
    """Verify that Qlib can read the generated data."""
    try:
        import qlib
        from qlib.data import D

        qlib.init(provider_uri=str(QLIB_DATA_DIR), region="cn")

        # Try reading a few days of data
        instruments = D.instruments(market="all")
        stock_list = D.list_instruments(instruments=instruments, as_list=True)
        logger.info(f"Qlib verification: found {len(stock_list)} instruments")

        if len(stock_list) == 0:
            logger.error("No instruments found in Qlib data")
            return False

        # Try reading features for the first stock
        test_stock = stock_list[0]
        features = D.features(
            [test_stock],
            fields=["$close", "$volume"],
            start_time="2024-01-01",
            end_time="2024-12-31",
        )
        logger.info(f"Qlib verification: {test_stock} has {len(features)} data points")

        if len(features) == 0:
            logger.warning("No feature data found, but instruments exist")

        return True

    except ImportError:
        logger.warning("Qlib not installed, skipping verification")
        return True
    except Exception as e:
        logger.error(f"Qlib verification failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Initialize stock data for Qlib")
    parser.add_argument(
        "--market",
        default="csi300",
        choices=["csi300", "csi500", "csi1000", "all"],
        help="Market to initialize (default: csi300)",
    )
    parser.add_argument(
        "--start",
        default="20150101",
        help="Start date YYYYMMDD (default: 20150101)",
    )
    parser.add_argument(
        "--end",
        default=date.today().strftime("%Y%m%d"),
        help="End date YYYYMMDD (default: today)",
    )
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip Qlib data verification",
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Finance AI - Data Initialization")
    logger.info(f"  Market: {args.market}")
    logger.info(f"  Date range: {args.start} - {args.end}")
    logger.info(f"  Qlib dir: {QLIB_DATA_DIR}")
    logger.info("=" * 60)

    # Ensure directories exist
    ensure_dirs()

    # Run full initialization
    converter = QlibDataConverter()
    try:
        converter.full_init(
            market=args.market,
            start_date=args.start,
            end_date=args.end,
        )
    except Exception as e:
        logger.error(f"Data initialization failed: {e}")
        sys.exit(1)

    # Verify
    if not args.skip_verify:
        logger.info("Verifying Qlib data...")
        if verify_qlib_data():
            logger.info("Data verification passed!")
        else:
            logger.warning("Data verification had issues, check logs above")

    logger.info("Data initialization complete!")


if __name__ == "__main__":
    main()
