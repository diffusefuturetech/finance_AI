#!/usr/bin/env python3
"""Daily incremental data update script.

Usage:
    python scripts/update_data.py [--market csi300] [--date 20260314]

Run this daily after market close (16:00 CST) to keep Qlib data up to date.
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import ensure_dirs
from data.converter import QlibDataConverter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Daily incremental data update")
    parser.add_argument(
        "--market",
        default="csi300",
        choices=["csi300", "csi500", "csi1000", "all"],
    )
    parser.add_argument(
        "--date",
        default=date.today().strftime("%Y%m%d"),
        help="Date to update YYYYMMDD (default: today)",
    )
    args = parser.parse_args()

    ensure_dirs()

    logger.info(f"Incremental update: market={args.market}, date={args.date}")

    converter = QlibDataConverter()
    symbols = converter._get_symbols(args.market)
    converter.incremental_update(symbols, args.date)

    logger.info("Incremental update complete!")


if __name__ == "__main__":
    main()
