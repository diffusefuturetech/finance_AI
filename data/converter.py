"""Convert fetched stock data into Qlib's binary format."""

import logging
import shutil
from datetime import date
from pathlib import Path

import pandas as pd

from config.settings import (
    CSV_STAGING_DIR,
    DEFAULT_START_DATE,
    QLIB_DATA_DIR,
    symbol_to_qlib,
)
from data.cache import DataCache
from data.fetcher import StockDataFetcher

logger = logging.getLogger(__name__)


class QlibDataConverter:
    """Converts fetched data into Qlib's expected directory structure and bin format.

    Qlib expects:
        qlib_data_dir/
            calendars/day.txt       # one date per line (YYYY-MM-DD)
            instruments/all.txt     # <instrument>\t<start_date>\t<end_date>
            features/<INSTRUMENT>/  # binary .bin + .day files per feature
    """

    def __init__(
        self,
        fetcher: StockDataFetcher | None = None,
        qlib_dir: Path | None = None,
        staging_dir: Path | None = None,
    ):
        self.fetcher = fetcher or StockDataFetcher()
        self.qlib_dir = qlib_dir or QLIB_DATA_DIR
        self.staging_dir = staging_dir or CSV_STAGING_DIR

    def full_init(
        self,
        market: str = "csi300",
        start_date: str = DEFAULT_START_DATE,
        end_date: str | None = None,
    ) -> None:
        """Full pipeline: fetch all -> stage CSVs -> generate calendar + instruments -> dump_bin.

        Args:
            market: 'csi300', 'csi500', or 'all'
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format, defaults to today
        """
        if end_date is None:
            end_date = date.today().strftime("%Y%m%d")

        logger.info(f"Starting full data init: market={market}, range=[{start_date}, {end_date}]")

        # Step 1: Get symbol list
        symbols = self._get_symbols(market)
        logger.info(f"Got {len(symbols)} symbols for market={market}")

        # Step 2: Fetch and stage CSVs
        self.prepare_csv_for_qlib(symbols, start_date, end_date)

        # Step 3: Generate calendar and instruments
        self.generate_calendar(start_date, end_date)
        self.generate_instruments(symbols, start_date)

        # Step 4: Dump to binary format
        self.dump_to_bin()

        logger.info("Full data init completed successfully")

    def prepare_csv_for_qlib(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
    ) -> Path:
        """Fetch data for all symbols and write per-symbol CSVs for Qlib.

        Qlib expects CSV columns: date, open, close, high, low, volume, factor
        File naming: SH600519.csv, SZ000001.csv

        Returns: path to staging directory
        """
        self.staging_dir.mkdir(parents=True, exist_ok=True)

        total = len(symbols)
        for i, symbol in enumerate(symbols, 1):
            qlib_code = symbol_to_qlib(symbol)
            csv_path = self.staging_dir / f"{qlib_code}.csv"

            if csv_path.exists():
                logger.debug(f"[{i}/{total}] Skipping {qlib_code} (CSV exists)")
                continue

            logger.info(f"[{i}/{total}] Fetching {qlib_code}...")
            try:
                df = self.fetcher.get_daily_history(
                    symbol, start_date, end_date, adjust="hfq"
                )
                if df is None or df.empty:
                    logger.warning(f"No data for {qlib_code}, skipping")
                    continue

                # Prepare Qlib format
                qlib_df = pd.DataFrame({
                    "date": pd.to_datetime(df["date"]),
                    "open": df["open"].astype(float),
                    "close": df["close"].astype(float),
                    "high": df["high"].astype(float),
                    "low": df["low"].astype(float),
                    "volume": df["volume"].astype(float),
                })
                # Calculate adjustment factor (hfq close / raw close approximation)
                # For Qlib, factor = 1.0 when using pre-adjusted data
                qlib_df["factor"] = 1.0

                qlib_df = qlib_df.sort_values("date")
                qlib_df.to_csv(csv_path, index=False)
                logger.debug(f"Saved {qlib_code}: {len(qlib_df)} rows")

            except Exception as e:
                logger.error(f"Failed to process {qlib_code}: {e}")
                continue

        return self.staging_dir

    def generate_calendar(self, start_date: str, end_date: str) -> None:
        """Generate calendars/day.txt listing all trading dates."""
        calendar_dir = self.qlib_dir / "calendars"
        calendar_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Use AKShare to get trading calendar
            import akshare as ak
            trade_dates = ak.tool_trade_date_hist_sina()
            trade_dates["trade_date"] = pd.to_datetime(trade_dates["trade_date"])

            start_dt = pd.to_datetime(self._normalize_date(start_date))
            end_dt = pd.to_datetime(self._normalize_date(end_date))

            mask = (trade_dates["trade_date"] >= start_dt) & (trade_dates["trade_date"] <= end_dt)
            dates = trade_dates[mask]["trade_date"].sort_values()

            with open(calendar_dir / "day.txt", "w") as f:
                for d in dates:
                    f.write(d.strftime("%Y-%m-%d") + "\n")

            logger.info(f"Generated calendar: {len(dates)} trading days")

        except Exception as e:
            logger.error(f"Calendar generation failed: {e}")
            # Fallback: extract dates from staged CSVs
            self._generate_calendar_from_csvs(calendar_dir)

    def _generate_calendar_from_csvs(self, calendar_dir: Path) -> None:
        """Fallback: build calendar from existing CSV files."""
        all_dates = set()
        for csv_file in self.staging_dir.glob("*.csv"):
            try:
                df = pd.read_csv(csv_file, usecols=["date"])
                all_dates.update(df["date"].tolist())
            except Exception:
                continue

        sorted_dates = sorted(all_dates)
        with open(calendar_dir / "day.txt", "w") as f:
            for d in sorted_dates:
                f.write(str(d) + "\n")
        logger.info(f"Generated calendar from CSVs: {len(sorted_dates)} dates")

    def generate_instruments(
        self, symbols: list[str], start_date: str | None = None
    ) -> None:
        """Generate instruments/all.txt.

        Format: SH600519<tab>2001-08-27<tab>2099-12-31
        """
        instruments_dir = self.qlib_dir / "instruments"
        instruments_dir.mkdir(parents=True, exist_ok=True)

        lines = []
        for symbol in symbols:
            qlib_code = symbol_to_qlib(symbol)
            csv_path = self.staging_dir / f"{qlib_code}.csv"

            if csv_path.exists():
                try:
                    df = pd.read_csv(csv_path, usecols=["date"])
                    first_date = df["date"].min()
                    lines.append(f"{qlib_code}\t{first_date}\t2099-12-31")
                except Exception:
                    if start_date:
                        norm_start = self._normalize_date(start_date)
                        lines.append(f"{qlib_code}\t{norm_start}\t2099-12-31")
            elif start_date:
                norm_start = self._normalize_date(start_date)
                lines.append(f"{qlib_code}\t{norm_start}\t2099-12-31")

        with open(instruments_dir / "all.txt", "w") as f:
            f.write("\n".join(lines) + "\n")

        logger.info(f"Generated instruments: {len(lines)} stocks")

    def dump_to_bin(self) -> None:
        """Convert staged CSVs to Qlib binary format using qlib's dump tools."""
        try:
            from qlib.data import D
            from qlib.scripts.dump_bin import DumpDataAll

            dumper = DumpDataAll(
                csv_path=str(self.staging_dir),
                qlib_dir=str(self.qlib_dir),
                include_fields="open,close,high,low,volume,factor",
                exclude_fields="",
                max_workers=4,
                date_field_name="date",
                symbol_field_name=None,  # use filename as symbol
            )
            dumper.dump()
            logger.info("Qlib binary dump completed")

        except ImportError:
            logger.warning("Qlib not installed, attempting command-line dump")
            self._dump_via_cli()

    def _dump_via_cli(self) -> None:
        """Fallback: dump via qlib CLI command."""
        import subprocess
        cmd = [
            "python", "-m", "qlib.scripts.dump_bin", "dump_all",
            f"--csv_path={self.staging_dir}",
            f"--qlib_dir={self.qlib_dir}",
            "--include_fields=open,close,high,low,volume,factor",
            "--date_field_name=date",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Qlib dump failed: {result.stderr}")
            raise RuntimeError(f"Qlib dump_bin failed: {result.stderr}")
        logger.info("Qlib binary dump completed via CLI")

    def incremental_update(self, symbols: list[str], update_date: str) -> None:
        """Append single day's data for given symbols.

        Called daily after market close to keep Qlib data up to date.
        """
        logger.info(f"Incremental update for {len(symbols)} symbols on {update_date}")

        for symbol in symbols:
            try:
                df = self.fetcher.get_daily_history(
                    symbol, update_date, update_date, adjust="hfq"
                )
                if df is None or df.empty:
                    continue

                qlib_code = symbol_to_qlib(symbol)
                csv_path = self.staging_dir / f"{qlib_code}.csv"

                # Append to existing CSV
                if csv_path.exists():
                    existing = pd.read_csv(csv_path)
                    new_row = pd.DataFrame({
                        "date": [pd.to_datetime(df.iloc[0]["date"])],
                        "open": [float(df.iloc[0]["open"])],
                        "close": [float(df.iloc[0]["close"])],
                        "high": [float(df.iloc[0]["high"])],
                        "low": [float(df.iloc[0]["low"])],
                        "volume": [float(df.iloc[0]["volume"])],
                        "factor": [1.0],
                    })
                    combined = pd.concat([existing, new_row], ignore_index=True)
                    combined.drop_duplicates(subset=["date"], keep="last", inplace=True)
                    combined.to_csv(csv_path, index=False)

            except Exception as e:
                logger.error(f"Incremental update failed for {symbol}: {e}")

        # Re-dump to update binary format
        self.dump_to_bin()

        # Update calendar
        calendar_path = self.qlib_dir / "calendars" / "day.txt"
        if calendar_path.exists():
            norm_date = self._normalize_date(update_date)
            with open(calendar_path) as f:
                dates = set(f.read().strip().split("\n"))
            if norm_date not in dates:
                dates.add(norm_date)
                with open(calendar_path, "w") as f:
                    f.write("\n".join(sorted(dates)) + "\n")

    def _get_symbols(self, market: str) -> list[str]:
        """Get symbol list for a given market."""
        if market == "all":
            df = self.fetcher.get_stock_list()
            return df["symbol"].tolist()
        elif market in ("csi300", "000300"):
            return self.fetcher.get_index_components("000300")
        elif market in ("csi500", "000905"):
            return self.fetcher.get_index_components("000905")
        elif market in ("csi1000", "000852"):
            return self.fetcher.get_index_components("000852")
        else:
            logger.warning(f"Unknown market '{market}', defaulting to csi300")
            return self.fetcher.get_index_components("000300")

    @staticmethod
    def _normalize_date(date_str: str) -> str:
        date_str = date_str.replace("-", "").replace("/", "")
        if len(date_str) == 8:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        return date_str
