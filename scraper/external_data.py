import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf

logger = logging.getLogger(__name__)

DEFAULT_MACRO_TICKERS = {
    "^GSPC": "SP500",
    "^DJI": "DOW30",
    "^IXIC": "NASDAQ",
    "^N225": "NIKKEI225",
    "^HSI": "HANGSENG",
    "USDIDR=X": "USDIDR",
    "CL=F": "CRUDE_OIL",
    "GC=F": "GOLD",
    "^TNX": "US10Y_YIELD"
}


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (pd.Series, list, tuple)):
        if len(v) == 0:
            return None
        v = v.iloc[0] if hasattr(v, "iloc") else v[0]
    try:
        if pd.isna(v):
            return None
        return float(v)
    except (ValueError, TypeError):
        return None

class MacroDataFetcher:
    """Fetches global macro indices, FX rates, and commodities via yfinance."""

    def __init__(self, tickers: Optional[Union[List[str], Dict[str, str]]] = None):
        if tickers is None:
            self.ticker_map = DEFAULT_MACRO_TICKERS.copy()
        elif isinstance(tickers, dict):
            self.ticker_map = tickers.copy()
        elif isinstance(tickers, list):
            self.ticker_map = {t: DEFAULT_MACRO_TICKERS.get(t, t) for t in tickers}
        else:
            raise ValueError(f"Invalid type for tickers: {type(tickers)}")

    def get_tracked_tickers(self) -> List[str]:
        return list(self.ticker_map.keys())

    def fetch_macro_snapshot(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Downloads historical OHLCV data for tracked macro tickers between start_date and end_date."""
        tickers = self.get_tracked_tickers()
        if not tickers:
            return []

        try:
            df = yf.download(
                tickers=tickers,
                start=start_date,
                end=end_date,
                progress=False,
                auto_adjust=False
            )
        except Exception as e:
            logger.error(f"Error downloading macro data from yfinance: {e}")
            return []

        if df is None or df.empty:
            return []

        records = []
        for dt_idx in df.index:
            date_val = dt_idx.strftime("%Y-%m-%d") if hasattr(dt_idx, "strftime") else str(dt_idx)

            for ticker in tickers:
                name = self.ticker_map.get(ticker, ticker)
                open_val = None
                high_val = None
                low_val = None
                close_val = None
                vol_val = None
                adj_close_val = None

                if isinstance(df.columns, pd.MultiIndex):
                    if ticker in df.columns.get_level_values(1):
                        open_val = df.loc[dt_idx, ("Open", ticker)] if ("Open", ticker) in df.columns else None
                        high_val = df.loc[dt_idx, ("High", ticker)] if ("High", ticker) in df.columns else None
                        low_val = df.loc[dt_idx, ("Low", ticker)] if ("Low", ticker) in df.columns else None
                        close_val = df.loc[dt_idx, ("Close", ticker)] if ("Close", ticker) in df.columns else None
                        vol_val = df.loc[dt_idx, ("Volume", ticker)] if ("Volume", ticker) in df.columns else None
                        adj_close_val = df.loc[dt_idx, ("Adj Close", ticker)] if ("Adj Close", ticker) in df.columns else None
                    elif ticker in df.columns.get_level_values(0):
                        open_val = df.loc[dt_idx, (ticker, "Open")] if (ticker, "Open") in df.columns else None
                        high_val = df.loc[dt_idx, (ticker, "High")] if (ticker, "High") in df.columns else None
                        low_val = df.loc[dt_idx, (ticker, "Low")] if (ticker, "Low") in df.columns else None
                        close_val = df.loc[dt_idx, (ticker, "Close")] if (ticker, "Close") in df.columns else None
                        vol_val = df.loc[dt_idx, (ticker, "Volume")] if (ticker, "Volume") in df.columns else None
                        adj_close_val = df.loc[dt_idx, (ticker, "Adj Close")] if (ticker, "Adj Close") in df.columns else None
                else:
                    if len(tickers) == 1:
                        open_val = df.loc[dt_idx, "Open"] if "Open" in df.columns else None
                        high_val = df.loc[dt_idx, "High"] if "High" in df.columns else None
                        low_val = df.loc[dt_idx, "Low"] if "Low" in df.columns else None
                        close_val = df.loc[dt_idx, "Close"] if "Close" in df.columns else None
                        vol_val = df.loc[dt_idx, "Volume"] if "Volume" in df.columns else None
                        adj_close_val = df.loc[dt_idx, "Adj Close"] if "Adj Close" in df.columns else None
                    else:
                        for col in df.columns:
                            col_str = str(col).lower()
                            if ticker.lower() in col_str:
                                if "open" in col_str:
                                    open_val = df.loc[dt_idx, col]
                                elif "high" in col_str:
                                    high_val = df.loc[dt_idx, col]
                                elif "low" in col_str:
                                    low_val = df.loc[dt_idx, col]
                                elif "close" in col_str and "adj" not in col_str:
                                    close_val = df.loc[dt_idx, col]
                                elif "volume" in col_str:
                                    vol_val = df.loc[dt_idx, col]
                                elif "adj" in col_str:
                                    adj_close_val = df.loc[dt_idx, col]

                open_val = _to_float(open_val)
                high_val = _to_float(high_val)
                low_val = _to_float(low_val)
                close_val = _to_float(close_val)
                vol_val = _to_float(vol_val)
                adj_close_val = _to_float(adj_close_val)

                if close_val is None and open_val is None and high_val is None and low_val is None:
                    continue

                records.append({
                    "date": date_val,
                    "ticker": ticker,
                    "name": name,
                    "open": open_val,
                    "high": high_val,
                    "low": low_val,
                    "close": close_val,
                    "adj_close": adj_close_val,
                    "volume": vol_val
                })

        return records

    def save_macro_parquet(self, base_output_dir: Union[str, Path], date_str: str) -> Path:
        """Fetches macro data snapshot for the given date and saves to parquet under base_output_dir / date=YYYYMMDD / macro / macro_daily.parquet."""
        base_dir = Path(base_output_dir)
        clean_date_str = str(date_str).replace("-", "")
        try:
            dt = datetime.strptime(clean_date_str, "%Y%m%d")
        except ValueError:
            dt = datetime.fromisoformat(date_str)
            clean_date_str = dt.strftime("%Y%m%d")

        start_date = dt.strftime("%Y-%m-%d")
        end_date = (dt + timedelta(days=1)).strftime("%Y-%m-%d")

        records = self.fetch_macro_snapshot(start_date=start_date, end_date=end_date)

        macro_dir = base_dir / f"date={clean_date_str}" / "macro"
        macro_dir.mkdir(parents=True, exist_ok=True)
        out_path = macro_dir / "macro_daily.parquet"

        # Exclude 'date' column when writing to Hive partition date=YYYYMMDD to avoid type conflict during dataset read
        file_records = [
            {k: v for k, v in r.items() if k != "date"}
            for r in records
        ]

        if file_records:
            table = pa.Table.from_pylist(file_records)
        else:
            schema = pa.schema([
                ("ticker", pa.string()),
                ("name", pa.string()),
                ("open", pa.float64()),
                ("high", pa.float64()),
                ("low", pa.float64()),
                ("close", pa.float64()),
                ("adj_close", pa.float64()),
                ("volume", pa.float64())
            ])
            table = pa.Table.from_batches([], schema=schema)

        pq.write_table(table, out_path)
        logger.info(f"Saved {len(file_records)} macro records to {out_path}")
        return out_path

    def fetch_macro_intraday_snapshot(self, date_str: str) -> List[Dict[str, Any]]:
        """Fetches 1-minute intraday macro data for all tracked tickers for the given date."""
        clean_date_str = str(date_str).replace("-", "")
        try:
            dt = datetime.strptime(clean_date_str, "%Y%m%d")
        except ValueError:
            dt = datetime.fromisoformat(date_str)
            clean_date_str = dt.strftime("%Y%m%d")

        start_date = dt.strftime("%Y-%m-%d")
        end_date = (dt + timedelta(days=1)).strftime("%Y-%m-%d")

        tickers = self.get_tracked_tickers()
        try:
            df = yf.download(
                tickers=tickers,
                start=start_date,
                end=end_date,
                interval="1m",
                group_by="column",
                auto_adjust=False,
                progress=False,
                threads=True
            )
        except Exception as e:
            logger.error(f"Error downloading intraday macro data from yfinance: {e}")
            return []

        if df is None or df.empty:
            logger.warning(f"No 1-minute intraday macro data returned for date {start_date}")
            return []

        records = []
        is_multi_index = isinstance(df.columns, pd.MultiIndex)

        for idx, row in df.iterrows():
            time_str = idx.isoformat() if hasattr(idx, "isoformat") else str(idx)

            for ticker in tickers:
                name = self.ticker_map.get(ticker, ticker)
                open_val, high_val, low_val, close_val, vol_val = None, None, None, None, None

                if is_multi_index:
                    if ("Open", ticker) in df.columns:
                        open_val = row.get(("Open", ticker))
                        high_val = row.get(("High", ticker))
                        low_val = row.get(("Low", ticker))
                        close_val = row.get(("Close", ticker))
                        vol_val = row.get(("Volume", ticker))
                    elif (ticker, "Open") in df.columns:
                        open_val = row.get((ticker, "Open"))
                        high_val = row.get((ticker, "High"))
                        low_val = row.get((ticker, "Low"))
                        close_val = row.get((ticker, "Close"))
                        vol_val = row.get((ticker, "Volume"))
                else:
                    if len(tickers) == 1:
                        open_val = row.get("Open")
                        high_val = row.get("High")
                        low_val = row.get("Low")
                        close_val = row.get("Close")
                        vol_val = row.get("Volume")

                open_val = _to_float(open_val)
                high_val = _to_float(high_val)
                low_val = _to_float(low_val)
                close_val = _to_float(close_val)
                vol_val = _to_float(vol_val)

                if close_val is None and open_val is None and high_val is None and low_val is None:
                    continue

                records.append({
                    "ticker": ticker,
                    "name": name,
                    "time": time_str,
                    "open": open_val,
                    "high": high_val,
                    "low": low_val,
                    "close": close_val,
                    "volume": vol_val
                })

        return records

    def save_macro_intraday_parquet(self, base_output_dir: Union[str, Path], date_str: str) -> Path:
        """Fetches 1-minute intraday macro data snapshot and saves to parquet under base_output_dir / date=YYYYMMDD / macro / macro_intraday_1m.parquet."""
        base_dir = Path(base_output_dir)
        clean_date_str = str(date_str).replace("-", "")
        try:
            dt = datetime.strptime(clean_date_str, "%Y%m%d")
        except ValueError:
            dt = datetime.fromisoformat(date_str)
            clean_date_str = dt.strftime("%Y%m%d")

        records = self.fetch_macro_intraday_snapshot(date_str=date_str)

        macro_dir = base_dir / f"date={clean_date_str}" / "macro"
        macro_dir.mkdir(parents=True, exist_ok=True)
        out_path = macro_dir / "macro_intraday_1m.parquet"

        file_records = [
            {k: v for k, v in r.items() if k != "date"}
            for r in records
        ]

        if file_records:
            table = pa.Table.from_pylist(file_records)
        else:
            schema = pa.schema([
                ("ticker", pa.string()),
                ("name", pa.string()),
                ("time", pa.string()),
                ("open", pa.float64()),
                ("high", pa.float64()),
                ("low", pa.float64()),
                ("close", pa.float64()),
                ("volume", pa.float64())
            ])
            table = pa.Table.from_batches([], schema=schema)

        pq.write_table(table, out_path)
        logger.info(f"Saved {len(file_records)} intraday macro records to {out_path}")
        return out_path

