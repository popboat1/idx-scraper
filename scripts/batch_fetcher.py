import asyncio
import logging
import os
from datetime import datetime, timedelta
import pytz
from pathlib import Path
import sys
from typing import Optional
import yaml
import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# ensure scraper modules can be imported
sys.path.append(str(Path(__file__).resolve().parent.parent))

from scraper.idx_scraper import IDXScraper
from scraper.universe import UniverseManager
from scraper.external_data import MacroDataFetcher

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# suppress verbose http request logs from httpx client during scraping
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

WIB = pytz.timezone('Asia/Jakarta')

def resolve_trading_date(dt: datetime) -> datetime:
    """Returns the active trading date for data scraping.
    
    If run before 09:00 WIB on Mon-Fri, targets the previous trading day.
    If Saturday or Sunday, rolls back to Friday.
    """
    # before market open (09:00 WIB), data belongs to yesterday's session
    if dt.hour < 9:
        dt = dt - timedelta(days=1)
        
    if dt.weekday() == 5: # Saturday -> Friday
        return dt - timedelta(days=1)
    elif dt.weekday() == 6: # Sunday -> Friday
        return dt - timedelta(days=2)
    return dt

def get_next_run_time(now: datetime) -> datetime:
    """Calculates the sleep time until the next 19:00 WIB that falls on Mon-Fri."""
    target = now.replace(hour=19, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    
    while target.weekday() >= 5: # 5 is Sat, 6 is Sun
        target += timedelta(days=1)
        
    return target

def normalize_broker_summary(brokers_buy: list, brokers_sell: list, symbol: str = "", date_str: str = "") -> list[dict]:
    """Normalizes BUY and SELL broker summary items into a uniform schema."""
    normalized = []
    for b in brokers_buy or []:
        broker_code = b.get("netbs_broker_code") or b.get("broker_code") or b.get("broker", "")
        lot = b.get("blot") if b.get("blot") is not None else b.get("lot", 0)
        lot_vol = b.get("blotv") if b.get("blotv") is not None else b.get("lot_vol", b.get("vol", 0))
        val = b.get("bval") if b.get("bval") is not None else b.get("val", 0)
        val_vol = b.get("bvalv") if b.get("bvalv") is not None else b.get("val_vol", 0)
        avg_price = b.get("netbs_buy_avg_price") if b.get("netbs_buy_avg_price") is not None else b.get("avg_price", b.get("price", 0))
        freq = b.get("freq", 0)
        
        normalized.append({
            "broker_code": str(broker_code),
            "side": "BUY",
            "type": str(b.get("type", "")),
            "lot": float(lot) if lot is not None else 0.0,
            "lot_vol": float(lot_vol) if lot_vol is not None else 0.0,
            "val": float(val) if val is not None else 0.0,
            "val_vol": float(val_vol) if val_vol is not None else 0.0,
            "avg_price": float(avg_price) if avg_price is not None else 0.0,
            "freq": int(freq) if freq is not None else 0,
        })
        
    for s in brokers_sell or []:
        broker_code = s.get("netbs_broker_code") or s.get("broker_code") or s.get("broker", "")
        lot = s.get("slot") if s.get("slot") is not None else s.get("lot", 0)
        lot_vol = s.get("slotv") if s.get("slotv") is not None else s.get("lot_vol", s.get("vol", 0))
        val = s.get("sval") if s.get("sval") is not None else s.get("val", 0)
        val_vol = s.get("svalv") if s.get("svalv") is not None else s.get("val_vol", 0)
        avg_price = s.get("netbs_sell_avg_price") if s.get("netbs_sell_avg_price") is not None else s.get("avg_price", s.get("price", 0))
        freq = s.get("freq", 0)
        
        normalized.append({
            "broker_code": str(broker_code),
            "side": "SELL",
            "type": str(s.get("type", "")),
            "lot": abs(float(lot)) if lot is not None else 0.0,
            "lot_vol": abs(float(lot_vol)) if lot_vol is not None else 0.0,
            "val": abs(float(val)) if val is not None else 0.0,
            "val_vol": abs(float(val_vol)) if val_vol is not None else 0.0,
            "avg_price": float(avg_price) if avg_price is not None else 0.0,
            "freq": int(freq) if freq is not None else 0,
        })
    return normalized

def is_symbol_scraped(symbol_dir: Path) -> bool:
    """Checks if symbol directory already contains non-empty parquet data."""
    if not symbol_dir.exists():
        return False
    pq_files = list(symbol_dir.glob("*.parquet"))
    if not pq_files:
        return False
    return any(f.stat().st_size > 0 for f in pq_files)

async def fetch_ihsg_data(scraper: IDXScraper, base_output_dir: Path, date_str: str):
    """Fetches IHSG 1-minute intraday prices and foreign/domestic market summary."""
    ihsg_dir = base_output_dir / f"date={date_str}" / "symbol=IHSG"
    ihsg_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Intraday Prices
    try:
        prices = await scraper.fetch_ihsg_intraday_prices()
        if prices and isinstance(prices, list):
            table_prices = pa.Table.from_pylist(prices)
            if "symbol" in table_prices.column_names:
                col_idx = table_prices.schema.get_field_index("symbol")
                table_prices = table_prices.set_column(col_idx, "symbol", pc.dictionary_encode(table_prices["symbol"]))
            prices_path = ihsg_dir / "ihsg_intraday_1m.parquet"
            pq.write_table(table_prices, prices_path)
            logger.info(f"Saved {len(prices)} IHSG intraday prices to {prices_path}")
    except Exception as e:
        logger.error(f"Failed to fetch/save IHSG intraday prices: {e}")
        
    # 2. Market Summary
    try:
        summary = await scraper.fetch_ihsg_market_summary()
        if summary and isinstance(summary, (dict, list)):
            summary_list = summary if isinstance(summary, list) else [summary]
            table_summary = pa.Table.from_pylist(summary_list)
            if "symbol" in table_summary.column_names:
                col_idx = table_summary.schema.get_field_index("symbol")
                table_summary = table_summary.set_column(col_idx, "symbol", pc.dictionary_encode(table_summary["symbol"]))
            summary_path = ihsg_dir / "ihsg_market_summary.parquet"
            pq.write_table(table_summary, summary_path)
            logger.info(f"Saved IHSG market summary to {summary_path}")
    except Exception as e:
        logger.error(f"Failed to fetch/save IHSG market summary: {e}")

def fetch_macro_data(base_output_dir: Path, date_str: str):
    """Fetches global macro indices, FX rates, and commodities (daily & 1m intraday) and saves to parquet."""
    results = {}
    fetcher = MacroDataFetcher()
    try:
        daily_path = fetcher.save_macro_parquet(base_output_dir=base_output_dir, date_str=date_str)
        logger.info(f"Saved daily macro data to {daily_path}")
        results["daily"] = daily_path
    except Exception as e:
        logger.error(f"Failed to fetch/save daily macro data: {e}")

    try:
        intraday_path = fetcher.save_macro_intraday_parquet(base_output_dir=base_output_dir, date_str=date_str)
        logger.info(f"Saved intraday 1m macro data to {intraday_path}")
        results["intraday"] = intraday_path
    except Exception as e:
        logger.error(f"Failed to fetch/save intraday 1m macro data: {e}")

    return results

async def fetch_single_symbol(
    scraper: IDXScraper,
    symbol: str,
    symbol_dir: Path,
    date_str: str,
    api_date_str: str,
    force_recompute: bool = False,
) -> None:
    """Fetches order queue, running trades, and market detector for a single symbol."""
    if not force_recompute and is_symbol_scraped(symbol_dir):
        logger.info(f"[{symbol}] Already scraped for date={date_str}. Skipping.")
        return
        
    symbol_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Fetch Order Queue
    try:
        queue_data = await scraper.fetch_order_queue(
            symbol=symbol, 
            limit=100, 
            paginate=True, 
            max_pages=10000
        )
        if queue_data:
            table_q = pa.Table.from_pylist(queue_data)
            q_path = symbol_dir / "order_queue.parquet"
            pq.write_table(table_q, q_path)
            logger.info(f"[{symbol}] Saved {len(queue_data)} orders to {q_path}")
    except Exception as e:
        logger.error(f"Failed to fetch/save order queue for {symbol}: {e}")
        
    # 2. Fetch Running Trades
    try:
        trades = await scraper.fetch_running_trade(
            symbol=symbol, 
            limit=100, 
            paginate=True, 
            max_pages=10000
        )
        if trades:
            table_trades = pa.Table.from_pylist(trades)
            trades_path = symbol_dir / "running_trades.parquet"
            pq.write_table(table_trades, trades_path)
            logger.info(f"[{symbol}] Saved {len(trades)} running trades to {trades_path}")
    except Exception as e:
        logger.error(f"Failed to fetch/save running trades for {symbol}: {e}")
        
    # 3. Fetch Market Detector
    try:
        detector_data = await scraper.fetch_market_detector(symbol, api_date_str)
        if detector_data:
            bandar_list = detector_data.get("bandar_detector", [])
            if bandar_list:
                table_bandar = pa.Table.from_pylist(bandar_list)
                pq.write_table(table_bandar, symbol_dir / "market_detector.parquet")
                
            brokers_buy = detector_data.get("brokers_buy", [])
            brokers_sell = detector_data.get("brokers_sell", [])
            brokers_all = normalize_broker_summary(brokers_buy, brokers_sell, symbol, date_str)
            if brokers_all:
                table_brokers = pa.Table.from_pylist(brokers_all)
                pq.write_table(table_brokers, symbol_dir / "broker_summary.parquet")
                
            logger.info(f"[{symbol}] Saved market detector data to {symbol_dir}")
    except Exception as e:
        logger.error(f"Failed to fetch/save market detector for {symbol}: {e}")

async def fetch_cycle(
    scraper: IDXScraper,
    symbols: list,
    base_output_dir: Path,
    concurrency: int = 3,
    force_recompute: bool = False,
    target_date: Optional[str] = None,
):
    if target_date:
        # parse explicit target date string YYYYMMDD
        trading_date = datetime.strptime(target_date, "%Y%m%d")
    else:
        now_wib = datetime.now(WIB)
        trading_date = resolve_trading_date(now_wib)
    date_str = trading_date.strftime("%Y%m%d")
    api_date_str = trading_date.strftime("%Y-%m-%d")
    
    logger.info(
        f"Starting batch fetch for {len(symbols)} symbols (concurrency={concurrency}, "
        f"force_recompute={force_recompute}). Trading Date: {date_str}"
    )
    
    sem = asyncio.Semaphore(max(1, int(concurrency)))
    
    async def worker(idx: int, symbol: str):
        async with sem:
            logger.info(f"[{idx+1}/{len(symbols)}] Processing {symbol}...")
            symbol_dir = base_output_dir / f"date={date_str}" / f"symbol={symbol}"
            await fetch_single_symbol(
                scraper=scraper,
                symbol=symbol,
                symbol_dir=symbol_dir,
                date_str=date_str,
                api_date_str=api_date_str,
                force_recompute=force_recompute,
            )
            await asyncio.sleep(scraper.rate_limit_delay)
            
    tasks = [worker(i, sym) for i, sym in enumerate(symbols)]
    await asyncio.gather(*tasks)
        
    # fetch IHSG market summary & intraday prices
    logger.info("Fetching IHSG data...")
    await fetch_ihsg_data(scraper, base_output_dir, date_str)

    # fetch global macro data
    logger.info("Fetching global macro data...")
    fetch_macro_data(base_output_dir, date_str)

def load_stockbit_token(env_path: Optional[Path] = None) -> str:
    """Dynamically loads STOCKBIT_TOKEN from .env file or environment variables,
    and logs JWT expiration status for operational observability.
    """
    token = ""
    target_env = env_path if env_path is not None else Path(__file__).resolve().parent.parent / ".env"
    if target_env.exists():
        with open(target_env, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("STOCKBIT_TOKEN="):
                    token = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break
    if not token:
        token = os.environ.get("STOCKBIT_TOKEN", "").strip().strip('"').strip("'")

    if token:
        try:
            import base64
            import json
            raw = token[7:].strip() if token.startswith("Bearer ") else token
            payload_b64 = raw.split(".")[1]
            payload_b64 += "=" * (-len(payload_b64) % 4)
            payload = json.loads(base64.b64decode(payload_b64).decode("utf-8"))
            exp = payload.get("exp")
            if exp:
                exp_dt = datetime.fromtimestamp(exp, tz=WIB)
                now_dt = datetime.now(WIB)
                remaining = (exp_dt - now_dt).total_seconds()
                if remaining <= 0:
                    logger.error(
                        f"STOCKBIT_TOKEN has EXPIRED at {exp_dt.strftime('%Y-%m-%d %H:%M:%S')} WIB "
                        f"({abs(remaining)/60:.1f} minutes ago)!"
                    )
                elif remaining < 7200:
                    logger.warning(
                        f"STOCKBIT_TOKEN will expire soon at {exp_dt.strftime('%Y-%m-%d %H:%M:%S')} WIB "
                        f"(in {remaining/60:.1f} minutes)."
                    )
                else:
                    logger.info(
                        f"STOCKBIT_TOKEN is valid until {exp_dt.strftime('%Y-%m-%d %H:%M:%S')} WIB "
                        f"({remaining/3600:.1f} hours remaining)."
                    )
        except Exception as e:
            logger.debug(f"Could not parse JWT token expiry: {e}")

    return token

def parse_cli_args(args: Optional[list[str]] = None):
    import argparse
    parser = argparse.ArgumentParser(description="IDX LOB & Market Data Batch Fetcher")
    parser.add_argument("--run-now", action="store_true", help="Run fetch cycle immediately")
    parser.add_argument("--force-recompute", action="store_true", help="Force re-fetch of symbols even if already scraped")
    parser.add_argument("--concurrency", type=int, default=None, help="Number of concurrent symbol workers")
    parser.add_argument("--target-date", type=str, default=None, help="Explicit target trading date (YYYYMMDD) to scrape")
    parser.add_argument("--once", action="store_true", help="Run single fetch cycle and exit cleanly without entering 24h daemon sleep loop")
    return parser.parse_known_args(args)[0]

async def main():
    args = parse_cli_args()
    
    token = load_stockbit_token()
    if not token:
        logger.error("STOCKBIT_TOKEN not found in .env or environment. Please set it before running.")
        sys.exit(1)

    config_path = Path(__file__).resolve().parent.parent / "configs" / "default.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    idx_config = config.get("idx_scraper", {})

    base_output_dir = Path(idx_config.get("output_dir", "idx_data"))
    concurrency = args.concurrency if args.concurrency is not None else idx_config.get("concurrency", 3)
    force_recompute = args.force_recompute

    scraper = IDXScraper(
        base_url=idx_config.get("base_url", "https://exodus.stockbit.com"),
        auth_token=token,
        timeout=idx_config.get("timeout", 10.0),
        max_retries=idx_config.get("max_retries", 5),
        retry_backoff=idx_config.get("retry_backoff", 2.0),
        rate_limit_delay=idx_config.get("rate_limit_delay", 1.0),
    )

    universe = UniverseManager()
    symbols = universe.get_all_symbols()

    try:
        # run immediate fetch cycle if triggered via --run-now or one-shot mode --once
        if args.run_now or args.once:
            logger.info("Manual trigger (--run-now/--once) detected. Running fetch cycle immediately.")
            fresh_token = load_stockbit_token()
            if fresh_token:
                scraper.set_auth_token(fresh_token)
            await fetch_cycle(
                scraper=scraper,
                symbols=symbols,
                base_output_dir=base_output_dir,
                concurrency=concurrency,
                force_recompute=force_recompute,
                target_date=args.target_date,
            )
            if args.once:
                # clean exit for one-shot batch tasks (e.g. github actions workflow)
                logger.info("One-shot fetch cycle completed (--once). Exiting cleanly.")
                return

        while True:
            now_wib = datetime.now(WIB)
            next_run = get_next_run_time(now_wib)

            sleep_seconds = (next_run - now_wib).total_seconds()
            logger.info(f"Current time: {now_wib}. Sleeping until {next_run} ({sleep_seconds} seconds)")

            await asyncio.sleep(sleep_seconds)

            # dynamically reload token from .env before every fetch cycle
            fresh_token = load_stockbit_token()
            if fresh_token:
                scraper.set_auth_token(fresh_token)
            else:
                logger.error("No valid STOCKBIT_TOKEN found before scheduled fetch cycle!")

            # dynamically reload universe from configs/idx_universe.csv before every scheduled fetch cycle
            universe = UniverseManager()
            symbols = universe.get_all_symbols()
            logger.info(f"Loaded {len(symbols)} symbols from universe for scheduled batch scrape.")

            await fetch_cycle(
                scraper=scraper,
                symbols=symbols,
                base_output_dir=base_output_dir,
                concurrency=concurrency,
                force_recompute=force_recompute,
                target_date=args.target_date,
            )

    finally:
        await scraper.close()
        logger.info("Daemon stopped.")


if __name__ == "__main__":
    asyncio.run(main())

