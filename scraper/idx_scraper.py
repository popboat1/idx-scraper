import asyncio
import httpx
import logging
import json
from typing import Dict, Any, List, Optional
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

# suppress verbose http request logs from internal httpx client
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def _parse_num(v: Any, is_float: bool = False) -> Any:
    if v is None or v == "":
        return 0.0 if is_float else 0
    try:
        cleaned = str(v).replace(",", "")
        return float(cleaned) if is_float else int(float(cleaned))
    except (ValueError, TypeError):
        return 0.0 if is_float else 0


def _extract_nested_value(val: Any, is_float: bool = True) -> Any:
    if val is None:
        return 0.0 if is_float else 0
    if isinstance(val, dict):
        if "value" in val:
            val = val["value"]
        if isinstance(val, dict) and "percentage" in val:
            val = val["percentage"]
        if isinstance(val, dict) and "raw" in val:
            val = val["raw"]
        elif isinstance(val, dict):
            val = next(iter(val.values()), 0)
    return _parse_num(val, is_float=is_float)


class IDXScraper:
    RETRY_STATUS_CODES = (429, 500, 502, 503, 504)

    def __init__(
        self,
        base_url: str = "https://exodus.stockbit.com",
        auth_token: str = "",
        timeout: float = 10.0,
        max_retries: int = 5,
        retry_backoff: float = 2.0,
        max_keepalive_connections: int = 20,
        max_connections: int = 50,
        rate_limit_delay: float = 1.0
    ):
        self.base_url = base_url
        self.auth_token = auth_token
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.max_keepalive_connections = max_keepalive_connections
        self.max_connections = max_connections
        self.rate_limit_delay = rate_limit_delay
        token_clean = self.auth_token.strip()
        if token_clean.startswith("Bearer "):
            token_clean = token_clean[7:].strip()
        auth_header = f"Bearer {token_clean}" if token_clean else ""
        self.headers = {
            "Authorization": auth_header,
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        }
        limits = httpx.Limits(
            max_keepalive_connections=self.max_keepalive_connections,
            max_connections=self.max_connections
        )
        self.client = httpx.AsyncClient(limits=limits, headers=self.headers, timeout=timeout)

    def set_auth_token(self, auth_token: str) -> None:
        """Dynamically update authorization token and client headers."""
        self.auth_token = auth_token
        token_clean = self.auth_token.strip()
        if token_clean.startswith("Bearer "):
            token_clean = token_clean[7:].strip()
        auth_header = f"Bearer {token_clean}" if token_clean else ""
        self.headers["Authorization"] = auth_header
        self.client.headers["Authorization"] = auth_header

    async def close(self):
        await self.client.aclose()
        
    def _reload_token_if_updated(self) -> bool:
        """reload token from environment or local .env file if updated."""
        try:
            from paper_trading.stockbit_virtual_adapter import load_token_from_env_file
            fresh_tok = load_token_from_env_file()
            if fresh_tok:
                tok_clean = fresh_tok.strip()
                if tok_clean.startswith("Bearer "):
                    tok_clean = tok_clean[7:].strip()
                cur_clean = self.auth_token.strip()
                if cur_clean.startswith("Bearer "):
                    cur_clean = cur_clean[7:].strip()
                if tok_clean and tok_clean != cur_clean:
                    logger.info("idx_scraper: detected updated STOCKBIT_TOKEN in .env; hot-reloading...")
                    self.set_auth_token(tok_clean)
                    return True
        except Exception:
            pass
        return False

    async def _request_with_backoff(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        # exponential backoff for rate limits and 5xx errors
        for attempt in range(self.max_retries):
            try:
                response = await self.client.request(method, url, **kwargs)
                if response.status_code == 401:
                    # attempt hot-reloading if .env was updated
                    if self._reload_token_if_updated():
                        response = await self.client.request(method, url, **kwargs)
                if response.status_code in self.RETRY_STATUS_CODES:
                    logger.warning(f"Got {response.status_code} for {url}. Retrying...")
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPError, json.JSONDecodeError, OSError) as e:
                if attempt == self.max_retries - 1:
                    logger.warning(f"Request failed after {self.max_retries} attempts for {url}: {e}")
                    raise
                
                if hasattr(e, 'response') and e.response is not None and e.response.status_code not in self.RETRY_STATUS_CODES and e.response.status_code >= 400:
                    if e.response.status_code == 401 and self._reload_token_if_updated():
                        continue
                    logger.warning(f"Client error {e.response.status_code} for {url}: {e}")
                    raise
                    
                delay = self.retry_backoff * (2 ** attempt)
                logger.info(f"Retrying in {delay} seconds...")
                await asyncio.sleep(delay)
        return {}

    async def fetch_order_queue(
        self,
        symbol: str,
        limit: int = 100,
        paginate: bool = False,
        max_pages: int = 10000,
        order_status: str = "ORDER_STATUS_ALL",
        sort_direction: str = "SORT_DIRECTION_ASC",
        sort_by: str = "SORT_BY_OPEN",
        action_type: str = "ACTION_TYPE_ALL",
        board_type: str = "BOARD_TYPE_ALL",
    ) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/order-trade/order-queue"
        all_data = []
        has_next_page = True
        page_count = 0
        last_exchange_order_number = None
        
        while has_next_page:
            if page_count >= max_pages:
                logger.warning(f"Reached max_pages limit ({max_pages}) for {symbol} order queue")
                break
            page_count += 1
            
            # preserve batch_fetcher pagination while allowing live caller to request open orders
            params = {
                "stock_code": symbol,
                "action_type": action_type,
                "board_type": board_type,
                "order_status": order_status,
                "limit": limit,
                "sort_by": sort_by,
                "sort_direction": sort_direction,
            }
            
            if last_exchange_order_number:
                params["last_exchange_order_number"] = last_exchange_order_number
                
            try:
                data = await self._request_with_backoff("GET", url, params=params)
                items = data.get("data", {}).get("orders", [])
                
                if not items:
                    break
                    
                for item in items:
                    def parse_num(v, is_float=False):
                        if v is None or v == "": return 0.0 if is_float else 0
                        try: return float(str(v).replace(",", "")) if is_float else int(float(str(v).replace(",", "")))
                        except: return 0.0 if is_float else 0
                        
                    raw_eon = item.get("exchange_order_number")
                    eon_val = raw_eon.get("full", "") if isinstance(raw_eon, dict) else (str(raw_eon) if raw_eon is not None else "")
                    
                    record = {
                        "id": str(item.get("id", "")),
                        "queue_number": parse_num(item.get("queue_number")),
                        "stock_code": str(item.get("stock_code", symbol)),
                        "time": str(item.get("time", "")),
                        "action_type": str(item.get("action_type", "")),
                        "price": parse_num(item.get("price"), is_float=True),
                        "status": str(item.get("status", "")),
                        "open": parse_num(item.get("open")),
                        "lot": parse_num(item.get("lot")),
                        "board_type": str(item.get("board_type", "")),
                        "broker_code": str(item.get("broker_code", "")),
                        "exchange_order_number": eon_val,
                        "queue_lot": parse_num(item.get("queue_lot")),
                        "broker_group": str(item.get("broker_group", "")),
                        "order_number": str(item.get("order_number", ""))
                    }
                    all_data.append(record)
                
                if paginate and items:
                    # fetch until empty array, cursor is the last item's exchange order number
                    last_item = items[-1]
                    raw_exchange_number = last_item.get("exchange_order_number")
                    last_exchange_order_number = raw_exchange_number.get("full", "") if isinstance(raw_exchange_number, dict) else (str(raw_exchange_number) if raw_exchange_number is not None else "")
                    
                    if not last_exchange_order_number:
                        has_next_page = False
                    else:
                        # strict sleep to prevent waf bans
                        await asyncio.sleep(self.rate_limit_delay)
                else:
                    has_next_page = False
            except (httpx.HTTPError, json.JSONDecodeError, OSError) as e:
                logger.debug(f"Error fetching order queue for {symbol}: {e}")
                raise
                
        return all_data

    async def fetch_running_trade(self, symbol: str, limit: int = 80, paginate: bool = False, max_pages: int = 10000) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/order-trade/running-trade"
        all_data = []
        has_next_page = True
        page_count = 0
        trade_number = None
        
        while has_next_page:
            if page_count >= max_pages:
                logger.warning(f"Reached max_pages limit ({max_pages}) for {symbol} running trade")
                break
            page_count += 1
            
            params = [
                ("symbols[]", symbol),
                ("sort", "DESC"),
                ("limit", str(limit)),
                ("order_by", "RUNNING_TRADE_ORDER_BY_TIME")
            ]
            
            if trade_number:
                params.append(("trade_number", str(trade_number)))
                
            query_string = urlencode(params)
            full_url = f"{url}?{query_string}"
            
            try:
                data = await self._request_with_backoff("GET", full_url)
                items = data.get("data", {}).get("running_trade", [])
                
                if not items:
                    break
                    
                for item in items:
                    def parse_num(v, is_float=False):
                        if v is None or v == "": return 0.0 if is_float else 0
                        try: return float(str(v).replace(",", "")) if is_float else int(float(str(v).replace(",", "")))
                        except: return 0.0 if is_float else 0

                    raw_val = item.get("value")
                    val_parsed = float(raw_val.get("raw", 0.0)) if isinstance(raw_val, dict) else (float(raw_val) if raw_val is not None else 0.0)

                    record = {
                        "id": str(item.get("id", "")),
                        "time": str(item.get("time", "")),
                        "code": str(item.get("code", symbol)),
                        "price": parse_num(item.get("price"), is_float=True),
                        "lot": parse_num(item.get("lot")),
                        "value": val_parsed,
                        "action": str(item.get("action", "")),
                        "buyer": str(item.get("buyer", "")),
                        "seller": str(item.get("seller", "")),
                        "buyer_type": str(item.get("buyer_type", "")),
                        "seller_type": str(item.get("seller_type", "")),
                        "market_board": str(item.get("market_board", "RG")),
                        "buy_order_number": str(item.get("buy_order_number", "")),
                        "sell_order_number": str(item.get("sell_order_number", ""))
                    }
                    all_data.append(record)
                    
                if paginate and items:
                    last_item = items[-1]
                    # use trade_number if available, fallback to id if missing
                    trade_number = last_item.get("trade_number") or last_item.get("id")
                    
                    if not trade_number:
                        has_next_page = False
                    else:
                        # strict sleep to prevent waf bans
                        await asyncio.sleep(self.rate_limit_delay)
                else:
                    has_next_page = False
            except (httpx.HTTPError, json.JSONDecodeError, OSError) as e:
                logger.debug(f"Error fetching running trade for {symbol}: {e}")
                raise
                
        return all_data

    async def fetch_market_running_trade(
        self,
        limit: int = 80,
        board: Optional[str] = "RG",
    ) -> List[Dict[str, Any]]:
        """fetch real-time market-wide running trade tape prints across all equities.
        
        queries exodus.stockbit.com/order-trade/running-trade with sort=DESC and limit=80
        without symbols[] parameter, returning raw transaction prints from the tape.
        """
        url = f"{self.base_url}/order-trade/running-trade"
        params = [
            ("sort", "DESC"),
            ("limit", str(limit)),
            ("order_by", "RUNNING_TRADE_ORDER_BY_TIME"),
        ]
        query_string = urlencode(params)
        full_url = f"{url}?{query_string}"

        data = await self._request_with_backoff("GET", full_url)
        items = data.get("data", {}).get("running_trade", [])
        if not items:
            return []

        parsed = []
        for item in items:
            m_board = str(item.get("market_board", "RG")).upper()
            if board is not None and m_board != board.upper():
                continue

            raw_code = item.get("code") or item.get("symbol", "")
            if not raw_code:
                continue

            sym = str(raw_code).strip().upper()

            def _parse_float(val: Any) -> float:
                if val is None or val == "":
                    return 0.0
                try:
                    return float(str(val).replace(",", ""))
                except (ValueError, TypeError):
                    return 0.0

            def _parse_int(val: Any) -> int:
                if val is None or val == "":
                    return 0
                try:
                    return int(float(str(val).replace(",", "")))
                except (ValueError, TypeError):
                    return 0

            price = _parse_float(item.get("price", 0))
            lot = _parse_int(item.get("lot", 0))
            raw_action = str(item.get("action", "buy")).upper()
            side = "BUY" if "BUY" in raw_action else "SELL"

            val_dict = item.get("value", {})
            val_raw = (
                float(val_dict.get("raw", 0.0))
                if isinstance(val_dict, dict)
                else price * lot * 100.0
            )

            parsed.append({
                "id": str(item.get("id", "")),
                "time": str(item.get("time", "")),
                "symbol": sym,
                "price": price,
                "lot": lot,
                "value": val_raw,
                "side": side,
                "market_board": m_board,
                "trade_number": str(item.get("trade_number", "")),
            })

        return parsed

    async def fetch_broker_summary(self) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/order-trade/broker/top"
        params = {
            "sort": "TB_SORT_BY_TOTAL_VALUE",
            "order": "ORDER_BY_DESC",
            "period": "TB_PERIOD_LAST_1_DAY",
            "market_type": "MARKET_TYPE_ALL",
            "eod_only": "true"
        }
        
        try:
            data = await self._request_with_backoff("GET", url, params=params)
            items = data.get("data", {}).get("list", [])
            records = []
            def parse_num(v, is_float=False):
                if v is None or v == "": return 0.0 if is_float else 0
                try: return float(str(v).replace(",", "")) if is_float else int(float(str(v).replace(",", "")))
                except: return 0.0 if is_float else 0

            for item in items:
                record = {
                    "broker_code": str(item.get("broker_code", "")),
                    "total_value": parse_num(item.get("total_value"), is_float=True),
                    "total_lot": parse_num(item.get("total_lot")),
                    "total_frequency": parse_num(item.get("total_frequency")),
                    "buy_value": parse_num(item.get("buy_value"), is_float=True),
                    "sell_value": parse_num(item.get("sell_value"), is_float=True),
                }
                records.append(record)
            return records
        except (httpx.HTTPError, json.JSONDecodeError, OSError) as e:
            logger.error(f"Error fetching broker summary: {e}")
            raise

    async def fetch_market_detector(self, symbol: str, date_str: str, limit: int = 25) -> Dict[str, Any]:
        """
        Fetches the market detector data (Bandar Detector & Broker Summary) for a specific symbol.
        date_str should be in YYYY-MM-DD format.
        """
        url = f"{self.base_url}/marketdetectors/{symbol}"
        params = {
            "from": date_str,
            "to": date_str,
            "transaction_type": "TRANSACTION_TYPE_NET",
            "market_board": "MARKET_BOARD_REGULER",
            "investor_type": "INVESTOR_TYPE_ALL",
            "limit": str(limit)
        }
        
        try:
            data = await self._request_with_backoff("GET", url, params=params)
            resp = data.get("data", {})
            
            bandar = resp.get("bandar_detector", {})
            if not isinstance(bandar, dict):
                bandar = {}
                
            flat_bandar = {}
            for k, v in bandar.items():
                if isinstance(v, dict):
                    for sub_k, sub_v in v.items():
                        flat_bandar[f"{k}_{sub_k}"] = sub_v
                else:
                    flat_bandar[k] = v
                    
            bandar_list = [flat_bandar] if flat_bandar else []
            
            broker_summary = resp.get("broker_summary", {})
            if isinstance(broker_summary, dict) and broker_summary:
                brokers_buy = broker_summary.get("brokers_buy", [])
                brokers_sell = broker_summary.get("brokers_sell", [])
            else:
                brokers_buy = resp.get("brokers_buy", [])
                brokers_sell = resp.get("brokers_sell", [])
                
            return {
                "bandar_detector": bandar_list,
                "brokers_buy": brokers_buy,
                "brokers_sell": brokers_sell
            }
        except (httpx.HTTPError, json.JSONDecodeError, OSError) as e:
            logger.error(f"Error fetching market detector for {symbol}: {e}")
            return {"bandar_detector": [], "brokers_buy": [], "brokers_sell": []}

    async def fetch_ihsg_intraday_prices(self) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/charts/IHSG/daily?timeframe=today"
        try:
            data = await self._request_with_backoff("GET", url)
            items = data.get("data", {}).get("prices", []) if isinstance(data, dict) else []
            
            records = []
            for item in items:
                record = {
                    "symbol": "IHSG",
                    "time": str(item.get("formatted_date") or item.get("time", "")),
                    "raw_timestamp": str(item.get("date", "")),
                    "xlabel": str(item.get("xlabel", "")),
                    "value": _parse_num(item.get("value"), is_float=True),
                    "percentage": _parse_num(item.get("percentage"), is_float=True),
                    "change": _parse_num(item.get("change"), is_float=True),
                    "open": _parse_num(item.get("open"), is_float=True),
                    "high": _parse_num(item.get("high"), is_float=True),
                    "low": _parse_num(item.get("low"), is_float=True),
                    "volume": _parse_num(item.get("volume"), is_float=True),
                }
                records.append(record)
            return records
        except (httpx.HTTPError, json.JSONDecodeError, OSError) as e:
            logger.error(f"Error fetching IHSG intraday prices: {e}")
            raise

    async def fetch_ihsg_market_summary(self) -> Dict[str, Any]:
        url = f"{self.base_url}/findata-view/foreign-domestic/v1/chart-data/IHSG?market_type=MARKET_TYPE_REGULAR&period=PERIOD_RANGE_1D"
        try:
            data = await self._request_with_backoff("GET", url)
            summary_data = data.get("data", {}) if isinstance(data, dict) else {}
            raw_summary = summary_data.get("summary", {}) if isinstance(summary_data, dict) else {}
            raw_val = summary_data.get("value", {}) if isinstance(summary_data, dict) else {}
            raw_vol = summary_data.get("volume", {}) if isinstance(summary_data, dict) else {}
            raw_freq = summary_data.get("frequency", {}) if isinstance(summary_data, dict) else {}

            return {
                "foreign_buy_val": _extract_nested_value(raw_summary.get("foreign_buy")),
                "foreign_sell_val": _extract_nested_value(raw_summary.get("foreign_sell")),
                "net_foreign_val": _extract_nested_value(raw_summary.get("net_foreign")),
                "domestic_buy_val": _extract_nested_value(raw_summary.get("domestic_buy")),
                "domestic_sell_val": _extract_nested_value(raw_summary.get("domestic_sell")),
                "net_domestic_val": _extract_nested_value(raw_summary.get("net_domestic")),
                "total_val": _extract_nested_value(raw_val.get("total")),
                "foreign_val_pct": _extract_nested_value(raw_val.get("foreign_total")),
                "domestic_val_pct": _extract_nested_value(raw_val.get("domestic_total")),
                "total_vol": _extract_nested_value(raw_vol.get("total")),
                "foreign_vol_pct": _extract_nested_value(raw_vol.get("foreign_total")),
                "domestic_vol_pct": _extract_nested_value(raw_vol.get("domestic_total")),
                "total_freq": _extract_nested_value(raw_freq.get("total"), is_float=False),
                "from_date": str(summary_data.get("from", "")),
                "to_date": str(summary_data.get("to", "")),
                "raw_json": json.dumps(data)
            }
        except (httpx.HTTPError, json.JSONDecodeError, OSError) as e:
            logger.error(f"Error fetching IHSG market summary: {e}")
            raise

    async def fetch_company_orderbook(self, symbol: str) -> Dict[str, Any]:
        """fetch company orderbook feed containing official ara/arb, previous close, and market volume breakdown."""
        url = f"{self.base_url}/company-price-feed/v2/orderbook/companies/{symbol}"
        try:
            data = await self._request_with_backoff("GET", url)
            resp = data.get("data", {}) if isinstance(data, dict) else {}

            ara_val = resp.get("ara", {}).get("value") if isinstance(resp.get("ara"), dict) else resp.get("ara")
            arb_val = resp.get("arb", {}).get("value") if isinstance(resp.get("arb"), dict) else resp.get("arb")

            reg_vol, reg_val = 0.0, 0.0
            nego_vol, nego_val = 0.0, 0.0
            cash_vol, cash_val = 0.0, 0.0

            for md in resp.get("market_data", []):
                label = str(md.get("label", "")).lower()
                vol = _extract_nested_value(md.get("volume"), is_float=True)
                val = _extract_nested_value(md.get("value"), is_float=True)
                if label == "regular":
                    reg_vol, reg_val = vol, val
                elif label == "nego":
                    nego_vol, nego_val = vol, val
                elif label == "cash":
                    cash_vol, cash_val = vol, val

            return {
                "symbol": str(resp.get("symbol", symbol)),
                "ara_price": _parse_num(ara_val, is_float=True),
                "arb_price": _parse_num(arb_val, is_float=True),
                "previous_close": _parse_num(resp.get("previous"), is_float=True),
                "open_price": _parse_num(resp.get("open"), is_float=True),
                "high_price": _parse_num(resp.get("high"), is_float=True),
                "low_price": _parse_num(resp.get("low"), is_float=True),
                "close_price": _parse_num(resp.get("close"), is_float=True),
                "avg_price": _parse_num(resp.get("average"), is_float=True),
                "volume": _parse_num(resp.get("volume"), is_float=True),
                "value": _parse_num(resp.get("value"), is_float=True),
                "frequency": _parse_num(resp.get("frequency"), is_float=False),
                "reguler_val": float(reg_val),
                "reguler_vol": float(reg_vol),
                "nego_val": float(nego_val),
                "nego_vol": float(nego_vol),
                "cash_val": float(cash_val),
                "cash_vol": float(cash_vol),
                "foreign_buy_val": _parse_num(resp.get("fbuy"), is_float=True),
                "foreign_sell_val": _parse_num(resp.get("fsell"), is_float=True),
                "foreign_net_val": _parse_num(resp.get("fnet"), is_float=True),
                "raw_json": json.dumps(data),
            }
        except (httpx.HTTPError, json.JSONDecodeError, OSError) as e:
            logger.error(f"Error fetching company orderbook for {symbol}: {e}")
            raise

    async def fetch_trade_book_chart(
        self,
        symbol: str,
        date_str: str,
        time_interval: str = "1m",
    ) -> Dict[str, Any]:
        """fetch 1m trade-book chart time series including price steps and big money flow."""
        url = f"{self.base_url}/order-trade/trade-book/chart"
        params = {
            "symbol": symbol,
            "time_interval": time_interval,
            "date": date_str,
        }
        try:
            data = await self._request_with_backoff("GET", url, params=params)
            resp = data.get("data", {}) if isinstance(data, dict) else {}

            prices = []
            for item in resp.get("prices", []):
                val = item.get("value", {})
                px = _parse_num(val.get("raw") if isinstance(val, dict) else val, is_float=True)
                prices.append({"time": str(item.get("time", "")), "price": px})

            big_money = []
            for item in resp.get("big_money_net_values", []):
                val = item.get("value", {})
                v = _parse_num(val.get("raw") if isinstance(val, dict) else val, is_float=True)
                big_money.append({"time": str(item.get("time", "")), "value": v})

            return {
                "symbol": symbol,
                "date": str(resp.get("date", date_str)),
                "is_fca_stock": bool(resp.get("is_fca_stock", False)),
                "prices": prices,
                "big_money_net_values": big_money,
                "raw_json": json.dumps(data),
            }
        except (httpx.HTTPError, json.JSONDecodeError, OSError) as e:
            logger.error(f"Error fetching trade book chart for {symbol} on {date_str}: {e}")
            raise

