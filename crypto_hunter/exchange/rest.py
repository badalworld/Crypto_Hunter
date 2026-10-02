"""
MEXC Futures REST client.

* Signing per MEXC Futures OpenAPI: ``HMAC_SHA256(secret, accessKey + reqTime + paramString)``
  where ``paramString`` is the dictionary-sorted query string for GET/DELETE and the raw
  JSON body for POST.
* Pooled aiohttp session with keep-alive, bounded concurrency, request timeouts.
* Rate limiting via ``RateLimiter`` (95 % of documented limits by default).
* Retries with exponential backoff + jitter for network errors / 5xx / 429.
  Business errors (4xx-style ``code`` values) are surfaced immediately as ``MexcAPIError``.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import random
import time
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import aiohttp

from ..security import Credentials
from .models import AccountAsset, Contract, ExchangePosition, MexcAPIError, Ticker
from .rate_limiter import RateLimiter

log = logging.getLogger("ch.rest")

PUBLIC_PREFIX = "/api/v1/contract/"


class MexcFuturesREST:
    def __init__(
        self,
        base_url: str,
        rate_limiter: RateLimiter,
        credentials: Optional[Credentials] = None,
        legacy_base_url: Optional[str] = None,
        timeout_sec: float = 10.0,
        max_retries: int = 4,
        backoff_base: float = 0.25,
        pool_size: int = 32,
        recv_window_sec: int = 10,
    ):
        self.base_url = base_url.rstrip("/")
        self.legacy_base_url = (legacy_base_url or "").rstrip("/") or None
        self.rl = rate_limiter
        self.creds = credentials
        self.timeout = aiohttp.ClientTimeout(total=timeout_sec, connect=min(5.0, timeout_sec))
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.pool_size = pool_size
        self.recv_window = recv_window_sec
        self._session: Optional[aiohttp.ClientSession] = None
        self._time_offset_ms = 0
        self.last_latency_ms: float = 0.0

    # ----------------------------------------------------------------- session
    async def start(self) -> None:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=self.pool_size, limit_per_host=self.pool_size, ttl_dns_cache=300,
                enable_cleanup_closed=True, keepalive_timeout=30,
            )
            self._session = aiohttp.ClientSession(connector=connector, timeout=self.timeout, json_serialize=json.dumps)

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    def set_credentials(self, creds: Optional[Credentials]) -> None:
        self.creds = creds

    @property
    def session(self) -> aiohttp.ClientSession:
        assert self._session is not None, "REST client not started"
        return self._session

    # ----------------------------------------------------------------- signing
    @staticmethod
    def _query_string(params: Dict[str, Any]) -> str:
        items = sorted((k, v) for k, v in params.items() if v is not None)
        return "&".join(f"{k}={quote(str(v), safe='')}" for k, v in items)

    def _sign(self, req_time: str, param_string: str) -> str:
        assert self.creds is not None
        payload = f"{self.creds.api_key}{req_time}{param_string}"
        return hmac.new(self.creds.api_secret.encode(), payload.encode(), hashlib.sha256).hexdigest()

    def _now_ms(self) -> int:
        return int(time.time() * 1000) + self._time_offset_ms

    # ----------------------------------------------------------------- request
    async def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None,
                       body: Optional[Any] = None, private: bool = False,
                       base_url: Optional[str] = None) -> Any:
        if private and self.creds is None:
            raise MexcAPIError(401, "API credentials not configured", path)
        params = {k: v for k, v in (params or {}).items() if v is not None}
        if isinstance(body, dict):
            body = {k: v for k, v in body.items() if v is not None}

        attempt = 0
        while True:
            await self.rl.acquire(path)
            headers = {"Content-Type": "application/json", "Language": "English"}
            data: Optional[bytes] = None
            if method == "POST" and body is not None:
                raw = json.dumps(body, separators=(",", ":"))
                data = raw.encode()
                param_string = raw
            else:
                param_string = self._query_string(params)
            if private:
                req_time = str(self._now_ms())
                headers.update({
                    "ApiKey": self.creds.api_key,  # type: ignore[union-attr]
                    "Request-Time": req_time,
                    "Signature": self._sign(req_time, param_string),
                    "Recv-Window": str(self.recv_window),
                })
            url = f"{base_url or self.base_url}{path}"
            if method != "POST" and param_string:
                url = f"{url}?{param_string}"

            t0 = time.perf_counter()
            try:
                async with self.session.request(method, url, headers=headers, data=data) as resp:
                    text = await resp.text()
                    self.last_latency_ms = (time.perf_counter() - t0) * 1000
                    if resp.status == 429 or resp.status >= 500:
                        raise MexcAPIError(resp.status, f"HTTP {resp.status}: {text[:200]}", path)
                    try:
                        payload = json.loads(text) if text else {}
                    except json.JSONDecodeError:
                        raise MexcAPIError(resp.status, f"Non-JSON response: {text[:200]}", path)
                    if isinstance(payload, dict) and payload.get("success") is False:
                        code = int(payload.get("code", resp.status) or resp.status)
                        msg = str(payload.get("message") or payload.get("msg") or "unknown error")
                        if code == 10073 or "Request-Time" in msg:
                            self._time_offset_ms = 0
                        raise MexcAPIError(code, msg, path, payload)
                    if resp.status >= 400:
                        raise MexcAPIError(resp.status, f"HTTP {resp.status}: {text[:200]}", path)
                    return payload.get("data") if isinstance(payload, dict) and "data" in payload else payload
            except (aiohttp.ClientError, asyncio.TimeoutError, MexcAPIError) as exc:
                retryable = not isinstance(exc, MexcAPIError) or exc.retryable
                if not retryable or attempt >= self.max_retries:
                    raise
                attempt += 1
                delay = self.backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 0.1)
                log.warning("%s %s failed (%s) – retry %d/%d in %.2fs", method, path, exc, attempt, self.max_retries, delay)
                await asyncio.sleep(delay)

    async def _public_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        try:
            return await self._request("GET", path, params)
        except (aiohttp.ClientError, asyncio.TimeoutError, MexcAPIError) as exc:
            if self.legacy_base_url and self.legacy_base_url != self.base_url:
                log.warning("Public %s failed on %s (%s) – trying %s", path, self.base_url, exc, self.legacy_base_url)
                return await self._request("GET", path, params, base_url=self.legacy_base_url)
            raise

    # ====================================================================== public
    async def ping(self) -> int:
        data = await self._public_get(PUBLIC_PREFIX + "ping")
        server_ms = int(data) if isinstance(data, (int, float, str)) and str(data).isdigit() else int(time.time() * 1000)
        self._time_offset_ms = server_ms - int(time.time() * 1000)
        return server_ms

    async def get_contracts(self) -> List[Contract]:
        data = await self._public_get(PUBLIC_PREFIX + "detail")
        return [Contract.from_api(d) for d in (data or [])]

    async def get_tickers(self) -> List[Ticker]:
        data = await self._public_get(PUBLIC_PREFIX + "ticker")
        return [Ticker.from_api(d) for d in (data or []) if d.get("symbol")]

    async def get_ticker(self, symbol: str) -> Ticker:
        data = await self._public_get(PUBLIC_PREFIX + "ticker", {"symbol": symbol})
        return Ticker.from_api(data)

    async def get_kline(self, symbol: str, interval: str = "Min5", start: Optional[int] = None,
                        end: Optional[int] = None) -> Dict[str, List[float]]:
        """Returns columnar kline data: time/open/close/high/low/vol/amount (seconds)."""
        data = await self._public_get(PUBLIC_PREFIX + f"kline/{symbol}", {"interval": interval, "start": start, "end": end})
        return data or {"time": [], "open": [], "close": [], "high": [], "low": [], "vol": [], "amount": []}

    # ===================================================================== account
    async def get_assets(self) -> List[AccountAsset]:
        data = await self._request("GET", "/api/v1/private/account/assets", private=True)
        return [AccountAsset.from_api(d) for d in (data or [])]

    async def get_asset(self, currency: str = "USDT") -> AccountAsset:
        for a in await self.get_assets():
            if a.currency == currency:
                return a
        return AccountAsset(currency, 0, 0, 0, 0, 0, 0)

    async def get_open_positions(self, symbol: Optional[str] = None) -> List[ExchangePosition]:
        data = await self._request("GET", "/api/v1/private/position/open_positions", {"symbol": symbol}, private=True)
        return [ExchangePosition.from_api(d) for d in (data or [])]

    async def get_history_positions(self, symbol: Optional[str] = None, page_size: int = 20) -> List[Dict[str, Any]]:
        data = await self._request("GET", "/api/v1/private/position/list/history_positions",
                                   {"symbol": symbol, "page_num": 1, "page_size": page_size}, private=True)
        return list(data or [])

    async def get_position_mode(self) -> int:
        data = await self._request("GET", "/api/v1/private/position/position_mode", private=True)
        return int(data or 1)

    async def change_leverage(self, symbol: str, leverage: int, open_type: int, position_type: int,
                              position_id: Optional[int] = None) -> Any:
        body: Dict[str, Any] = {"leverage": leverage}
        if position_id:
            body["positionId"] = position_id
        else:
            body.update({"openType": open_type, "symbol": symbol, "positionType": position_type})
        return await self._request("POST", "/api/v1/private/position/change_leverage", body=body, private=True)

    # ====================================================================== orders
    async def create_order(self, symbol: str, vol: float, side: int, order_type: int, open_type: int,
                           price: Optional[float] = None, leverage: Optional[int] = None,
                           position_id: Optional[int] = None, external_oid: Optional[str] = None,
                           stop_loss_price: Optional[float] = None, take_profit_price: Optional[float] = None,
                           loss_trend: Optional[int] = None, profit_trend: Optional[int] = None,
                           position_mode: Optional[int] = None, reduce_only: Optional[bool] = None) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "symbol": symbol, "price": price if price is not None else 0, "vol": vol, "side": side,
            "type": order_type, "openType": open_type, "leverage": leverage, "positionId": position_id,
            "externalOid": external_oid, "stopLossPrice": stop_loss_price, "takeProfitPrice": take_profit_price,
            "lossTrend": loss_trend, "profitTrend": profit_trend, "positionMode": position_mode,
            "reduceOnly": reduce_only,
        }
        data = await self._request("POST", "/api/v1/private/order/create", body=body, private=True)
        if isinstance(data, dict):
            return data
        return {"orderId": str(data)}

    async def get_order(self, order_id: str) -> Dict[str, Any]:
        return await self._request("GET", f"/api/v1/private/order/get/{order_id}", private=True) or {}

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> Any:
        return await self._request("POST", "/api/v1/private/order/cancel_all", body={"symbol": symbol}, private=True)

    # ---------------------------------------------------------------- TP/SL (position)
    async def get_stop_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        data = await self._request("GET", "/api/v1/private/stoporder/open_orders", {"symbol": symbol}, private=True)
        return list(data or [])

    async def change_plan_price(self, stop_plan_order_id: str | int, stop_loss_price: Optional[float] = None,
                                take_profit_price: Optional[float] = None, loss_trend: Optional[int] = None,
                                profit_trend: Optional[int] = None) -> Any:
        body = {"stopPlanOrderId": int(stop_plan_order_id), "stopLossPrice": stop_loss_price,
                "takeProfitPrice": take_profit_price, "lossTrend": loss_trend, "profitTrend": profit_trend}
        return await self._request("POST", "/api/v1/private/stoporder/change_plan_price", body=body, private=True)

    async def cancel_stop_orders(self, ids: List[str | int]) -> Any:
        body = [{"stopPlanOrderId": int(i)} for i in ids]
        return await self._request("POST", "/api/v1/private/stoporder/cancel", body=body, private=True)  # type: ignore[arg-type]

    async def cancel_all_stop_orders(self, symbol: Optional[str] = None, position_id: Optional[int] = None) -> Any:
        return await self._request("POST", "/api/v1/private/stoporder/cancel_all",
                                   body={"symbol": symbol, "positionId": position_id}, private=True)

    # ---------------------------------------------------------------- trigger (plan) orders – fallback
    async def place_plan_order(self, symbol: str, vol: float, side: int, open_type: int, trigger_price: float,
                               trigger_type: int, trend: int, leverage: int, order_type: int = 5,
                               price: Optional[float] = None, execute_cycle: int = 2,
                               position_mode: Optional[int] = None, reduce_only: Optional[bool] = None) -> Any:
        body = {"symbol": symbol, "price": price, "vol": vol, "leverage": leverage, "side": side,
                "openType": open_type, "triggerPrice": trigger_price, "triggerType": trigger_type,
                "executeCycle": execute_cycle, "orderType": order_type, "trend": trend,
                "positionMode": position_mode, "reduceOnly": reduce_only}
        return await self._request("POST", "/api/v1/private/planorder/place/v2", body=body, private=True)

    async def cancel_plan_orders(self, symbol: str, order_ids: List[str | int]) -> Any:
        body = [{"symbol": symbol, "orderId": str(i)} for i in order_ids]
        return await self._request("POST", "/api/v1/private/planorder/cancel", body=body, private=True)  # type: ignore[arg-type]

    async def get_plan_orders(self, symbol: Optional[str] = None, states: str = "1") -> List[Dict[str, Any]]:
        now = int(time.time() * 1000)
        data = await self._request("GET", "/api/v1/private/planorder/list/orders",
                                   {"symbol": symbol, "states": states, "start_time": now - 7 * 86400_000,
                                    "end_time": now, "page_num": 1, "page_size": 100}, private=True)
        return list(data or [])
