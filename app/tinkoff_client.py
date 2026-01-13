"""Клиент Tinkoff Invest API (REST) для получения real-time данных."""
from __future__ import annotations

import asyncio
import datetime
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

import aiohttp
import pandas as pd

logger = logging.getLogger(__name__)

BASE_URL = "https://invest-public-api.tinkoff.ru/rest"


@dataclass
class InstrumentInfo:
    """Информация об инструменте."""
    figi: str
    ticker: str
    name: str
    instrument_type: str  # share, future, bond


class TinkoffClient:
    """Асинхронный клиент для Tinkoff Invest REST API."""
    
    def __init__(self, token: str) -> None:
        self._token = token
        self._session: aiohttp.ClientSession | None = None
        self._candles_cache: dict[str, Any] = {}

    def _get_headers(self) -> dict[str, str]:
        """Получить заголовки для запроса."""
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "accept": "application/json",
        }

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Получить или создать сессию."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        """Закрыть сессию."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def _post(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        """Выполнить POST запрос."""
        session = await self._ensure_session()
        url = f"{BASE_URL}{endpoint}"
        
        async with session.post(url, json=data, headers=self._get_headers()) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.error(f"API error {resp.status}: {text}")
                return {}
            return await resp.json()

    def _parse_quotation(self, quotation: dict[str, Any]) -> float:
        """Преобразовать Quotation в float."""
        if not quotation:
            return 0.0
        units = int(quotation.get("units", 0))
        nano = int(quotation.get("nano", 0))
        return units + nano / 1e9

    async def get_all_shares(self, only_rub: bool = True) -> list[InstrumentInfo]:
        """Получить все акции."""
        data = await self._post(
            "/tinkoff.public.invest.api.contract.v1.InstrumentsService/Shares",
            {"instrumentStatus": "INSTRUMENT_STATUS_BASE"}
        )
        
        result: list[InstrumentInfo] = []
        for item in data.get("instruments", []):
            if only_rub and item.get("currency") != "rub":
                continue
            result.append(InstrumentInfo(
                figi=item.get("figi", ""),
                ticker=item.get("ticker", ""),
                name=item.get("name", ""),
                instrument_type="share"
            ))
        return result

    async def get_all_futures(
        self,
        exclude_stock_futures: bool = True,
        nearest_only: bool = True
    ) -> list[InstrumentInfo]:
        """Получить фьючерсы."""
        data = await self._post(
            "/tinkoff.public.invest.api.contract.v1.InstrumentsService/Futures",
            {"instrumentStatus": "INSTRUMENT_STATUS_BASE"}
        )
        
        futures_list: list[dict[str, Any]] = []
        for item in data.get("instruments", []):
            if exclude_stock_futures:
                asset_type = item.get("assetType", "")
                if "SECURITY" in str(asset_type).upper():
                    continue
            
            futures_list.append({
                "figi": item.get("figi", ""),
                "ticker": item.get("ticker", ""),
                "name": item.get("name", ""),
                "basic_asset": item.get("basicAsset", ""),
                "expiration": item.get("expirationDate", "9999-12-31")
            })
        
        if nearest_only:
            by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for f in futures_list:
                by_asset[f["basic_asset"]].append(f)
            
            result: list[InstrumentInfo] = []
            for contracts in by_asset.values():
                nearest = min(contracts, key=lambda x: x["expiration"])
                result.append(InstrumentInfo(
                    figi=nearest["figi"],
                    ticker=nearest["ticker"],
                    name=nearest["name"],
                    instrument_type="future"
                ))
            return result
        
        return [
            InstrumentInfo(
                figi=f["figi"],
                ticker=f["ticker"],
                name=f["name"],
                instrument_type="future"
            )
            for f in futures_list
        ]

    async def get_all_bonds(self) -> list[InstrumentInfo]:
        """Получить все облигации."""
        data = await self._post(
            "/tinkoff.public.invest.api.contract.v1.InstrumentsService/Bonds",
            {"instrumentStatus": "INSTRUMENT_STATUS_BASE"}
        )
        
        return [
            InstrumentInfo(
                figi=item.get("figi", ""),
                ticker=item.get("ticker", ""),
                name=item.get("name", ""),
                instrument_type="bond"
            )
            for item in data.get("instruments", [])
        ]

    async def get_candles_exchange(
        self,
        figi: str,
        interval_mins: int = 10,
        period_days: int = 10,
        max_retries: int = 3
    ) -> pd.DataFrame:
        """Получить биржевые свечи за последние N дней."""
        interval_map = {
            1: "CANDLE_INTERVAL_1_MIN",
            5: "CANDLE_INTERVAL_5_MIN",
            10: "CANDLE_INTERVAL_10_MIN",
            15: "CANDLE_INTERVAL_15_MIN",
            60: "CANDLE_INTERVAL_HOUR",
        }
        interval = interval_map.get(interval_mins, "CANDLE_INTERVAL_10_MIN")
        
        now = datetime.datetime.now(datetime.timezone.utc)
        start = now - datetime.timedelta(days=period_days)
        
        for attempt in range(max_retries):
            try:
                data = await self._post(
                    "/tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles",
                    {
                        "figi": figi,
                        "from": start.isoformat(),
                        "to": now.isoformat(),
                        "interval": interval,
                        "candleSourceType": "CANDLE_SOURCE_EXCHANGE"
                    }
                )
                
                candles_list: list[dict[str, Any]] = []
                for candle in data.get("candles", []):
                    time_str = candle.get("time", "")
                    if time_str:
                        candle_time = datetime.datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                        msk_time = candle_time + datetime.timedelta(hours=3)
                    else:
                        msk_time = datetime.datetime.now()
                    
                    candles_list.append({
                        "begin": msk_time.replace(tzinfo=None),
                        "open": self._parse_quotation(candle.get("open", {})),
                        "close": self._parse_quotation(candle.get("close", {})),
                        "high": self._parse_quotation(candle.get("high", {})),
                        "low": self._parse_quotation(candle.get("low", {})),
                        "volume": int(candle.get("volume", 0))
                    })
                
                df = pd.DataFrame(candles_list)
                if len(df) > 0:
                    df = df.sort_values("begin").reset_index(drop=True)
                return df
                
            except Exception as e:
                logger.warning(f"Error getting candles (attempt {attempt + 1}): {e}")
                await asyncio.sleep((attempt + 1) * 2)
        
        return pd.DataFrame()

    async def get_candles(
        self,
        figi: str,
        interval_mins: int = 10,
        period_days: int = 10,
        max_retries: int = 3
    ) -> pd.DataFrame:
        """Получить свечи (алиас для get_candles_exchange)."""
        return await self.get_candles_exchange(figi, interval_mins, period_days, max_retries)

    async def get_candles_today_only(
        self,
        figi: str,
        interval_mins: int = 10,
        max_retries: int = 3
    ) -> pd.DataFrame:
        """Получить только сегодняшние свечи."""
        interval_map = {
            1: "CANDLE_INTERVAL_1_MIN",
            5: "CANDLE_INTERVAL_5_MIN",
            10: "CANDLE_INTERVAL_10_MIN",
            15: "CANDLE_INTERVAL_15_MIN",
            60: "CANDLE_INTERVAL_HOUR",
        }
        interval = interval_map.get(interval_mins, "CANDLE_INTERVAL_10_MIN")
        
        now = datetime.datetime.now(datetime.timezone.utc)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        for attempt in range(max_retries):
            try:
                data = await self._post(
                    "/tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles",
                    {
                        "figi": figi,
                        "from": today_start.isoformat(),
                        "to": now.isoformat(),
                        "interval": interval,
                        "candleSourceType": "CANDLE_SOURCE_EXCHANGE"
                    }
                )
                
                candles_list: list[dict[str, Any]] = []
                for candle in data.get("candles", []):
                    time_str = candle.get("time", "")
                    if time_str:
                        candle_time = datetime.datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                        msk_time = candle_time + datetime.timedelta(hours=3)
                    else:
                        msk_time = datetime.datetime.now()
                    
                    candles_list.append({
                        "begin": msk_time.replace(tzinfo=None),
                        "open": self._parse_quotation(candle.get("open", {})),
                        "close": self._parse_quotation(candle.get("close", {})),
                        "high": self._parse_quotation(candle.get("high", {})),
                        "low": self._parse_quotation(candle.get("low", {})),
                        "volume": int(candle.get("volume", 0))
                    })
                
                return pd.DataFrame(candles_list)
                
            except Exception as e:
                logger.warning(f"Error getting today candles (attempt {attempt + 1}): {e}")
                await asyncio.sleep((attempt + 1) * 2)
        
        return pd.DataFrame()

    async def get_last_price(self, figi: str) -> float | None:
        """Получить последнюю цену."""
        data = await self._post(
            "/tinkoff.public.invest.api.contract.v1.MarketDataService/GetLastPrices",
            {"figi": [figi]}
        )
        
        prices = data.get("lastPrices", [])
        if prices:
            return self._parse_quotation(prices[0].get("price", {}))
        return None

    async def get_last_prices_batch(self, figis: list[str]) -> dict[str, float]:
        """Получить последние цены для списка инструментов."""
        data = await self._post(
            "/tinkoff.public.invest.api.contract.v1.MarketDataService/GetLastPrices",
            {"figi": figis}
        )
        
        result: dict[str, float] = {}
        for item in data.get("lastPrices", []):
            figi = item.get("figi", "")
            price = self._parse_quotation(item.get("price", {}))
            if figi and price > 0:
                result[figi] = price
        return result
