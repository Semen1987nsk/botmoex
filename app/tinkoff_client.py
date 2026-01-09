import asyncio
import pandas as pd
import datetime
from tinkoff.invest import AsyncClient, CandleInterval
from tinkoff.invest.schemas import CandleSource

INTERVAL_MAPPING = {
    1: CandleInterval.CANDLE_INTERVAL_1_MIN,
    5: CandleInterval.CANDLE_INTERVAL_5_MIN,
    10: CandleInterval.CANDLE_INTERVAL_10_MIN,
    15: CandleInterval.CANDLE_INTERVAL_15_MIN,
    60: CandleInterval.CANDLE_INTERVAL_HOUR
}


class TinkoffClient:
    def __init__(self, token):
        self.token = token
        self._candles_cache = {}  # Кэш: {figi: {'df': DataFrame, 'last_time': datetime}}

    async def get_all_shares(self, only_rub=True):
        """Получить все акции. only_rub=True - только рублёвые (MOEX)."""
        async with AsyncClient(self.token) as client:
            instruments = await client.instruments.shares(instrument_status=1)
            result = []
            for i in instruments.instruments:
                if only_rub and i.currency != 'rub':
                    continue
                result.append({
                    'figi': i.figi, 
                    'ticker': i.ticker, 
                    'name': i.name, 
                    'type': 'share'
                })
            return result

    async def get_all_futures(self, exclude_stock_futures=True, nearest_only=True):
        """
        Получить фьючерсы.
        exclude_stock_futures=True - убирает фьючерсы на акции
        nearest_only=True - оставляет только ближайший контракт по каждому базовому активу
        """
        async with AsyncClient(self.token) as client:
            instruments = await client.instruments.futures(instrument_status=1)
            
            # Собираем все подходящие фьючерсы
            futures_list = []
            for i in instruments.instruments:
                # Фильтр по типу актива
                if exclude_stock_futures:
                    asset_type = str(i.asset_type) if hasattr(i.asset_type, 'name') else i.asset_type
                    if 'SECURITY' in str(asset_type).upper():
                        continue
                
                futures_list.append({
                    'figi': i.figi,
                    'ticker': i.ticker,
                    'name': i.name,
                    'type': 'future',
                    'basic_asset': i.basic_asset,
                    'expiration': i.expiration_date
                })
            
            # Если нужны только ближайшие - группируем по базовому активу
            if nearest_only:
                from collections import defaultdict
                by_asset = defaultdict(list)
                for f in futures_list:
                    by_asset[f['basic_asset']].append(f)
                
                # Выбираем ближайший по дате экспирации
                result = []
                for asset, contracts in by_asset.items():
                    nearest = min(contracts, key=lambda x: x['expiration'])
                    result.append({
                        'figi': nearest['figi'],
                        'ticker': nearest['ticker'],
                        'name': nearest['name'],
                        'type': 'future'
                    })
                return result
            
            return [{'figi': f['figi'], 'ticker': f['ticker'], 'name': f['name'], 'type': 'future'} for f in futures_list]

    async def get_all_bonds(self):
        """Получить все облигации."""
        async with AsyncClient(self.token) as client:
            instruments = await client.instruments.bonds(instrument_status=1)
            return [
                {'figi': i.figi, 'ticker': i.ticker, 'name': i.name, 'type': 'bond'} 
                for i in instruments.instruments
            ]

    async def get_candles_exchange(self, figi, interval_mins=10, period_days=10, max_retries=3):
        """
        Получить биржевые свечи (EXCHANGE) за последние N дней.
        Время в МСК. Это основной метод для расчёта каналов.
        """
        tf = INTERVAL_MAPPING.get(interval_mins, CandleInterval.CANDLE_INTERVAL_10_MIN)
        now = datetime.datetime.now(datetime.timezone.utc)
        start = now - datetime.timedelta(days=period_days)
        
        for attempt in range(max_retries):
            try:
                candles_list = []
                async with AsyncClient(self.token) as client:
                    async for candle in client.get_all_candles(
                        figi=figi,
                        from_=start,
                        to=now,
                        interval=tf,
                        candle_source_type=CandleSource.CANDLE_SOURCE_EXCHANGE
                    ):
                        # Конвертируем UTC -> МСК
                        msk_time = candle.time + datetime.timedelta(hours=3)
                        candles_list.append({
                            'begin': msk_time.replace(tzinfo=None),
                            'open': candle.open.units + candle.open.nano / 1e9,
                            'close': candle.close.units + candle.close.nano / 1e9,
                            'high': candle.high.units + candle.high.nano / 1e9,
                            'low': candle.low.units + candle.low.nano / 1e9,
                            'volume': candle.volume
                        })
                
                df = pd.DataFrame(candles_list)
                if len(df) > 0:
                    df = df.sort_values('begin').reset_index(drop=True)
                return df
                
            except Exception as e:
                if 'RESOURCE_EXHAUSTED' in str(e):
                    wait_time = (attempt + 1) * 5
                    await asyncio.sleep(wait_time)
                else:
                    raise
        
        return pd.DataFrame()

    async def get_candles(self, figi, interval_mins=10, period_days=10, max_retries=3):
        """Получить свечи за последние N дней с кэшированием и retry."""
        tf = INTERVAL_MAPPING.get(interval_mins, CandleInterval.CANDLE_INTERVAL_10_MIN)
        now = datetime.datetime.now(datetime.timezone.utc)
        
        cache_key = f"{figi}_{interval_mins}"
        cached = self._candles_cache.get(cache_key)
        
        # Если есть кэш — загружаем только новые свечи
        if cached and cached['df'] is not None and len(cached['df']) > 0:
            last_time = cached['last_time']
            # Загружаем с последней свечи + небольшой запас
            start = last_time - datetime.timedelta(minutes=interval_mins * 2)
        else:
            start = now - datetime.timedelta(days=period_days)
        
        for attempt in range(max_retries):
            try:
                candles_list = []
                async with AsyncClient(self.token) as client:
                    async for candle in client.get_all_candles(
                        figi=figi,
                        from_=start,
                        to=now,
                        interval=tf,
                        candle_source_type=CandleSource.CANDLE_SOURCE_EXCHANGE
                    ):
                        candles_list.append({
                            'begin': candle.time,
                            'open': candle.open.units + candle.open.nano / 1e9,
                            'close': candle.close.units + candle.close.nano / 1e9,
                            'high': candle.high.units + candle.high.nano / 1e9,
                            'low': candle.low.units + candle.low.nano / 1e9,
                            'volume': candle.volume
                        })
                
                new_df = pd.DataFrame(candles_list)
                
                # Объединяем с кэшем
                if cached and cached['df'] is not None and len(cached['df']) > 0 and len(new_df) > 0:
                    # Объединяем старые + новые, убираем дубликаты
                    combined = pd.concat([cached['df'], new_df])
                    combined = combined.drop_duplicates(subset=['begin'], keep='last')
                    combined = combined.sort_values('begin').reset_index(drop=True)
                    # Оставляем только последние N дней
                    cutoff = now - datetime.timedelta(days=period_days)
                    combined = combined[combined['begin'] >= cutoff]
                    result_df = combined
                else:
                    result_df = new_df
                
                # Обновляем кэш
                if len(result_df) > 0:
                    self._candles_cache[cache_key] = {
                        'df': result_df,
                        'last_time': result_df['begin'].max()
                    }
                
                return result_df
                
            except Exception as e:
                if 'RESOURCE_EXHAUSTED' in str(e):
                    wait_time = (attempt + 1) * 5  # 5, 10, 15 секунд (более длинное ожидание)
                    await asyncio.sleep(wait_time)
                else:
                    raise
        
        # При неудаче возвращаем кэш если есть
        if cached and cached['df'] is not None:
            return cached['df']
        return pd.DataFrame()

    async def get_candles_today_only(self, figi, interval_mins=10, max_retries=3):
        """Получить только сегодняшние свечи через Tinkoff (для гибридной загрузки)."""
        tf = INTERVAL_MAPPING.get(interval_mins, CandleInterval.CANDLE_INTERVAL_10_MIN)
        now = datetime.datetime.now(datetime.timezone.utc)
        # Начало сегодняшнего дня в UTC
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        for attempt in range(max_retries):
            try:
                candles_list = []
                async with AsyncClient(self.token) as client:
                    async for candle in client.get_all_candles(
                        figi=figi,
                        from_=today_start,
                        to=now,
                        interval=tf,
                        candle_source_type=CandleSource.CANDLE_SOURCE_EXCHANGE
                    ):
                        # Конвертируем UTC -> МСК (для совместимости с MOEX)
                        msk_time = candle.time + datetime.timedelta(hours=3)
                        candles_list.append({
                            'begin': msk_time.replace(tzinfo=None),  # Убираем timezone для совместимости
                            'open': candle.open.units + candle.open.nano / 1e9,
                            'close': candle.close.units + candle.close.nano / 1e9,
                            'high': candle.high.units + candle.high.nano / 1e9,
                            'low': candle.low.units + candle.low.nano / 1e9,
                            'volume': candle.volume
                        })
                        
                return pd.DataFrame(candles_list)
                
            except Exception as e:
                if 'RESOURCE_EXHAUSTED' in str(e):
                    wait_time = (attempt + 1) * 5
                    await asyncio.sleep(wait_time)
                else:
                    raise
        
        return pd.DataFrame()

    async def get_last_price(self, figi):
        """Получить последнюю цену (real-time)."""
        async with AsyncClient(self.token) as client:
            resp = await client.market_data.get_last_prices(figi=[figi])
            if resp.last_prices:
                lp = resp.last_prices[0]
                return lp.price.units + lp.price.nano / 1e9
        return None

    async def get_last_prices_batch(self, figis):
        """Получить последние цены для списка инструментов (до 100)."""
        result = {}
        async with AsyncClient(self.token) as client:
            resp = await client.market_data.get_last_prices(figi=figis)
            for lp in resp.last_prices:
                price = lp.price.units + lp.price.nano / 1e9
                result[lp.figi] = price
        return result
