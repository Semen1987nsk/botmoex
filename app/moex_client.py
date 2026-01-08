import aiohttp
import pandas as pd
import datetime
import asyncio
from . import config

BASE_URL = "https://iss.moex.com/iss"

# Московский часовой пояс
MSK_OFFSET = datetime.timedelta(hours=3)

class MoexClient:
    def __init__(self):
        self.session = None

    async def create_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def close_session(self):
        if self.session:
            await self.session.close()

    async def get_json(self, url, params=None):
        if self.session is None:
            await self.create_session()
        async with self.session.get(url, params=params) as resp:
            return await resp.json()

    async def get_shares_tickers(self):
        # TQBR - Т+ Акции и ДР
        url = f"{BASE_URL}/engines/stock/markets/shares/boards/TQBR/securities.json"
        data = await self.get_json(url)
        # Parse ISS format: {'securities': {'columns': [...], 'data': [...]}}
        cols = data['securities']['columns']
        rows = data['securities']['data']
        df = pd.DataFrame(rows, columns=cols)
        return df['SECID'].tolist()

    async def get_futures_tickers(self):
        # RFUD - Фьючерсы
        url = f"{BASE_URL}/engines/futures/markets/forts/boards/RFUD/securities.json"
        data = await self.get_json(url)
        cols = data['securities']['columns']
        rows = data['securities']['data']
        df = pd.DataFrame(rows, columns=cols)
        return df['SECID'].tolist()

    async def get_bonds_tickers(self):
        # TQOB - Т+ Облигации
        url = f"{BASE_URL}/engines/stock/markets/bonds/boards/TQOB/securities.json"
        data = await self.get_json(url)
        cols = data['securities']['columns']
        rows = data['securities']['data']
        df = pd.DataFrame(rows, columns=cols)
        return df['SECID'].tolist()

    async def get_candles(self, engine, market, board, symbol, interval=1, fetch_bars=500, days_back=None):
        """
        Скачивает свечи.
        interval: 1 (1 мин), 10 (10 мин), 60 (1 час), 24 (1 день)
        """
        url = f"{BASE_URL}/engines/{engine}/markets/{market}/boards/{board}/securities/{symbol}/candles.json"
        
        # Determine start date
        # MOEX default limit is 500.
        # If interval=1, 1 day = ~840 candles (14h * 60). So 1 day > 500 limit.
        # If interval=10, 1 day = ~84 candles. 500 candles = ~6 days.
        
        if days_back is None:
            # Default logic
            if interval == 1:
                # For 1 min, we likely just want recent data or need to handle paging. 
                # If we want 200 bars history: 200 mins = 3.5 hours.
                # Let's verify 'fetch_bars' intent. 
                # If we want JUST the latest price, we should ask for very recent.
                days_back = 0 # Today
            elif interval == 10:
                days_back = 4 # Enough for 200 bars (needs ~2.5 days)
            else:
                days_back = 7
        
        # If days_back=0, use today's date
        # If we need to go back specific hours, datetime math is needed.
        # MOEX `from` param expects YYYY-MM-DD usually, or full datetime? 
        # Documentation says "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS".
        
        start_dt = datetime.datetime.now() - datetime.timedelta(days=days_back)
        start_date_str = start_dt.strftime('%Y-%m-%d')
        
        params = {
            'interval': interval,
            'from': start_date_str
        }
        
        try:
            data = await self.get_json(url, params=params)
            cols = data['candles']['columns']
            rows = data['candles']['data']
            df = pd.DataFrame(rows, columns=cols)
            
            # If we got data, format it
            if not df.empty:
                df['begin'] = pd.to_datetime(df['begin'])
                df['close'] = pd.to_numeric(df['close'])
                df['open'] = pd.to_numeric(df['open'])
                df['high'] = pd.to_numeric(df['high'])
                df['low'] = pd.to_numeric(df['low'])
            
            return df
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            return pd.DataFrame()

    async def get_candles_until_today(self, engine, market, board, symbol, interval=10, days_back=10):
        """
        Получить свечи до начала сегодняшнего дня (для гибридной загрузки).
        Возвращает данные в формате совместимом с Tinkoff (begin, open, high, low, close, volume).
        """
        url = f"{BASE_URL}/engines/{engine}/markets/{market}/boards/{board}/securities/{symbol}/candles.json"
        
        start_dt = datetime.datetime.now() - datetime.timedelta(days=days_back)
        start_date_str = start_dt.strftime('%Y-%m-%d')
        
        # Конец — вчера 23:59 (чтобы не пересекаться с сегодняшними данными Tinkoff)
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        end_date_str = yesterday.strftime('%Y-%m-%d')
        
        params = {
            'interval': interval,
            'from': start_date_str,
            'till': end_date_str
        }
        
        try:
            data = await self.get_json(url, params=params)
            cols = data['candles']['columns']
            rows = data['candles']['data']
            df = pd.DataFrame(rows, columns=cols)
            
            if not df.empty:
                df['begin'] = pd.to_datetime(df['begin'])
                df['close'] = pd.to_numeric(df['close'])
                df['open'] = pd.to_numeric(df['open'])
                df['high'] = pd.to_numeric(df['high'])
                df['low'] = pd.to_numeric(df['low'])
                # Переименуем volume если нужно
                if 'volume' not in df.columns and 'value' in df.columns:
                    df['volume'] = df['value']
            
            return df
        except Exception as e:
            print(f"Error fetching history for {symbol}: {e}")
            return pd.DataFrame()

