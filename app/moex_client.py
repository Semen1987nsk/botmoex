import aiohttp
import pandas as pd
import datetime
import asyncio
import logging
from . import config

logger = logging.getLogger(__name__)

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
        Скачивает свечи с пагинацией.
        interval: 1 (1 мин), 10 (10 мин), 60 (1 час), 24 (1 день)
        """
        url = f"{BASE_URL}/engines/{engine}/markets/{market}/boards/{board}/securities/{symbol}/candles.json"
        
        if days_back is None:
            if interval == 1:
                days_back = 1
            elif interval == 10:
                days_back = 15
            elif interval == 24:
                days_back = 500  # ~1.5 года для дневок
            else:
                days_back = 30
        
        start_dt = datetime.datetime.now() - datetime.timedelta(days=days_back)
        start_date_str = start_dt.strftime('%Y-%m-%d')
        
        all_rows = []
        cols = []
        start_offset = 0
        
        try:
            # Пагинация для получения всех свечей
            while True:
                params = {
                    'interval': interval,
                    'from': start_date_str,
                    'start': start_offset
                }
                
                data = await self.get_json(url, params=params)
                cols = data['candles']['columns']
                rows = data['candles']['data']
                
                if not rows:
                    break
                
                all_rows.extend(rows)
                
                # MOEX возвращает максимум 500 свечей за запрос
                if len(rows) < 500:
                    break
                
                start_offset += len(rows)
            
            if not all_rows:
                return pd.DataFrame()
            
            df = pd.DataFrame(all_rows, columns=cols)
            df['begin'] = pd.to_datetime(df['begin'])
            df['close'] = pd.to_numeric(df['close'])
            df['open'] = pd.to_numeric(df['open'])
            df['high'] = pd.to_numeric(df['high'])
            df['low'] = pd.to_numeric(df['low'])
            if 'volume' not in df.columns and 'value' in df.columns:
                df['volume'] = df['value']
            
            return df
        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            return pd.DataFrame()

    async def get_candles_until_today(self, engine, market, board, symbol, interval=10, days_back=10):
        """
        Получить свечи до начала сегодняшнего дня (для гибридной загрузки).
        Использует пагинацию для получения всех свечей (MOEX лимит 500 за запрос).
        """
        url = f"{BASE_URL}/engines/{engine}/markets/{market}/boards/{board}/securities/{symbol}/candles.json"
        
        start_dt = datetime.datetime.now() - datetime.timedelta(days=days_back)
        start_date_str = start_dt.strftime('%Y-%m-%d')
        
        # Конец — вчера 23:59 (чтобы не пересекаться с сегодняшними данными Tinkoff)
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        end_date_str = yesterday.strftime('%Y-%m-%d')
        
        all_rows = []
        start_offset = 0
        
        while True:
            params = {
                'interval': interval,
                'from': start_date_str,
                'till': end_date_str,
                'start': start_offset
            }
            
            try:
                data = await self.get_json(url, params=params)
                cols = data['candles']['columns']
                rows = data['candles']['data']
                
                if not rows:
                    break
                    
                all_rows.extend(rows)
                
                # MOEX возвращает максимум 500 свечей за запрос
                if len(rows) < 500:
                    break
                    
                start_offset += len(rows)
                
            except Exception as e:
                logger.error(f"Error fetching history for {symbol}: {e}")
                break
        
        if not all_rows:
            return pd.DataFrame()
            
        df = pd.DataFrame(all_rows, columns=cols)
        df['begin'] = pd.to_datetime(df['begin'])
        df['close'] = pd.to_numeric(df['close'])
        df['open'] = pd.to_numeric(df['open'])
        df['high'] = pd.to_numeric(df['high'])
        df['low'] = pd.to_numeric(df['low'])
        if 'volume' not in df.columns and 'value' in df.columns:
            df['volume'] = df['value']
        
        return df

