"""
Тест: Real-time цена SFIN + расчет канала линейной регрессии (4 STD)
"""
import asyncio
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

TINKOFF_TOKEN = os.getenv("TINKOFF_TOKEN")

async def test_sfin_channel():
    if not TINKOFF_TOKEN:
        print("❌ Ошибка: Не задан TINKOFF_TOKEN")
        return
    
    from tinkoff.invest import AsyncClient, CandleInterval
    import datetime
    
    async with AsyncClient(TINKOFF_TOKEN) as client:
        # 1. Find SFIN
        instruments = await client.instruments.find_instrument(query="SFIN")
        sfin_figi = None
        for inst in instruments.instruments:
            if inst.ticker == "SFIN":
                sfin_figi = inst.figi
                print(f"📌 Инструмент: {inst.name} (SFIN)")
                print(f"   FIGI: {inst.figi}")
                break
        
        if not sfin_figi:
            print("SFIN не найден")
            return
        
        # 2. Get REAL-TIME price
        last_prices = await client.market_data.get_last_prices(figi=[sfin_figi])
        current_price = None
        for lp in last_prices.last_prices:
            current_price = lp.price.units + lp.price.nano / 1e9
            print(f"\n💰 ТЕКУЩАЯ ЦЕНА (Real-time): {current_price}")
            print(f"   Время: {lp.time}")
        
        # 3. Fetch 10-min candles for regression
        now = datetime.datetime.now(datetime.timezone.utc)
        start = now - datetime.timedelta(days=5)
        
        candles_list = []
        async for candle in client.get_all_candles(
            figi=sfin_figi,
            from_=start,
            to=now,
            interval=CandleInterval.CANDLE_INTERVAL_10_MIN
        ):
            candles_list.append({
                'begin': candle.time,
                'close': candle.close.units + candle.close.nano / 1e9,
            })
        
        df = pd.DataFrame(candles_list)
        print(f"\n📊 Загружено {len(df)} свечей (10 мин)")
        
        if len(df) < 200:
            print(f"⚠️ Недостаточно данных для 200-периодной регрессии (есть {len(df)})")
            return
        
        # 4. Calculate Linear Regression Channel
        length = 200
        std_mult = 4.0
        
        df_subset = df.iloc[-length:].copy()
        x = np.arange(len(df_subset))
        y = df_subset['close'].values
        
        # Linear regression
        A = np.vstack([x, np.ones(len(x))]).T
        m, c = np.linalg.lstsq(A, y, rcond=None)[0]
        
        regression_line = m * x + c
        residuals = y - regression_line
        std_dev = np.std(residuals)
        
        upper_channel = regression_line[-1] + (std_dev * std_mult)
        lower_channel = regression_line[-1] - (std_dev * std_mult)
        regression_value = regression_line[-1]
        
        last_close = y[-1]
        last_time = df_subset['begin'].iloc[-1]
        
        print(f"\n📈 КАНАЛ ЛИНЕЙНОЙ РЕГРЕССИИ (200 свечей, 4 STD):")
        print(f"   Последняя свеча: {last_time}")
        print(f"   Close свечи: {last_close:.2f}")
        print(f"   ─────────────────────────────")
        print(f"   Верхняя граница (+4 STD): {upper_channel:.2f}")
        print(f"   Линия регрессии:          {regression_value:.2f}")
        print(f"   Нижняя граница (-4 STD):  {lower_channel:.2f}")
        print(f"   ─────────────────────────────")
        print(f"   STD: {std_dev:.2f}")
        print(f"   Наклон (slope): {m:.4f}")
        
        # 5. Check signal
        print(f"\n🎯 АНАЛИЗ:")
        if current_price:
            if current_price > upper_channel:
                print(f"   🔴 ПРОБОЙ ВВЕРХ! Цена {current_price:.2f} > Верхняя граница {upper_channel:.2f}")
            elif current_price < lower_channel:
                print(f"   🔴 ПРОБОЙ ВНИЗ! Цена {current_price:.2f} < Нижняя граница {lower_channel:.2f}")
            else:
                distance_to_upper = upper_channel - current_price
                distance_to_lower = current_price - lower_channel
                print(f"   🟢 Цена внутри канала")
                print(f"   До верхней границы: {distance_to_upper:.2f}")
                print(f"   До нижней границы: {distance_to_lower:.2f}")

if __name__ == "__main__":
    asyncio.run(test_sfin_channel())
