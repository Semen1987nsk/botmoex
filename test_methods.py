"""
Сравнение разных методов расчета канала линейной регрессии
"""
import asyncio
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

TINKOFF_TOKEN = os.getenv("TINKOFF_TOKEN")

async def test_different_methods():
    from tinkoff.invest import AsyncClient, CandleInterval
    import datetime
    
    async with AsyncClient(TINKOFF_TOKEN) as client:
        # Find SFIN
        instruments = await client.instruments.find_instrument(query="SFIN")
        sfin_figi = None
        for inst in instruments.instruments:
            if inst.ticker == "SFIN":
                sfin_figi = inst.figi
                break
        
        # Get candles
        now = datetime.datetime.now(datetime.timezone.utc)
        start = now - datetime.timedelta(days=7)
        
        candles_list = []
        async for candle in client.get_all_candles(
            figi=sfin_figi,
            from_=start,
            to=now,
            interval=CandleInterval.CANDLE_INTERVAL_10_MIN
        ):
            candles_list.append({
                'begin': candle.time,
                'open': candle.open.units + candle.open.nano / 1e9,
                'high': candle.high.units + candle.high.nano / 1e9,
                'low': candle.low.units + candle.low.nano / 1e9,
                'close': candle.close.units + candle.close.nano / 1e9,
            })
        
        df = pd.DataFrame(candles_list)
        print(f"Загружено {len(df)} свечей")
        print(f"Последняя свеча: {df['begin'].iloc[-1]}, Close: {df['close'].iloc[-1]}")
        
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
        
        print(f"\n{'='*60}")
        print("СРАВНЕНИЕ МЕТОДОВ РАСЧЕТА:")
        print(f"{'='*60}")
        
        # Method 1: STD от остатков (population, ddof=0)
        std1 = np.std(residuals, ddof=0)
        upper1 = regression_line[-1] + std1 * std_mult
        lower1 = regression_line[-1] - std1 * std_mult
        print(f"\n1️⃣ STD остатков (population, ddof=0):")
        print(f"   STD: {std1:.2f}")
        print(f"   Верх: {upper1:.2f} | Низ: {lower1:.2f}")
        
        # Method 2: STD от остатков (sample, ddof=1)
        std2 = np.std(residuals, ddof=1)
        upper2 = regression_line[-1] + std2 * std_mult
        lower2 = regression_line[-1] - std2 * std_mult
        print(f"\n2️⃣ STD остатков (sample, ddof=1):")
        print(f"   STD: {std2:.2f}")
        print(f"   Верх: {upper2:.2f} | Низ: {lower2:.2f}")
        
        # Method 3: STD от цен закрытия (не от остатков)
        std3 = np.std(y, ddof=0)
        mean_price = np.mean(y)
        upper3 = mean_price + std3 * std_mult
        lower3 = mean_price - std3 * std_mult
        print(f"\n3️⃣ STD от цен (не регрессия, а среднее ± 4STD):")
        print(f"   STD: {std3:.2f}")
        print(f"   Mean: {mean_price:.2f}")
        print(f"   Верх: {upper3:.2f} | Низ: {lower3:.2f}")
        
        # Method 4: STD от цен + линия регрессии на конце
        upper4 = regression_line[-1] + std3 * std_mult
        lower4 = regression_line[-1] - std3 * std_mult
        print(f"\n4️⃣ Линия регрессии + STD от цен:")
        print(f"   STD: {std3:.2f}")
        print(f"   Reg line: {regression_line[-1]:.2f}")
        print(f"   Верх: {upper4:.2f} | Низ: {lower4:.2f}")
        
        # Method 5: Pearson's method - Standard Error
        n = len(y)
        se = std2 * np.sqrt(1 + 1/n + (n-1)**2 / (n * np.sum((x - np.mean(x))**2)))
        upper5 = regression_line[-1] + se * std_mult
        lower5 = regression_line[-1] - se * std_mult
        print(f"\n5️⃣ Standard Error метод:")
        print(f"   SE: {se:.2f}")
        print(f"   Верх: {upper5:.2f} | Низ: {lower5:.2f}")
        
        print(f"\n{'='*60}")
        print(f"ВАШИ ЗНАЧЕНИЯ: Верх: 858.4 | Низ: 793.7")
        print(f"Ширина вашего канала: {858.4 - 793.7:.1f}")
        print(f"Ваш STD (ширина/8): {(858.4 - 793.7)/8:.2f}")
        print(f"{'='*60}")

if __name__ == "__main__":
    asyncio.run(test_different_methods())
