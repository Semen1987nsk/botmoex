#!/usr/bin/env python3
"""
Проверка любого тикера - расчёт канала линейной регрессии.
Использует ту же логику, что и основной бот (bot.py).
"""
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os
import sys
sys.path.insert(0, '/workspaces/botmoex')

load_dotenv()

# Параметры (как в config.py)
TIMEFRAME = 10
REGRESSION_LENGTH = 300
STD_DEV_MULTIPLIER = 3.5


async def check_ticker(ticker='AFLT', instr_type='share'):
    """
    Проверка тикера с расчётом канала.
    instr_type: 'share' или 'future'
    """
    from app.moex_client import MoexClient
    from app.tinkoff_client import TinkoffClient
    
    # Время в МСК
    now_utc = datetime.now(timezone.utc)
    now_msk = now_utc + timedelta(hours=3)
    
    print(f'=== {ticker}: {TIMEFRAME}м × {REGRESSION_LENGTH} × {STD_DEV_MULTIPLIER}σ ===')
    print(f'Время: {now_msk.strftime("%d.%m.%Y %H:%M")} МСК')
    print()
    
    token = os.getenv('TINKOFF_TOKEN')
    tinkoff = TinkoffClient(token)
    moex = MoexClient()
    
    # Определяем параметры MOEX
    if instr_type == 'future':
        engine, market, board = 'futures', 'forts', 'RFUD'
        instruments = await tinkoff.get_all_futures(exclude_stock_futures=False, nearest_only=False)
    else:
        engine, market, board = 'stock', 'shares', 'TQBR'
        instruments = await tinkoff.get_all_shares()
    
    # Найти figi
    found = next((i for i in instruments if i['ticker'] == ticker), None)
    if not found:
        print(f'❌ Тикер {ticker} не найден!')
        await moex.close_session()
        return
    
    figi = found['figi']
    name = found['name']
    print(f'📌 {ticker} - {name}')
    print(f'   FIGI: {figi}')
    
    # Текущая цена (real-time)
    price = await tinkoff.get_last_price(figi)
    print(f'\n💰 Текущая цена: {price:.2f}')
    
    # === ГИБРИДНАЯ ЗАГРУЗКА (как в bot.py) ===
    
    # 1. MOEX: история до вчера
    df_moex = await moex.get_candles_until_today(
        engine, market, board, ticker, 
        interval=TIMEFRAME, days_back=15
    )
    
    # 2. Tinkoff: только сегодня
    df_today = await tinkoff.get_candles_today_only(figi, interval_mins=TIMEFRAME)
    
    print(f'\n📊 MOEX история: {len(df_moex)} свечей')
    print(f'📊 Tinkoff сегодня: {len(df_today)} свечей')
    
    # 3. Объединяем
    if len(df_moex) > 0 and len(df_today) > 0:
        df = pd.concat([df_moex, df_today], ignore_index=True)
        df = df.drop_duplicates(subset=['begin']).sort_values('begin').reset_index(drop=True)
        source_info = f"MOEX + Tinkoff"
    elif len(df_today) > 0:
        df = df_today
        source_info = "Tinkoff"
    else:
        df = df_moex
        source_info = "MOEX"
    
    print(f'📊 Всего: {len(df)} свечей ({source_info})')
    
    await moex.close_session()
    
    if len(df) < REGRESSION_LENGTH:
        print(f'\n❌ Недостаточно свечей (нужно {REGRESSION_LENGTH}, есть {len(df)})')
        return
    
    # Берём последние N свечей
    df = df.tail(REGRESSION_LENGTH).reset_index(drop=True)
    closes = df['close'].values
    
    print(f'\n📅 Диапазон данных:')
    print(f'   Первая:    {df.iloc[0]["begin"]} | close={df.iloc[0]["close"]:.2f}')
    print(f'   Последняя: {df.iloc[-1]["begin"]} | close={df.iloc[-1]["close"]:.2f}')
    
    # === РАСЧЁТ КАНАЛА (как в analyzer.py) ===
    x = np.arange(REGRESSION_LENGTH)
    A = np.vstack([x, np.ones(REGRESSION_LENGTH)]).T
    m, c = np.linalg.lstsq(A, closes, rcond=None)[0]
    
    regression = m * (REGRESSION_LENGTH - 1) + c
    predicted = m * x + c
    residuals = closes - predicted
    std = np.std(residuals, ddof=0)  # population STD, как в TradingView
    
    upper = regression + std * STD_DEV_MULTIPLIER
    lower = regression - std * STD_DEV_MULTIPLIER
    
    # EMA 50
    ema50 = pd.Series(closes).ewm(span=50, adjust=False).mean().iloc[-1]
    
    print(f'\n=== КАНАЛ ({STD_DEV_MULTIPLIER}σ) ===')
    print(f'⬆️ Upper:      {upper:.2f}')
    print(f'📊 Regression: {regression:.2f}')
    print(f'⬇️ Lower:      {lower:.2f}')
    print(f'📏 STD:        {std:.4f}')
    print(f'📐 Slope:      {m:.6f} ({"📈 рост" if m > 0 else "📉 падение"})')
    print(f'📐 EMA50:      {ema50:.2f}')
    
    # === АНАЛИЗ ===
    print(f'\n=== АНАЛИЗ ===')
    
    # Позиция относительно EMA
    ema_status = "🟢 выше" if price > ema50 else "🔴 ниже"
    print(f'EMA50: {ema_status} ({price:.2f} vs {ema50:.2f})')
    
    # Позиция относительно канала
    if price > upper:
        deviation = (price - regression) / regression * 100
        print(f'🔺 ВЫШЕ +{STD_DEV_MULTIPLIER}σ!')
        print(f'   Цена {price:.2f} > Upper {upper:.2f}')
        print(f'   Отклонение от регрессии: {deviation:+.2f}%')
    elif price < lower:
        deviation = (price - regression) / regression * 100
        print(f'🔻 НИЖЕ -{STD_DEV_MULTIPLIER}σ!')
        print(f'   Цена {price:.2f} < Lower {lower:.2f}')
        print(f'   Отклонение от регрессии: {deviation:+.2f}%')
    else:
        print(f'✅ Внутри канала')
        print(f'   До верха: {upper - price:.2f} ({(upper - price) / std:.1f}σ)')
        print(f'   До низа:  {price - lower:.2f} ({(price - lower) / std:.1f}σ)')
    
    # Последние 10 свечей
    print(f'\n=== Последние 10 свечей ===')
    for i in range(-10, 0):
        row = df.iloc[i]
        print(f'   {row["begin"]} | close={row["close"]:.2f}')


if __name__ == '__main__':
    import sys
    
    # Можно передать тикер как аргумент: python check_aflt.py SBER
    ticker = sys.argv[1] if len(sys.argv) > 1 else 'AFLT'
    instr_type = sys.argv[2] if len(sys.argv) > 2 else 'share'
    
    asyncio.run(check_ticker(ticker, instr_type))
