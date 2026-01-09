import pandas as pd
import numpy as np
from . import config

def resample_candles(df, timeframe='15min'):
    if df.empty:
        return df
    
    # Ensure datetime index
    df = df.set_index('begin')
    
    # Resample logic
    ohlc_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    
    # If columns exist
    present_cols = {k: v for k, v in ohlc_dict.items() if k in df.columns}
    
    resampled = df.resample(timeframe).agg(present_cols)
    resampled = resampled.dropna()
    return resampled.reset_index()


def calculate_ema(closes, period=50):
    """Расчёт EMA."""
    if len(closes) < period:
        return None
    ema = pd.Series(closes).ewm(span=period, adjust=False).mean()
    return ema.iloc[-1]


def calculate_linreg_realtime(closes_299, current_price, std_dev_mult=3.5):
    """
    Realtime расчёт регрессии (как в Finam).
    Берём 199 закрытых свечей + текущую цену = 200 точек.
    Пересчитываем регрессию полностью.
    """
    # Добавляем текущую цену как 200-ю точку
    y = np.append(closes_299, current_price)
    x = np.arange(len(y))
    
    # Линейная регрессия
    A = np.vstack([x, np.ones(len(x))]).T
    m, c = np.linalg.lstsq(A, y, rcond=None)[0]
    
    # Линия регрессии
    regression_line = m * x + c
    
    # STD от остатков
    residuals = y - regression_line
    std_dev = np.std(residuals, ddof=0)
    
    # Значения на последней точке
    regression = regression_line[-1]
    upper = regression + (std_dev * std_dev_mult)
    lower = regression - (std_dev * std_dev_mult)
    
    return {
        'upper': upper,
        'lower': lower,
        'regression': regression,
        'std': std_dev,
        'slope': m
    }


def calculate_linreg_channel(df, length=200, std_dev_mult=3.5):
    """
    Расчет канала линейной регрессии (стиль TradingView/Финам):
    - Линейная регрессия по Close
    - STD от остатков (population, ddof=0)
    - Множитель применяется к STD
    """
    if len(df) < length:
        return None
    
    # Take last N candles
    df_subset = df.iloc[-length:].copy()
    
    # X axis is just 0 to N-1
    x = np.arange(len(df_subset))
    y = df_subset['close'].values
    
    # Linear Regression: y = mx + c
    A = np.vstack([x, np.ones(len(x))]).T
    m, c = np.linalg.lstsq(A, y, rcond=None)[0]
    
    # Calculate regression line values
    regression_line = m * x + c
    
    # STD от остатков (population stdev, как в TradingView)
    residuals = y - regression_line
    std_dev = np.std(residuals, ddof=0)
    
    upper_channel = regression_line[-1] + (std_dev * std_dev_mult)
    lower_channel = regression_line[-1] - (std_dev * std_dev_mult)
    
    current_close = y[-1]
    
    # EMA 50
    ema50 = calculate_ema(y, 50)
    
    # Check intersection/breakout
    status = "INSIDE"
    
    if current_close > upper_channel:
        status = "ABOVE_UPPER"
    elif current_close < lower_channel:
        status = "BELOW_LOWER"
        
    return {
        'status': status,
        'current_close': current_close,
        'upper': upper_channel,
        'lower': lower_channel,
        'regression': regression_line[-1],
        'std': std_dev,
        'slope': m,
        'ema50': ema50,
        'last_candle_time': df_subset['begin'].iloc[-1]
    }
