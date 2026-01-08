import pandas as pd
import numpy as np
import datetime
from app import analyzer

def test_breakout_detection():
    print("--- Тест математики (analyzer.py) ---")
    
    # 1. Генерируем идеальный тренд (y = x)
    # 200 свечей
    length = 200
    x = np.arange(length)
    # Цена растет линейно: 100, 101, 102...
    close_prices = 100 + x * 1.0 
    
    # Добавляем немного шума, чтобы STD не был равен 0 (иначе деление на ноль или странности с каналом)
    # Шум +/- 0.5
    noise = np.random.normal(0, 0.5, length)
    close_prices = close_prices + noise
    
    dates = [datetime.datetime.now() - datetime.timedelta(minutes=10 * (length - i)) for i in range(length)]
    
    df = pd.DataFrame({
        'begin': dates,
        'close': close_prices
    })
    
    print(f"1. Сгенерировали {length} свечей с линейным трендом.")
    
    # Считаем канал. STD должен быть около 0.5.
    # Граница 4 STD будет примерно +/- 2.0 от линии.
    result = analyzer.calculate_linreg_channel(df, length=200, std_dev_mult=4.0)
    print(f"   Результат на нормальных данных: {result['status']}")
    print(f"   STD: {result['std']:.4f}")
    
    # 2. Создаем ИСКУССТВЕННЫЙ ПРОБОЙ
    # Берем последнюю цену и увеличиваем её так, чтобы она вылетела за 4 STD.
    # Если STD ~0.5, 4 STD ~ 2.0.
    # Текущая цена по тренду ~ 100 + 199 = 299.
    # Сделаем цену 350 (огромный скачок).
    
    print("\n2. Добавляем пробойную свечу (цена улетает вверх)...")
    df.iloc[-1, df.columns.get_loc('close')] = df.iloc[-1]['close'] + 50 # +100 STD
    
    result_breakout = analyzer.calculate_linreg_channel(df, length=200, std_dev_mult=4.0)
    
    print(f"   Статус: {result_breakout['status']}")
    print(f"   Цена: {result_breakout['current_close']:.2f}")
    print(f"   Верхняя граница: {result_breakout['upper']:.2f}")
    
    if result_breakout['status'] == 'ABOVE_UPPER':
        print("\n✅ УСПЕХ: Пробой вверх успешно обнаружен!")
    else:
        print("\n❌ ОШИБКА: Пробой не обнаружен.")

if __name__ == "__main__":
    test_breakout_detection()
