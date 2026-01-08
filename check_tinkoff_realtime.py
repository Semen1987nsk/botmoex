"""
Тест получения real-time цены через Tinkoff Invest API.
Для работы нужен токен (получить на https://www.tinkoff.ru/invest/settings/api/).
"""
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

TINKOFF_TOKEN = os.getenv("TINKOFF_TOKEN")

async def get_realtime_price():
    if not TINKOFF_TOKEN:
        print("❌ Ошибка: Не задан TINKOFF_TOKEN в .env файле!")
        print("   Получите токен здесь: https://www.tinkoff.ru/invest/settings/api/")
        return
    
    try:
        from tinkoff.invest import AsyncClient
        from tinkoff.invest.services import InstrumentsService
        
        async with AsyncClient(TINKOFF_TOKEN) as client:
            # 1. Find SFIN by ticker
            instruments = await client.instruments.find_instrument(query="SFIN")
            
            sfin_figi = None
            for inst in instruments.instruments:
                if inst.ticker == "SFIN":
                    sfin_figi = inst.figi
                    print(f"Найден инструмент: {inst.name} (FIGI: {inst.figi})")
                    break
            
            if not sfin_figi:
                print("SFIN не найден.")
                return
            
            # 2. Get last prices (real-time!)
            last_prices = await client.market_data.get_last_prices(figi=[sfin_figi])
            
            for lp in last_prices.last_prices:
                price = lp.price.units + lp.price.nano / 1e9
                print(f"--- SFIN (Real-time) ---")
                print(f"Цена: {price}")
                print(f"Время: {lp.time}")
                
    except ImportError as e:
        print(f"Ошибка импорта: {e}")
    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    asyncio.run(get_realtime_price())
