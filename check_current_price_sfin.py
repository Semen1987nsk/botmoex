import asyncio
from app.moex_client import MoexClient

async def get_sfin_realtime():
    client = MoexClient()
    await client.create_session()
    
    try:
        # Fetch 1-minute candles for "Current" price
        print("Запрашиваю минутные свечи по SFIN...")
        df = await client.get_candles('stock', 'shares', 'TQBR', 'SFIN', interval=1)
        
        if not df.empty:
            last_candle = df.iloc[-1]
            print(f"--- SFIN (Минутка) ---")
            print(f"Цена (Close): {last_candle['close']}")
            print(f"Время свечи: {last_candle['begin']}")
            print(f"Volume: {last_candle['volume']}")
        else:
            print("Не удалось получить данные.")
            
    finally:
        await client.close_session()

if __name__ == "__main__":
    asyncio.run(get_sfin_realtime())
