import asyncio
from app.moex_client import MoexClient

async def get_sfi_price():
    client = MoexClient()
    await client.create_session()
    
    # SFIN is the ticker for SFI (ЭсЭфАй)
    # TQBR board for shares
    try:
        # Fetching candles to get the last price
        df = await client.get_candles('stock', 'shares', 'TQBR', 'SFIN', interval=10)
        
        if not df.empty:
            last_candle = df.iloc[-1]
            print(f"Последняя цена SFIN (ЭсЭфАй): {last_candle['close']}")
            print(f"Время: {last_candle['begin']}")
        else:
            print("Не удалось получить данные по SFIN.")
            
    finally:
        await client.close_session()

if __name__ == "__main__":
    asyncio.run(get_sfi_price())
