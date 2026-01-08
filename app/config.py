import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv("BOT_TOKEN")
TINKOFF_TOKEN = os.getenv("TINKOFF_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# Параметры стратегии
# 10 минутные свечи
TIMEFRAME = 10 
REGRESSION_LENGTH = 200
STD_DEV_MULTIPLIER = 4.0

# Торговая сессия MOEX (МСК)
# Утренняя: 06:50-09:50, Основная: 10:00-18:50, Вечерняя: 19:00-23:50
TRADING_START_HOUR = 7   # 06:50 МСК (округляем до 7)
TRADING_END_HOUR = 24    # 23:50 МСК (до полуночи)
