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
