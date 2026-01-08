import asyncio
import logging
import time
import pandas as pd
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message

from app.config import TINKOFF_TOKEN, TELEGRAM_TOKEN, TIMEFRAME, REGRESSION_LENGTH, STD_DEV_MULTIPLIER
from app.tinkoff_client import TinkoffClient
from app.moex_client import MoexClient
from app.analyzer import calculate_linreg_channel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = Router()
bot: Bot = None
dp: Dispatcher = None

# Глобальные данные
monitoring = False
subscribers = set()  # Чат-id для уведомлений

# ticker -> {figi, name, type, board, engine, market, upper, lower, last_signal_type}
instruments = {}

# figi -> ticker (для быстрого поиска)
figi_to_ticker = {}

tinkoff_client = None
moex_client = None


@router.message(Command("start"))
async def cmd_start(message: Message):
    subscribers.add(message.chat.id)
    await message.answer(
        "🤖 MOEX Breakout Monitor\n\n"
        "Мониторинг пробоев линейных регрессионных каналов (4 STD) на 10-минутках.\n\n"
        "Команды:\n"
        "/scan - Запустить мониторинг\n"
        "/stop - Остановить мониторинг\n"
        "/status - Статус бота\n"
        "/check - Проверить OZON, GAZP, SFIN\n"
        "/ticker SBER - Проверить любой тикер"
    )


@router.message(Command("scan"))
async def cmd_scan(message: Message):
    global monitoring
    if monitoring:
        await message.answer("⚠️ Мониторинг уже запущен!")
        return
    
    subscribers.add(message.chat.id)
    await message.answer("⏳ Загрузка инструментов...")
    
    # Загружаем инструменты
    await load_instruments()
    
    if not instruments:
        await message.answer("❌ Не удалось загрузить инструменты!")
        return
    
    await message.answer(f"✅ Загружено {len(instruments)} инструментов\n⏳ Расчёт каналов регрессии...")
    
    # Считаем каналы для всех инструментов
    await update_all_channels()
    
    active_count = sum(1 for i in instruments.values() if i.get('upper'))
    await message.answer(f"✅ Рассчитано каналов: {active_count}\n⏳ Проверка текущих позиций...")
    
    # Получаем текущие экстремальные позиции
    extremes_up, extremes_down = await get_current_extremes()
    extremes_msg = await format_extremes_message(extremes_up, extremes_down)
    await message.answer(extremes_msg, parse_mode='HTML')
    
    await message.answer(
        f"🔍 <b>Мониторинг запущен!</b>\n"
        f"Проверка цен каждую секунду.\n\n"
        f"<i>Сигналы будут приходить при НОВЫХ пробоях канала ±4σ</i>",
        parse_mode='HTML'
    )
    
    monitoring = True
    asyncio.create_task(monitoring_loop())


@router.message(Command("stop"))
async def cmd_stop(message: Message):
    global monitoring
    monitoring = False
    await message.answer("⛔ Мониторинг остановлен")


@router.message(Command("check"))
async def cmd_check(message: Message):
    """Проверка расчёта канала для тестовых инструментов."""
    test_tickers = ['OZON', 'GAZP', 'SFIN']
    
    await message.answer("⏳ Проверяю OZON, GAZP, SFIN...")
    
    # Получаем figi для тикеров
    shares = await tinkoff_client.get_all_shares()
    ticker_to_figi = {s['ticker']: s['figi'] for s in shares}
    ticker_to_name = {s['ticker']: s['name'] for s in shares}
    
    results = []
    for ticker in test_tickers:
        figi = ticker_to_figi.get(ticker)
        if not figi:
            results.append(f"❌ {ticker}: не найден")
            continue
        
        try:
            df = await tinkoff_client.get_candles(figi, interval_mins=TIMEFRAME, period_days=10)
            if len(df) < REGRESSION_LENGTH:
                results.append(f"❌ {ticker}: мало свечей ({len(df)})")
                continue
            
            channel = calculate_linreg_channel(df, REGRESSION_LENGTH)
            price = await tinkoff_client.get_last_price(figi)
            
            # Статус относительно канала
            if price > channel['upper']:
                status = "🔺 ВЫШЕ +4σ"
            elif price < channel['lower']:
                status = "🔻 НИЖЕ -4σ"
            else:
                status = "✅ Внутри канала"
            
            # EMA статус
            ema50 = channel.get('ema50')
            if ema50:
                ema_status = "🟢 выше" if price > ema50 else "🔴 ниже"
            else:
                ema_status = "н/д"
            
            # Тренд
            slope = channel.get('slope', 0)
            trend = "📈" if slope > 0 else "📉" if slope < 0 else "➡️"
            
            name = ticker_to_name.get(ticker, ticker)
            ema_str = f"{ema50:.2f}" if ema50 else "н/д"
            results.append(
                f"<b>{ticker}</b> | {name}\n"
                f"  💰 Цена: {price:.2f}\n"
                f"  📊 Регрессия: {channel['regression']:.2f}\n"
                f"  ⬆️ Верх (+4σ): {channel['upper']:.2f}\n"
                f"  ⬇️ Низ (-4σ): {channel['lower']:.2f}\n"
                f"  📐 EMA50: {ema_str} ({ema_status})\n"
                f"  {trend} Тренд | {status}\n"
                f"  📊 Свечей: {len(df)}"
            )
        except Exception as e:
            results.append(f"❌ {ticker}: ошибка - {str(e)[:50]}")
    
    await message.answer(
        f"<b>🔍 ПРОВЕРКА КАНАЛОВ</b>\n"
        f"TF: {TIMEFRAME} мин | Длина: {REGRESSION_LENGTH}\n\n" +
        "\n\n".join(results),
        parse_mode='HTML'
    )


@router.message(Command("ticker"))
async def cmd_ticker(message: Message):
    """Проверка расчёта канала для указанного тикера."""
    # Получаем тикер из сообщения
    args = message.text.split()
    if len(args) < 2:
        await message.answer(
            "⚠️ Укажите тикер!\n"
            "Пример: /ticker SBER"
        )
        return
    
    ticker = args[1].upper()
    await message.answer(f"⏳ Проверяю {ticker}...")
    
    # Ищем среди акций
    shares = await tinkoff_client.get_all_shares()
    found = next((s for s in shares if s['ticker'] == ticker), None)
    instr_type = "акция"
    
    # Если не нашли среди акций, ищем среди фьючерсов
    if not found:
        futures = await tinkoff_client.get_all_futures(exclude_stock_futures=False, nearest_only=False)
        found = next((f for f in futures if f['ticker'] == ticker), None)
        instr_type = "фьючерс"
    
    if not found:
        await message.answer(f"❌ Тикер {ticker} не найден!")
        return
    
    figi = found['figi']
    name = found['name']
    
    try:
        # Гибридная загрузка: MOEX + Tinkoff
        import datetime
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        
        # Определяем параметры для MOEX
        if instr_type == "фьючерс":
            engine = "futures"
            market = "forts"
            board = "RFUD"
        else:
            engine = "stock"
            market = "shares"
            board = "TQBR"
        
        # Загружаем историю с MOEX
        df_moex = await moex_client.get_candles_until_today(
            engine, market, board, ticker,
            interval=TIMEFRAME, days_back=10
        )
        
        # Загружаем сегодняшние свечи с Tinkoff
        df_tinkoff = await tinkoff_client.get_candles_today_only(figi, interval_mins=TIMEFRAME)
        
        # Объединяем
        if len(df_moex) > 0 and len(df_tinkoff) > 0:
            df = pd.concat([df_moex, df_tinkoff], ignore_index=True)
            df = df.drop_duplicates(subset=['begin']).sort_values('begin').reset_index(drop=True)
            source_info = f"MOEX: {len(df_moex)} + Tinkoff: {len(df_tinkoff)}"
        elif len(df_tinkoff) > 0:
            df = df_tinkoff
            source_info = f"Tinkoff: {len(df_tinkoff)}"
        else:
            df = df_moex
            source_info = f"MOEX: {len(df_moex)}"
        
        if len(df) < REGRESSION_LENGTH:
            await message.answer(f"❌ {ticker}: мало свечей ({len(df)}, нужно {REGRESSION_LENGTH})")
            return
        
        channel = calculate_linreg_channel(df, REGRESSION_LENGTH)
        price = await tinkoff_client.get_last_price(figi)
        
        # Статус относительно канала
        if price > channel['upper']:
            status = "🔺 ВЫШЕ +4σ"
        elif price < channel['lower']:
            status = "🔻 НИЖЕ -4σ"
        else:
            status = "✅ Внутри канала"
        
        # EMA статус
        ema50 = channel.get('ema50')
        if ema50:
            ema_status = "🟢 выше" if price > ema50 else "🔴 ниже"
        else:
            ema_status = "н/д"
        
        # Тренд
        slope = channel.get('slope', 0)
        trend = "📈" if slope > 0 else "📉" if slope < 0 else "➡️"
        
        ema_str = f"{ema50:.2f}" if ema50 else "н/д"
        
        # Последняя свеча
        last_candle = df.iloc[-1]
        last_time = last_candle['begin']
        last_volume = last_candle.get('volume', 0)
        last_close = last_candle.get('close', 0)
        
        # Оборот в рублях (объём * цена закрытия)
        turnover = last_volume * last_close
        if turnover >= 1_000_000_000:
            turnover_str = f"{turnover / 1_000_000_000:.1f}B ₽"
        elif turnover >= 1_000_000:
            turnover_str = f"{turnover / 1_000_000:.1f}M ₽"
        elif turnover >= 1_000:
            turnover_str = f"{turnover / 1_000:.1f}K ₽"
        else:
            turnover_str = f"{int(turnover)} ₽"
        
        result = (
            f"<b>🔍 {ticker}</b> | {name}\n"
            f"<i>{instr_type.capitalize()}</i>\n\n"
            f"💰 Цена: <b>{price:.2f}</b>\n"
            f"📊 Регрессия: {channel['regression']:.2f}\n"
            f"⬆️ Верх (+4σ): {channel['upper']:.2f}\n"
            f"⬇️ Низ (-4σ): {channel['lower']:.2f}\n"
            f"📐 EMA50: {ema_str} ({ema_status})\n"
            f"💹 Оборот (10м): {turnover_str}\n"
            f"{trend} Тренд | <b>{status}</b>\n\n"
            f"📊 Свечей: {len(df)} ({source_info})\n"
            f"🕐 Последняя: {last_time}"
        )
        
        await message.answer(result, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Ошибка проверки {ticker}: {e}")
        await message.answer(f"❌ {ticker}: ошибка - {str(e)[:100]}")


@router.message(Command("status"))
async def cmd_status(message: Message):
    active = sum(1 for i in instruments.values() if i.get('upper'))
    status = "🟢 Работает" if monitoring else "🔴 Остановлен"
    await message.answer(
        f"Статус: {status}\n"
        f"Инструментов: {len(instruments)}\n"
        f"С каналами: {active}\n"
        f"Подписчиков: {len(subscribers)}"
    )


async def load_instruments():
    """Загрузить инструменты: MOEX тикеры + Tinkoff figi."""
    global instruments, figi_to_ticker
    instruments = {}
    figi_to_ticker = {}
    
    try:
        # Получаем figi->ticker из Tinkoff
        shares_tinkoff = await tinkoff_client.get_all_shares()
        ticker_to_figi = {s['ticker']: s['figi'] for s in shares_tinkoff}
        ticker_to_name = {s['ticker']: s['name'] for s in shares_tinkoff}
        
        futures_tinkoff = await tinkoff_client.get_all_futures()
        for f in futures_tinkoff:
            ticker_to_figi[f['ticker']] = f['figi']
            ticker_to_name[f['ticker']] = f['name']
        
        logger.info(f"Tinkoff: {len(shares_tinkoff)} shares, {len(futures_tinkoff)} futures")
        
        # Получаем тикеры с MOEX
        shares_moex = await moex_client.get_shares_tickers()
        futures_moex = await moex_client.get_futures_tickers()
        
        logger.info(f"MOEX: {len(shares_moex)} shares, {len(futures_moex)} futures")
        
        # Акции (пересечение MOEX и Tinkoff)
        for ticker in shares_moex:
            if ticker in ticker_to_figi:
                figi = ticker_to_figi[ticker]
                instruments[ticker] = {
                    'figi': figi,
                    'name': ticker_to_name.get(ticker, ticker),
                    'type': 'share',
                    'engine': 'stock',
                    'market': 'shares',
                    'board': 'TQBR',
                    'upper': None,
                    'lower': None,
                    'last_signal_type': None
                }
                figi_to_ticker[figi] = ticker
        
        # Фьючерсы
        for ticker in futures_moex:
            if ticker in ticker_to_figi:
                figi = ticker_to_figi[ticker]
                instruments[ticker] = {
                    'figi': figi,
                    'name': ticker_to_name.get(ticker, ticker),
                    'type': 'future',
                    'engine': 'futures',
                    'market': 'forts',
                    'board': 'RFUD',
                    'upper': None,
                    'lower': None,
                    'last_signal_type': None
                }
                figi_to_ticker[figi] = ticker
        
        logger.info(f"Total instruments: {len(instruments)}")
        
    except Exception as e:
        logger.error(f"Error loading instruments: {e}")


async def update_all_channels():
    """Обновить каналы для всех инструментов (гибридно: MOEX + Tinkoff)."""
    tickers = list(instruments.keys())
    batch_size = 5  # Можно больше - MOEX без лимитов, Tinkoff только сегодня
    
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        tasks = [update_channel(ticker) for ticker in batch]
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(0.5)  # Небольшая пауза


async def get_current_extremes():
    """Получить список инструментов, которые уже за пределами канала."""
    extremes_up = []
    extremes_down = []
    
    # Собираем figi инструментов с рассчитанными каналами
    figis_with_channels = []
    for ticker, data in instruments.items():
        if data.get('upper') and data.get('figi'):
            figis_with_channels.append(data['figi'])
    
    if not figis_with_channels:
        return extremes_up, extremes_down
    
    # Получаем текущие цены
    for i in range(0, len(figis_with_channels), 100):
        batch = figis_with_channels[i:i+100]
        try:
            prices = await tinkoff_client.get_last_prices_batch(batch)
            
            for figi, price in prices.items():
                if price and price > 0:
                    ticker = figi_to_ticker.get(figi)
                    if ticker:
                        data = instruments.get(ticker)
                        if data and data.get('upper'):
                            if price > data['upper']:
                                instruments[ticker]['last_signal_type'] = 'up'  # Не отправлять сигнал
                                deviation = ((price - data['regression']) / data['regression'] * 100) if data.get('regression') else 0
                                extremes_up.append({
                                    'ticker': ticker,
                                    'name': data.get('name', ''),
                                    'price': price,
                                    'upper': data['upper'],
                                    'deviation': deviation
                                })
                            elif price < data['lower']:
                                instruments[ticker]['last_signal_type'] = 'down'  # Не отправлять сигнал
                                deviation = ((price - data['regression']) / data['regression'] * 100) if data.get('regression') else 0
                                extremes_down.append({
                                    'ticker': ticker,
                                    'name': data.get('name', ''),
                                    'price': price,
                                    'lower': data['lower'],
                                    'deviation': deviation
                                })
        except Exception as e:
            logger.error(f"Error getting prices for extremes: {e}")
    
    # Сортируем по отклонению
    extremes_up.sort(key=lambda x: x['deviation'], reverse=True)
    extremes_down.sort(key=lambda x: x['deviation'])
    
    return extremes_up, extremes_down


async def format_extremes_message(extremes_up, extremes_down):
    """Форматировать сообщение со списком экстремальных позиций."""
    if not extremes_up and not extremes_down:
        return "✅ <b>Все инструменты внутри каналов ±4σ</b>"
    
    lines = ["📊 <b>ТЕКУЩИЕ ПОЗИЦИИ ЗА ПРЕДЕЛАМИ ±4σ</b>\n"]
    
    if extremes_up:
        lines.append(f"\n🔺 <b>ВЫШЕ +4σ ({len(extremes_up)}):</b>")
        for item in extremes_up[:15]:  # Максимум 15
            lines.append(f"  • <b>{item['ticker']}</b> | {item['price']:.2f} ({item['deviation']:+.1f}%)")
        if len(extremes_up) > 15:
            lines.append(f"  ... и ещё {len(extremes_up) - 15}")
    
    if extremes_down:
        lines.append(f"\n🔻 <b>НИЖЕ -4σ ({len(extremes_down)}):</b>")
        for item in extremes_down[:15]:  # Максимум 15
            lines.append(f"  • <b>{item['ticker']}</b> | {item['price']:.2f} ({item['deviation']:+.1f}%)")
        if len(extremes_down) > 15:
            lines.append(f"  ... и ещё {len(extremes_down) - 15}")
    
    lines.append(f"\n⚠️ <i>Для этих инструментов сигналы отправляться не будут, пока цена не вернётся в канал</i>")
    
    return "\n".join(lines)


async def update_channel(ticker):
    """Обновить канал для одного инструмента через Tinkoff (свежие данные)."""
    data = instruments.get(ticker)
    if not data or not data.get('figi'):
        return
    
    try:
        # Гибридная загрузка: MOEX (история) + Tinkoff (сегодня)
        ticker_data = instruments[ticker]
        engine = ticker_data.get('engine', 'stock')
        market = ticker_data.get('market', 'shares')
        board = ticker_data.get('board', 'TQBR')
        
        # 1. MOEX: история до вчера (без лимитов)
        df_moex = await moex_client.get_candles_until_today(
            engine, market, board, ticker, interval=TIMEFRAME, days_back=10
        )
        
        # 2. Tinkoff: только сегодня (мало запросов)
        df_today = await tinkoff_client.get_candles_today_only(data['figi'], interval_mins=TIMEFRAME)
        
        # 3. Объединяем
        if len(df_moex) > 0 and len(df_today) > 0:
            df = pd.concat([df_moex, df_today])
            df = df.drop_duplicates(subset=['begin'], keep='last')
            df = df.sort_values('begin').reset_index(drop=True)
        elif len(df_moex) > 0:
            df = df_moex
        elif len(df_today) > 0:
            df = df_today
        else:
            df = pd.DataFrame()
        
        if len(df) >= REGRESSION_LENGTH:
            channel = calculate_linreg_channel(df, REGRESSION_LENGTH)
            if channel:
                instruments[ticker]['upper'] = channel['upper']
                instruments[ticker]['lower'] = channel['lower']
                instruments[ticker]['regression'] = channel['regression']
                instruments[ticker]['ema50'] = channel['ema50']
                instruments[ticker]['slope'] = channel['slope']
                instruments[ticker]['std'] = channel['std']
                # Сохраняем данные последней свечи для оборота
                last_candle = df.iloc[-1]
                instruments[ticker]['last_volume'] = last_candle.get('volume', 0)
                instruments[ticker]['last_candle_price'] = last_candle.get('close', 0)
                instruments[ticker]['last_candle_time'] = last_candle.get('begin', '')
    except Exception as e:
        logger.debug(f"Error updating channel for {ticker}: {e}")


async def monitoring_loop():
    """Основной цикл мониторинга."""
    global monitoring
    
    last_channel_update = time.time()
    channel_update_interval = TIMEFRAME * 60  # Обновление каналов раз в N минут
    
    while monitoring:
        try:
            # Обновляем каналы периодически
            if time.time() - last_channel_update > channel_update_interval:
                logger.info("Updating channels...")
                await update_all_channels()
                # Отправляем сводку после обновления каналов
                await send_periodic_summary()
                last_channel_update = time.time()
            
            # Проверяем цены батчами через Tinkoff (real-time)
            await check_prices_batch()
            
            await asyncio.sleep(1)  # Проверка каждую секунду
            
        except Exception as e:
            logger.error(f"Monitoring error: {e}")
            await asyncio.sleep(5)


async def send_periodic_summary():
    """Отправить сводку инструментов за пределами ±4σ."""
    if not subscribers:
        return
    
    # Получаем текущие цены
    figis_with_channels = []
    figi_to_ticker_local = {}
    for ticker, data in instruments.items():
        if data.get('upper') and data.get('figi'):
            figis_with_channels.append(data['figi'])
            figi_to_ticker_local[data['figi']] = ticker
    
    if not figis_with_channels:
        return
    
    # Получаем цены батчами
    all_prices = {}
    for i in range(0, len(figis_with_channels), 100):
        batch = figis_with_channels[i:i+100]
        try:
            prices = await tinkoff_client.get_last_prices_batch(batch)
            all_prices.update(prices)
        except Exception as e:
            logger.error(f"Error getting prices for summary: {e}")
    
    # Находим инструменты за пределами канала
    above_list = []
    below_list = []
    
    for figi, price in all_prices.items():
        if not price or price <= 0:
            continue
        ticker = figi_to_ticker_local.get(figi)
        if not ticker:
            continue
        data = instruments.get(ticker)
        if not data:
            continue
        
        upper = data.get('upper', 0)
        lower = data.get('lower', 0)
        regression = data.get('regression', 0)
        
        if price > upper and regression > 0:
            deviation = (price - regression) / regression * 100
            above_list.append((ticker, price, deviation))
        elif price < lower and regression > 0:
            deviation = (price - regression) / regression * 100
            below_list.append((ticker, price, deviation))
    
    # Если никого нет за каналом — не отправляем
    if not above_list and not below_list:
        return
    
    # Формируем сообщение
    import datetime
    now_msk = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    time_str = now_msk.strftime("%H:%M")
    
    lines = [f"📊 <b>ОБЗОР ({time_str})</b>\n"]
    
    if above_list:
        above_list.sort(key=lambda x: -x[2])  # Сортировка по отклонению
        lines.append(f"🔺 <b>ВЫШЕ +4σ ({len(above_list)}):</b>")
        for ticker, price, dev in above_list[:10]:  # Макс 10
            lines.append(f"  • {ticker} | {price:.2f} ({dev:+.1f}%)")
        lines.append("")
    
    if below_list:
        below_list.sort(key=lambda x: x[2])  # Сортировка по отклонению
        lines.append(f"🔻 <b>НИЖЕ -4σ ({len(below_list)}):</b>")
        for ticker, price, dev in below_list[:10]:  # Макс 10
            lines.append(f"  • {ticker} | {price:.2f} ({dev:+.1f}%)")
    
    message = "\n".join(lines)
    
    for chat_id in subscribers:
        try:
            await bot.send_message(chat_id, message, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error sending summary to {chat_id}: {e}")


async def check_prices_batch():
    """Проверить цены всех инструментов с каналами через Tinkoff."""
    # Собираем figi инструментов с рассчитанными каналами
    figis_with_channels = []
    for ticker, data in instruments.items():
        if data.get('upper') and data.get('figi'):
            figis_with_channels.append(data['figi'])
    
    if not figis_with_channels:
        return
    
    # Tinkoff API позволяет до 100 инструментов за раз
    for i in range(0, len(figis_with_channels), 100):
        batch = figis_with_channels[i:i+100]
        try:
            prices = await tinkoff_client.get_last_prices_batch(batch)
            
            for figi, price in prices.items():
                if price and price > 0:
                    ticker = figi_to_ticker.get(figi)
                    if ticker:
                        await check_breakout(ticker, price)
                    
        except Exception as e:
            logger.error(f"Error getting prices: {e}")


async def check_breakout(ticker, current_price):
    """Проверить пробой для инструмента."""
    data = instruments.get(ticker)
    if not data or not data.get('upper'):
        return
    
    upper = data['upper']
    lower = data['lower']
    last_signal = data.get('last_signal_type')
    
    signal_type = None
    
    if current_price > upper and last_signal != 'up':
        signal_type = 'up'
        instruments[ticker]['last_signal_type'] = 'up'
        
    elif current_price < lower and last_signal != 'down':
        signal_type = 'down'
        instruments[ticker]['last_signal_type'] = 'down'
        
    elif lower <= current_price <= upper:
        instruments[ticker]['last_signal_type'] = None
    
    if signal_type:
        await send_signal(ticker, data, current_price, signal_type)


async def send_signal(ticker, data, price, signal_type):
    """Отправить сигнал всем подписчикам."""
    emoji = "🔺" if signal_type == 'up' else "🔻"
    direction = "ВВЕРХ" if signal_type == 'up' else "ВНИЗ"
    
    type_emoji = {"share": "📊", "future": "📈", "bond": "💰"}.get(data['type'], "📊")
    type_name = {"share": "Акция", "future": "Фьючерс", "bond": "Облигация"}.get(data['type'], "")
    
    # Тренд канала
    slope = data.get('slope', 0)
    if slope > 0:
        trend = "📈 Восходящий"
    elif slope < 0:
        trend = "📉 Нисходящий"
    else:
        trend = "➡️ Боковой"
    
    # EMA 50
    ema50 = data.get('ema50')
    if ema50:
        if price > ema50:
            ema_status = f"🟢 Выше EMA50 ({ema50:.2f})"
        else:
            ema_status = f"🔴 Ниже EMA50 ({ema50:.2f})"
    else:
        ema_status = "EMA50: н/д"
    
    # Отклонение от регрессии
    regression = data.get('regression', 0)
    deviation_pct = ((price - regression) / regression * 100) if regression else 0
    
    # Ширина канала
    channel_width = data['upper'] - data['lower']
    channel_pct = (channel_width / regression * 100) if regression else 0
    
    # Оборот последней свечи в рублях
    last_volume = data.get('last_volume', 0)
    last_candle_price = data.get('last_candle_price', price)
    turnover = last_volume * last_candle_price
    if turnover >= 1_000_000_000:
        turnover_str = f"{turnover / 1_000_000_000:.1f}B ₽"
    elif turnover >= 1_000_000:
        turnover_str = f"{turnover / 1_000_000:.1f}M ₽"
    elif turnover >= 1_000:
        turnover_str = f"{turnover / 1_000:.1f}K ₽"
    else:
        turnover_str = f"{int(turnover)} ₽"

    message = (
        f"{emoji} <b>ПРОБОЙ {direction}!</b>\n\n"
        f"{type_emoji} <b>{ticker}</b> | {data.get('name', '')}\n"
        f"📋 {type_name}\n\n"
        f"💰 <b>Цена: {price:.2f}</b>\n"
        f"📊 Регрессия: {regression:.2f} ({deviation_pct:+.1f}%)\n"
        f"⬆️ Верх (+4σ): {data['upper']:.2f}\n"
        f"⬇️ Низ (-4σ): {data['lower']:.2f}\n"
        f"📏 Ширина канала: {channel_pct:.1f}%\n"
        f"💹 Оборот (10м): {turnover_str}\n\n"
    )
    
    for chat_id in subscribers:
        try:
            await bot.send_message(chat_id, message, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error sending to {chat_id}: {e}")


async def main():
    global bot, dp, tinkoff_client, moex_client
    
    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN not set in .env!")
        return
    
    if not TINKOFF_TOKEN:
        logger.error("TINKOFF_TOKEN not set in .env!")
        return
    
    bot = Bot(token=TELEGRAM_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    
    tinkoff_client = TinkoffClient(TINKOFF_TOKEN)
    moex_client = MoexClient()
    
    logger.info("Starting bot...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
