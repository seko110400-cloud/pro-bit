import os
import json
import time
import sqlite3
import threading
import requests
import websocket

from flask import Flask, jsonify


# ============================================================
# CONFIG
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT"
]

TIMEFRAME = "15"
MAX_CANDLES = 200

RR = 2.0

DB_FILE = "trades.db"


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# GLOBAL DATA
# ============================================================

candles = {
    symbol: []
    for symbol in SYMBOLS
}

current_prices = {
    symbol: None
    for symbol in SYMBOLS
}

active_trades = {}

last_signal_candle = {
    symbol: None
    for symbol in SYMBOLS
}

lock = threading.Lock()


# ============================================================
# DATABASE
# ============================================================

def init_db():

    conn = sqlite3.connect(DB_FILE)

    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry REAL NOT NULL,
            stop_loss REAL NOT NULL,
            take_profit REAL NOT NULL,
            status TEXT NOT NULL,
            exit_price REAL,
            created_at REAL NOT NULL,
            closed_at REAL
        )
    """)

    conn.commit()
    conn.close()

    print("✅ SQLite database hazır.")


# DATABASE-İ PROQRAM BAŞLAMAMIŞDAN ƏVVƏL YARAT
init_db()


# ============================================================
# DATABASE - SAVE TRADE
# ============================================================

def save_trade(trade):

    init_db()

    conn = sqlite3.connect(DB_FILE)

    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO trades
        (
            symbol,
            side,
            entry,
            stop_loss,
            take_profit,
            status,
            exit_price,
            created_at,
            closed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        trade["symbol"],
        trade["side"],
        trade["entry"],
        trade["sl"],
        trade["tp"],
        trade["status"],
        trade.get("exit_price"),
        trade["created_at"],
        trade.get("closed_at")
    ))

    conn.commit()
    conn.close()


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):

    if not TELEGRAM_BOT_TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN yoxdur.")
        return False

    if not TELEGRAM_CHAT_ID:
        print("❌ TELEGRAM_CHAT_ID yoxdur.")
        return False

    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }

    try:

        response = requests.post(
            url,
            json=payload,
            timeout=10
        )

        data = response.json()

        if data.get("ok"):
            print("✅ Telegram mesajı göndərildi.")
            return True

        print("❌ Telegram xətası:", data)

    except Exception as e:

        print(
            "❌ Telegram bağlantı xətası:",
            e
        )

    return False


# ============================================================
# BYBIT HISTORICAL DATA
# ============================================================

def load_initial_candles(symbol):

    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": TIMEFRAME,
        "limit": MAX_CANDLES
    }

    try:

        response = requests.get(
            BYBIT_KLINE_URL,
            params=params,
            timeout=15
        )

        data = response.json()

        if data.get("retCode") != 0:

            print(
                f"❌ Bybit error {symbol}:",
                data
            )

            return

        rows = data["result"]["list"]

        rows.reverse()

        loaded = []

        for row in rows:

            candle = {
                "time": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "confirm": True
            }

            loaded.append(candle)

        with lock:

            candles[symbol] = loaded[-MAX_CANDLES:]

        print(
            f"📥 {symbol}: "
            f"{len(loaded)} candle yükləndi."
        )

    except Exception as e:

        print(
            f"❌ {symbol} historical data xətası:",
            e
        )


def load_all_initial_data():

    print("📥 Bybit historical data yüklənir...")

    for symbol in SYMBOLS:

        load_initial_candles(symbol)

        time.sleep(0.5)

    print("✅ Historical data hazırdır.")


# ============================================================
# EMA
# ============================================================

def calculate_ema(values, period):

    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    result = values[0]

    for value in values[1:]:

        result = (
            (value - result) * multiplier
        ) + result

    return result


# ============================================================
# ATR
# ============================================================

def calculate_atr(data, period=14):

    if len(data) < period + 1:
        return None

    true_ranges = []

    for i in range(1, len(data)):

        high = data[i]["high"]
        low = data[i]["low"]

        previous_close = data[i - 1]["close"]

        tr = max(
            high - low,
            abs(high - previous_close),
            abs(low - previous_close)
        )

        true_ranges.append(tr)

    if len(true_ranges) < period:
        return None

    return sum(
        true_ranges[-period:]
    ) / period


# ============================================================
# REAL-TIME SMC ANALYSIS
# ============================================================

def analyze_symbol(symbol):

    with lock:

        data = list(candles[symbol])

    if len(data) < 60:
        return None

    # Son bağlanmış candle
    current = data[-1]

    previous = data[-2]

    # Son 10 candle
    lookback = data[-12:-2]

    if len(lookback) < 5:
        return None

    # ========================================================
    # EMA
    # ========================================================

    closes = [
        x["close"]
        for x in data
    ]

    ema20 = calculate_ema(
        closes[-80:],
        20
    )

    ema50 = calculate_ema(
        closes[-100:],
        50
    )

    if ema20 is None or ema50 is None:
        return None

    # ========================================================
    # ATR
    # ========================================================

    current_atr = calculate_atr(data)

    if current_atr is None or current_atr <= 0:
        return None

    # ========================================================
    # STRUCTURE
    # ========================================================

    recent_high = max(
        x["high"]
        for x in lookback
    )

    recent_low = min(
        x["low"]
        for x in lookback
    )

    # ========================================================
    # TREND
    # ========================================================

    bullish_trend = ema20 > ema50

    bearish_trend = ema20 < ema50

    # ========================================================
    # LIQUIDITY SWEEP
    # ========================================================

    bullish_sweep = (
        current["low"] < recent_low
        and
        current["close"] > recent_low
    )

    bearish_sweep = (
        current["high"] > recent_high
        and
        current["close"] < recent_high
    )

    # ========================================================
    # BOS
    # ========================================================

    bullish_bos = (
        current["close"] > previous["high"]
    )

    bearish_bos = (
        current["close"] < previous["low"]
    )

    # ========================================================
    # VOLUME
    # ========================================================

    previous_volumes = [
        x["volume"]
        for x in data[-21:-1]
    ]

    if not previous_volumes:
        return None

    average_volume = (
        sum(previous_volumes)
        /
        len(previous_volumes)
    )

    volume_confirmation = (
        current["volume"]
        >= average_volume * 0.8
    )

    # ========================================================
    # LONG
    # ========================================================

    if (
        bullish_trend
        and bullish_sweep
        and bullish_bos
        and volume_confirmation
    ):

        entry = current["close"]

        sl = (
            min(
                current["low"],
                recent_low
            )
            -
            current_atr * 0.20
        )

        risk = entry - sl

        if risk <= 0:
            return None

        tp = entry + (
            risk * RR
        )

        return {
            "symbol": symbol,
            "side": "LONG",
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "candle_time": current["time"],
            "reasons": [
                "Liquidity Sweep",
                "Bullish BOS",
                "EMA Trend",
                "Volume"
            ]
        }

    # ========================================================
    # SHORT
    # ========================================================

    if (
        bearish_trend
        and bearish_sweep
        and bearish_bos
        and volume_confirmation
    ):

        entry = current["close"]

        sl = (
            max(
                current["high"],
                recent_high
            )
            +
            current_atr * 0.20
        )

        risk = sl - entry

        if risk <= 0:
            return None

        tp = entry - (
            risk * RR
        )

        return {
            "symbol": symbol,
            "side": "SHORT",
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "candle_time": current["time"],
            "reasons": [
                "Liquidity Sweep",
                "Bearish BOS",
                "EMA Trend",
                "Volume"
            ]
        }

    return None


# ============================================================
# CREATE SIGNAL
# ============================================================

def create_signal(signal):

    symbol = signal["symbol"]

    with lock:

        # Eyni symbol-da aktiv trade varsa
        if symbol in active_trades:
            return

        candle_time = signal["candle_time"]

        # Eyni candle üçün ikinci signal yox
        if (
            last_signal_candle[symbol]
            ==
            candle_time
        ):
            return

        last_signal_candle[symbol] = candle_time

        trade = {
            "symbol": symbol,
            "side": signal["side"],
            "entry": signal["entry"],
            "sl": signal["sl"],
            "tp": signal["tp"],
            "status": "ACTIVE",
            "created_at": time.time()
        }

        active_trades[symbol] = trade

    emoji = (
        "🟢"
        if trade["side"] == "LONG"
        else "🔴"
    )

    reasons = "\n".join(
        "✅ " + reason
        for reason in signal["reasons"]
    )

    message = f"""
🚨 SMC PRO SIGNAL

{emoji} {symbol} {trade["side"]}

Entry:
{trade["entry"]:.6f}

Stop Loss:
{trade["sl"]:.6f}

Take Profit:
{trade["tp"]:.6f}

Risk / Reward:
1:{RR}

{reasons}

⏳ Status: ACTIVE
"""

    print(message)

    send_telegram(message)


# ============================================================
# TP / SL CHECK
# ============================================================

def check_trade(symbol, price):

    with lock:

        trade = active_trades.get(symbol)

    if not trade:
        return

    result = None

    # ========================================================
    # LONG
    # ========================================================

    if trade["side"] == "LONG":

        if price >= trade["tp"]:

            result = "WIN"

        elif price <= trade["sl"]:

            result = "LOSS"

    # ========================================================
    # SHORT
    # ========================================================

    elif trade["side"] == "SHORT":

        if price <= trade["tp"]:

            result = "WIN"

        elif price >= trade["sl"]:

            result = "LOSS"

    if result is None:
        return

    trade["status"] = result
    trade["exit_price"] = price
    trade["closed_at"] = time.time()

    with lock:

        active_trades.pop(
            symbol,
            None
        )

    save_trade(trade)

    print(
        f"🏁 {symbol} -> {result}"
    )

    send_result_message(trade)


# ============================================================
# RESULT MESSAGE
# ============================================================

def send_result_message(trade):

    if trade["status"] == "WIN":

        message = f"""
✅ TP HIT

{trade["symbol"]} {trade["side"]}

Entry:
{trade["entry"]:.6f}

TP:
{trade["tp"]:.6f}

Exit:
{trade["exit_price"]:.6f}

RESULT: WIN 🟢
"""

    else:

        message = f"""
❌ SL HIT

{trade["symbol"]} {trade["side"]}

Entry:
{trade["entry"]:.6f}

SL:
{trade["sl"]:.6f}

Exit:
{trade["exit_price"]:.6f}

RESULT: LOSS 🔴
"""

    send_telegram(message)

    send_statistics()


# ============================================================
# STATISTICS
# ============================================================

def get_statistics():

    # ƏSAS DÜZƏLİŞ:
    # Cədvəl yoxdursa əvvəlcə yaradır.
    init_db()

    conn = sqlite3.connect(DB_FILE)

    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            COUNT(*),
            COALESCE(
                SUM(
                    CASE
                        WHEN status = 'WIN'
                        THEN 1
                        ELSE 0
                    END
                ),
                0
            ),
            COALESCE(
                SUM(
                    CASE
                        WHEN status = 'LOSS'
                        THEN 1
                        ELSE 0
                    END
                ),
                0
            )
        FROM trades
    """)

    row = cursor.fetchone()

    conn.close()

    total = row[0] or 0
    wins = row[1] or 0
    losses = row[2] or 0

    if total > 0:

        win_rate = (
            wins / total
        ) * 100

    else:

        win_rate = 0

    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "win_rate": round(
            win_rate,
            2
        )
    }


# ============================================================
# TELEGRAM STATISTICS
# ============================================================

def send_statistics():

    stats = get_statistics()

    message = f"""
📊 SMC BOT STATISTICS

Total Trades:
{stats["total"]}

WIN:
{stats["wins"]}

LOSS:
{stats["losses"]}

Win Rate:
{stats["win_rate"]}%
"""

    send_telegram(message)


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def on_message(ws, message):

    try:

        data = json.loads(message)

        topic = data.get(
            "topic",
            ""
        )

        # ====================================================
        # KLINE
        # ====================================================

        if topic.startswith("kline."):

            symbol = topic.split(".")[-1]

            items = data.get(
                "data",
                []
            )

            for item in items:

                candle = {
                    "time": int(
                        item["start"]
                    ),
                    "open": float(
                        item["open"]
                    ),
                    "high": float(
                        item["high"]
                    ),
                    "low": float(
                        item["low"]
                    ),
                    "close": float(
                        item["close"]
                    ),
                    "volume": float(
                        item["volume"]
                    ),
                    "confirm": bool(
                        item["confirm"]
                    )
                }

                with lock:

                    data_list = candles[
                        symbol
                    ]

                    if (
                        data_list
                        and
                        data_list[-1]["time"]
                        ==
                        candle["time"]
                    ):

                        data_list[-1] = candle

                    else:

                        data_list.append(
                            candle
                        )

                    if len(data_list) > MAX_CANDLES:

                        del data_list[
                            :-MAX_CANDLES
                        ]

                # YALNIZ CANDLE BAĞLANANDA
                # YENİ SETUP AXTAR
                if candle["confirm"]:

                    signal = analyze_symbol(
                        symbol
                    )

                    if signal:

                        create_signal(
                            signal
                        )

        # ====================================================
        # TICKER
        # ====================================================

        elif topic.startswith("tickers."):

            symbol = topic.split(".")[-1]

            ticker_data = data.get(
                "data",
                {}
            )

            price = ticker_data.get(
                "lastPrice"
            )

            if price:

                price = float(price)

                with lock:

                    current_prices[
                        symbol
                    ] = price

                # TP / SL REAL-TIME
                check_trade(
                    symbol,
                    price
                )

    except Exception as e:

        print(
            "❌ WebSocket message error:",
            e
        )


# ============================================================
# WEBSOCKET OPEN
# ============================================================

def on_open(ws):

    print(
        "✅ Bybit WebSocket bağlantısı aktivdir."
    )

    topics = []

    for symbol in SYMBOLS:

        topics.append(
            f"kline.{TIMEFRAME}.{symbol}"
        )

        topics.append(
            f"tickers.{symbol}"
        )

    payload = {
        "op": "subscribe",
        "args": topics
    }

    ws.send(
        json.dumps(payload)
    )

    print(
        "📡 Subscribed:",
        topics
    )


# ============================================================
# WEBSOCKET ERROR
# ============================================================

def on_error(ws, error):

    print(
        "❌ WebSocket error:",
        error
    )


# ============================================================
# WEBSOCKET CLOSE
# ============================================================

def on_close(
    ws,
    close_status_code,
    close_msg
):

    print(
        "⚠️ WebSocket bağlantısı bağlandı."
    )


# ============================================================
# WEBSOCKET WORKER
# ============================================================

def websocket_worker():

    while True:

        try:

            ws = websocket.WebSocketApp(
                BYBIT_WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as e:

            print(
                "❌ WebSocket worker error:",
                e
            )

        print(
            "🔄 5 saniyədən sonra "
            "WebSocket yenidən qoşulur..."
        )

        time.sleep(5)


# ============================================================
# STARTUP
# ============================================================

def startup():

    print(
        "🚀 SMC PRO BOT BAŞLAYIR..."
    )

    # Əvvəl tarixi candle-ları götür
    load_all_initial_data()

    # Sonra WebSocket başlat
    thread = threading.Thread(
        target=websocket_worker,
        daemon=True
    )

    thread.start()

    send_telegram(
        "🚀 SMC PRO REAL-TIME BOT AKTİVDİR!\n\n"
        "📡 Bybit WebSocket bağlantısı hazırlanır.\n"
        "🔎 BTCUSDT / ETHUSDT / SOLUSDT izlənilir.\n"
        "📊 SMC + Liquidity Sweep + BOS + EMA + Volume\n"
        "🎯 TP/SL real-time izlənilir.\n"
        "💾 WIN/LOSS SQLite-də saxlanılır."
    )


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():

    stats = get_statistics()

    with lock:

        active_count = len(
            active_trades
        )

    return jsonify({
        "status": "online",
        "mode": "REAL-TIME",
        "symbols": SYMBOLS,
        "timeframe": TIMEFRAME,
        "active_trades": active_count,
        "statistics": stats
    })


@app.route("/health")
def health():

    return "OK", 200


@app.route("/stats")
def stats():

    return jsonify(
        get_statistics()
    )


@app.route("/active")
def active():

    with lock:

        return jsonify(
            list(
                active_trades.values()
            )
        )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    startup()

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
