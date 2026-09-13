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

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT"
]

# Struktur analizi üçün candle
TIMEFRAME = "15"

# Neçə candle saxlanılsın
MAX_CANDLES = 200

# Risk / Reward
RR = 2.0

# SQLite
DB_FILE = "trades.db"

# Bybit public linear websocket
WS_URL = "wss://stream.bybit.com/v5/public/linear"


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
            symbol TEXT,
            side TEXT,
            entry REAL,
            stop_loss REAL,
            take_profit REAL,
            status TEXT,
            exit_price REAL,
            created_at REAL,
            closed_at REAL
        )
    """)

    conn.commit()
    conn.close()


def save_trade(trade):

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

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram ENV dəyişənləri yoxdur.")
        return

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
            print("Telegram: OK")
        else:
            print("Telegram error:", data)

    except Exception as e:
        print("Telegram error:", e)


# ============================================================
# INDICATORS
# ============================================================

def ema(values, period):

    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    result = values[0]

    for value in values[1:]:
        result = (
            value - result
        ) * multiplier + result

    return result


def atr(data, period=14):

    if len(data) < period + 1:
        return None

    trs = []

    for i in range(1, len(data)):

        high = data[i]["high"]
        low = data[i]["low"]
        previous_close = data[i - 1]["close"]

        tr = max(
            high - low,
            abs(high - previous_close),
            abs(low - previous_close)
        )

        trs.append(tr)

    if len(trs) < period:
        return None

    return sum(trs[-period:]) / period


# ============================================================
# REAL-TIME PRICE
# ============================================================

def update_price(symbol, price):

    with lock:

        current_prices[symbol] = price

    check_trade(symbol, price)


# ============================================================
# SMC ANALYSIS
# ============================================================

def analyze_symbol(symbol):

    with lock:

        data = list(candles[symbol])

    # Yetərli data yoxdursa
    if len(data) < 60:
        return None

    # Son candle artıq bağlanmış candle olmalıdır.
    current = data[-1]
    previous = data[-2]

    # Son 10 candle
    lookback = data[-12:-2]

    if len(lookback) < 5:
        return None

    highs = [
        candle["high"]
        for candle in lookback
    ]

    lows = [
        candle["low"]
        for candle in lookback
    ]

    recent_high = max(highs)
    recent_low = min(lows)

    closes = [
        candle["close"]
        for candle in data
    ]

    ema20 = ema(
        closes[-80:],
        20
    )

    ema50 = ema(
        closes[-100:],
        50
    )

    current_atr = atr(data)

    if (
        ema20 is None
        or ema50 is None
        or current_atr is None
        or current_atr <= 0
    ):
        return None

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

    volumes = [
        candle["volume"]
        for candle in data[-21:-1]
    ]

    if not volumes:
        return None

    average_volume = (
        sum(volumes) / len(volumes)
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
            - current_atr * 0.20
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
            "reason": [
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
            + current_atr * 0.20
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
            "reason": [
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

        if symbol in active_trades:
            return

        candle_time = signal["candle_time"]

        # Eyni candle-dan ikinci signal vermə
        if (
            last_signal_candle[symbol]
            == candle_time
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
        "✅ " + x
        for x in signal["reason"]
    )

    message = f"""
🚨 SMC PRO REAL-TIME SIGNAL

{emoji} {symbol} {trade["side"]}

Entry: {trade["entry"]:.6f}
SL: {trade["sl"]:.6f}
TP: {trade["tp"]:.6f}

RR: 1:{RR}

{reasons}

⏳ Status: ACTIVE
"""

    print(message)

    send_telegram(message)


# ============================================================
# TP / SL MONITOR
# ============================================================

def check_trade(symbol, price):

    with lock:

        trade = active_trades.get(symbol)

    if not trade:
        return

    side = trade["side"]

    result = None

    # LONG
    if side == "LONG":

        if price >= trade["tp"]:
            result = "WIN"

        elif price <= trade["sl"]:
            result = "LOSS"

    # SHORT
    elif side == "SHORT":

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

    send_result(trade)


# ============================================================
# RESULT
# ============================================================

def send_result(trade):

    result = trade["status"]

    if result == "WIN":

        message = f"""
✅ TP HIT

{trade["symbol"]} {trade["side"]}

Entry: {trade["entry"]:.6f}
TP: {trade["tp"]:.6f}
Exit: {trade["exit_price"]:.6f}

RESULT: WIN 🟢
"""

    else:

        message = f"""
❌ SL HIT

{trade["symbol"]} {trade["side"]}

Entry: {trade["entry"]:.6f}
SL: {trade["sl"]:.6f}
Exit: {trade["exit_price"]:.6f}

RESULT: LOSS 🔴
"""

    send_telegram(message)

    send_statistics()


# ============================================================
# STATISTICS
# ============================================================

def get_statistics():

    conn = sqlite3.connect(DB_FILE)

    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            COUNT(*),
            SUM(
                CASE
                    WHEN status = 'WIN'
                    THEN 1
                    ELSE 0
                END
            ),
            SUM(
                CASE
                    WHEN status = 'LOSS'
                    THEN 1
                    ELSE 0
                END
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


def send_statistics():

    stats = get_statistics()

    message = f"""
📊 SMC BOT STATISTICS

Total: {stats["total"]}
WIN: {stats["wins"]}
LOSS: {stats["losses"]}

Win Rate:
{stats["win_rate"]}%
"""

    send_telegram(message)


# ============================================================
# BYBIT WEBSOCKET
# ============================================================

def on_message(ws, message):

    try:

        data = json.loads(message)

        topic = data.get(
            "topic",
            ""
        )

        # ----------------------------------------------------
        # KLINE
        # ----------------------------------------------------

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

                    existing = candles[
                        symbol
                    ]

                    # Eyni candle-ı yenilə
                    if (
                        existing
                        and
                        existing[-1]["time"]
                        == candle["time"]
                    ):

                        existing[-1] = candle

                    else:

                        existing.append(
                            candle
                        )

                    if len(existing) > MAX_CANDLES:
                        del existing[
                            :-MAX_CANDLES
                        ]

                # Yalnız candle bağlananda analiz
                if candle["confirm"]:

                    signal = analyze_symbol(
                        symbol
                    )

                    if signal:
                        create_signal(
                            signal
                        )

        # ----------------------------------------------------
        # TICKER
        # ----------------------------------------------------

        elif topic.startswith("tickers."):

            symbol = topic.split(".")[-1]

            items = data.get(
                "data",
                {}
            )

            last_price = items.get(
                "lastPrice"
            )

            if last_price:

                update_price(
                    symbol,
                    float(last_price)
                )

    except Exception as e:

        print(
            "WebSocket message error:",
            e
        )


def on_error(ws, error):

    print(
        "WebSocket error:",
        error
    )


def on_close(ws, close_status_code, close_msg):

    print(
        "WebSocket bağlandı."
    )


def on_open(ws):

    print(
        "🚀 Bybit WebSocket connected."
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
        "Subscribed:",
        topics
    )


def websocket_worker():

    while True:

        try:

            ws = websocket.WebSocketApp(
                WS_URL,
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
                "WebSocket restart:",
                e
            )

        print(
            "5 saniyə sonra yenidən qoşulur..."
        )

        time.sleep(5)


# ============================================================
# STARTUP
# ============================================================

def startup():

    init_db()

    thread = threading.Thread(
        target=websocket_worker,
        daemon=True
    )

    thread.start()

    send_telegram(
        "🚀 SMC PRO REAL-TIME BOT AKTİVDİR!\n\n"
        "📡 Bybit WebSocket bağlantısı aktivdir.\n"
        "🔎 BTCUSDT / ETHUSDT / SOLUSDT izlənilir.\n"
        "📊 SMC setup-ları real-time yoxlanılır.\n"
        "🎯 TP/SL avtomatik izlənilir."
    )


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/")
def home():

    stats = get_statistics()

    return jsonify({
        "status": "online",
        "mode": "REAL-TIME",
        "symbols": SYMBOLS,
        "active_trades": len(
            active_trades
        ),
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
