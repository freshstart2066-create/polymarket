"""
Polymarket Advanced Whale & Anomaly Bot
Tracks: whale trades, volume spikes, price divergences, order walls,
flash crashes, liquidity drains, coordinated trades, and market manipulation patterns.
"""

import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from statistics import stdev, mean

import httpx
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("polybot")

# ── Config ────────────────────────────────────────────────────────────────────
TOKEN                = os.getenv("TELEGRAM_TOKEN", "8913424520:AAEfpVp07jdokzhXlAgZjiQxR7bCvWu4qAg")
OWNER_CHAT_ID        = os.getenv("OWNER_CHAT_ID", "8316516258")
WHALE_THRESHOLD_USD  = float(os.getenv("WHALE_THRESHOLD_USD", "5000"))
VOLUME_SPIKE_MULT    = float(os.getenv("VOLUME_SPIKE_MULT", "3.0"))
FLASH_CRASH_PCT      = float(os.getenv("FLASH_CRASH_PCT", "10.0"))     # >10% in 5s
LIQUIDITY_DRAIN_PCT  = float(os.getenv("LIQUIDITY_DRAIN_PCT", "50.0")) # >50% gone in 10s
ORDER_WALL_THRESHOLD = float(os.getenv("ORDER_WALL_THRESHOLD", "10000"))
COORDINATED_THRESHOLD = int(os.getenv("COORDINATED_THRESHOLD", "5"))    # 5+ trades in 2s
POLYMARKET_WS_URL    = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
TELEGRAM_API         = f"https://api.telegram.org/bot{TOKEN}"

# ── Subscriber store ──────────────────────────────────────────────────────────
subscribers: dict[str, set] = {}
user_thresholds: dict[str, float] = {}
user_sensitivity: dict[str, str] = {}  # "strict" | "normal" | "relaxed"

ALL_TYPES = {
    "whale", "volume_spike", "flash_crash", "liquidity_drain",
    "order_wall", "coordinated_trades", "price_divergence",
    "bid_ask_collapse", "momentum_reversal"
}

# ── Market state ──────────────────────────────────────────────────────────────
market_trades = defaultdict(lambda: deque(maxlen=200))       # Recent trades
market_prices = defaultdict(dict)                             # Last price per outcome
market_volumes = defaultdict(lambda: deque(maxlen=120))      # Vol window (2 min)
market_order_book = defaultdict(dict)                         # Current book state
price_history = defaultdict(lambda: deque(maxlen=60))        # Price over time
bid_ask_spreads = defaultdict(lambda: deque(maxlen=50))      # Spread history

# ── Telegram helpers ──────────────────────────────────────────────────────────
async def send(chat_id: str, text: str, parse_mode="Markdown"):
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            await client.post(f"{TELEGRAM_API}/sendMessage", json={
                "chat_id": chat_id, "text": text, "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            })
    except Exception as e:
        log.warning(f"Send failed to {chat_id}: {e}")

async def broadcast(alert_type: str, text: str):
    """Send alert to subscribers who want this type."""
    tasks = []
    for chat_id, types in list(subscribers.items()):
        if alert_type in types:
            tasks.append(send(chat_id, text))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

# ── Command handlers ──────────────────────────────────────────────────────────
async def cmd_start(chat_id: str, username: str):
    subscribers[chat_id] = set(ALL_TYPES)
    user_sensitivity[chat_id] = "normal"
    await send(chat_id, (
        "🔭 *Polymarket Advanced Monitor v2*\n\n"
        "Tracking 9 anomaly types:\n"
        "🐳 Whale trades ($5k+)\n"
        "📈 Volume spikes (3×)\n"
        "💥 Flash crashes (>10% in 5s)\n"
        "🚨 Liquidity drains (>50% in 10s)\n"
        "🧱 Order walls ($10k+)\n"
        "🔄 Coordinated trades (5+ in 2s)\n"
        "⚡ Price divergences (>5%)\n"
        "📊 Bid-ask collapse (spread <1%)\n"
        "🔀 Momentum reversals\n\n"
        "/help — full command list"
    ))
    log.info(f"New subscriber: {chat_id} ({username})")

async def cmd_stop(chat_id: str):
    subscribers.pop(chat_id, None)
    user_thresholds.pop(chat_id, None)
    user_sensitivity.pop(chat_id, None)
    await send(chat_id, "👋 Unsubscribed. Send /start to rejoin.")

async def cmd_status(chat_id: str):
    if chat_id not in subscribers:
        await send(chat_id, "Not subscribed. Send /start.")
        return
    types = subscribers[chat_id]
    threshold = user_thresholds.get(chat_id, WHALE_THRESHOLD_USD)
    sensitivity = user_sensitivity.get(chat_id, "normal")
    active = len(types)
    await send(chat_id, (
        f"📊 *Your Settings*\n\n"
        f"Whale threshold: `${threshold:,.0f}`\n"
        f"Sensitivity: `{sensitivity}`\n"
        f"Active alerts: `{active}/9`\n"
        f"Total subscribers: `{len(subscribers)}`"
    ))

async def cmd_setalert(chat_id: str, args: list):
    if not args or not args[0].replace(".", "").isdigit():
        await send(chat_id, "Usage: `/setalert 10000`")
        return
    amount = float(args[0])
    user_thresholds[chat_id] = amount
    await send(chat_id, f"✅ Whale threshold → `${amount:,.0f}`")

async def cmd_sensitivity(chat_id: str, args: list):
    if not args or args[0] not in ("strict", "normal", "relaxed"):
        await send(chat_id, "Usage: `/sensitivity strict|normal|relaxed`\n\n"
                           "strict = more alerts (lower thresholds)\n"
                           "normal = balanced (default)\n"
                           "relaxed = fewer alerts (higher thresholds)")
        return
    mode = args[0]
    user_sensitivity[chat_id] = mode
    await send(chat_id, f"✅ Sensitivity → `{mode}`")

async def cmd_toggle_alerts(chat_id: str, args: list):
    """Toggle specific alert types: /toggle whale on, /toggle volume off"""
    mapping = {
        "whale": "whale",
        "volume": "volume_spike",
        "crash": "flash_crash",
        "drain": "liquidity_drain",
        "wall": "order_wall",
        "coordinated": "coordinated_trades",
        "price": "price_divergence",
        "spread": "bid_ask_collapse",
        "momentum": "momentum_reversal",
    }
    if len(args) < 2 or args[0] not in ("on", "off") or args[1] not in mapping:
        await send(chat_id, "Usage: `/toggle on whale` or `/toggle off volume`\n\n"
                           "Types: whale, volume, crash, drain, wall, coordinated,\n"
                           "price, spread, momentum")
        return
    action, key = args[0], mapping[args[1]]
    if chat_id not in subscribers:
        subscribers[chat_id] = set()
    if action == "on":
        subscribers[chat_id].add(key)
        await send(chat_id, f"✅ `{key}` ON")
    else:
        subscribers[chat_id].discard(key)
        await send(chat_id, f"🔕 `{key}` OFF")

async def cmd_help(chat_id: str):
    await send(chat_id, (
        "🤖 *Commands*\n\n"
        "/start — subscribe\n"
        "/stop — unsubscribe\n"
        "/status — your settings\n"
        "/setalert 5000 — whale threshold\n"
        "/sensitivity strict|normal|relaxed\n"
        "/toggle on|off TYPE\n\n"
        "*Alert Types:*\n"
        "whale, volume, crash, drain,\n"
        "wall, coordinated, price,\n"
        "spread, momentum"
    ))

# ── Telegram polling ──────────────────────────────────────────────────────────
async def poll_telegram():
    offset = 0
    log.info("Telegram polling started")
    async with httpx.AsyncClient(timeout=35) as client:
        while True:
            try:
                r = await client.get(f"{TELEGRAM_API}/getUpdates", params={
                    "offset": offset, "timeout": 30, "allowed_updates": ["message"]
                })
                data = r.json()
                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    asyncio.create_task(handle_update(update))
            except Exception as e:
                log.warning(f"Poll error: {e}")
                await asyncio.sleep(3)

async def handle_update(update: dict):
    msg = update.get("message", {})
    if not msg or not msg.get("text", "").startswith("/"):
        return
    chat_id = str(msg["chat"]["id"])
    username = msg.get("from", {}).get("username", "unknown")
    text = msg.get("text", "").strip()
    parts = text.split()
    command = parts[0].lower().split("@")[0]
    args = parts[1:]

    if command == "/start":
        await cmd_start(chat_id, username)
    elif command == "/stop":
        await cmd_stop(chat_id)
    elif command == "/status":
        await cmd_status(chat_id)
    elif command == "/setalert":
        await cmd_setalert(chat_id, args)
    elif command == "/sensitivity":
        await cmd_sensitivity(chat_id, args)
    elif command == "/toggle":
        await cmd_toggle_alerts(chat_id, args)
    elif command == "/help":
        await cmd_help(chat_id)

# ── Anomaly detection engine ──────────────────────────────────────────────────
async def process_trade(market_id: str, trade: dict):
    size = float(trade.get("size", 0))
    price = float(trade.get("price", 0))
    usd = size * price
    slug = trade.get("market_slug") or market_id[:16]
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    wallet = trade.get("maker_address") or trade.get("taker_address") or ""

    # Store trade
    now = time.monotonic()
    market_trades[market_id].append((now, usd, size, price, wallet))
    market_volumes[market_id].append((now, usd))

    # ── 1. WHALE DETECTION ────────────────────────────────────────────────
    for chat_id, types in list(subscribers.items()):
        if "whale" not in types:
            continue
        threshold = user_thresholds.get(chat_id, WHALE_THRESHOLD_USD)
        sensitivity = user_sensitivity.get(chat_id, "normal")
        
        # Adjust for sensitivity
        if sensitivity == "strict":
            threshold *= 0.5
        elif sensitivity == "relaxed":
            threshold *= 1.5
        
        if usd >= threshold:
            severity = "🚨 CRITICAL" if usd >= threshold * 5 else "🐳 WHALE"
            msg = (
                f"{severity} *Whale Trade*\n\n"
                f"Market: `{slug}`\n"
                f"Size: `${usd:,.0f}` ({size:,.0f} @ ${price:.4f})\n"
                f"{'Wallet: `' + wallet[:10] + '…`' if wallet else ''}\n"
                f"Time: `{ts}`"
            )
            await send(chat_id, msg)

    # ── 2. VOLUME SPIKE ───────────────────────────────────────────────────
    recent_vol = [v for t, v in market_volumes[market_id] if now - t <= 10]
    baseline_vol = [v for t, v in market_volumes[market_id] if 10 < now - t <= 60]
    if baseline_vol and recent_vol:
        avg_baseline = mean(baseline_vol)
        avg_recent = mean(recent_vol)
        if avg_baseline > 0 and avg_recent >= avg_baseline * VOLUME_SPIKE_MULT:
            msg = (
                f"📈 *Volume Spike*\n\n"
                f"Market: `{slug}`\n"
                f"Recent: `${avg_recent:,.0f}/s` | Baseline: `${avg_baseline:,.0f}/s`\n"
                f"Ratio: `{avg_recent/avg_baseline:.1f}×`\n"
                f"Time: `{ts}`"
            )
            await broadcast("volume_spike", msg)

    # ── 3. COORDINATED TRADES (5+ trades in 2 seconds) ──────────────────
    recent_trades = [t for t in market_trades[market_id] if now - t[0] <= 2]
    if len(recent_trades) >= COORDINATED_THRESHOLD:
        total_usd = sum(t[1] for t in recent_trades)
        msg = (
            f"🔄 *Coordinated Trades Detected*\n\n"
            f"Market: `{slug}`\n"
            f"Trades: `{len(recent_trades)}` in 2 seconds\n"
            f"Total volume: `${total_usd:,.0f}`\n"
            f"Time: `{ts}`"
        )
        await broadcast("coordinated_trades", msg)

async def process_price_update(market_id: str, update: dict):
    outcome_id = update.get("asset_id") or update.get("outcome_id", "unknown")
    price = float(update.get("price", 0))
    slug = update.get("market_slug") or market_id[:16]
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")

    if outcome_id not in market_prices[market_id]:
        market_prices[market_id][outcome_id] = price
        price_history[market_id].append((time.monotonic(), price))
        return

    old_price = market_prices[market_id][outcome_id]
    market_prices[market_id][outcome_id] = price
    now = time.monotonic()
    price_history[market_id].append((now, price))

    # ── 4. PRICE DIVERGENCE (>5% instant) ─────────────────────────────────
    if old_price > 0:
        pct_change = abs(price - old_price) / old_price
        if pct_change >= 0.05:
            msg = (
                f"⚡ *Price Divergence*\n\n"
                f"Market: `{slug}`\n"
                f"Outcome: `{outcome_id[:12]}…`\n"
                f"Move: `{pct_change*100:.1f}%` (${old_price:.4f} → ${price:.4f})\n"
                f"Time: `{ts}`"
            )
            await broadcast("price_divergence", msg)

    # ── 5. FLASH CRASH (>10% in 5 seconds) ────────────────────────────────
    recent_prices = [p for t, p in price_history[market_id] if now - t <= 5]
    if len(recent_prices) >= 3:
        max_p = max(recent_prices)
        min_p = min(recent_prices)
        if max_p > 0:
            crash_pct = (max_p - min_p) / max_p * 100
            if crash_pct >= FLASH_CRASH_PCT:
                msg = (
                    f"💥 *FLASH CRASH DETECTED*\n\n"
                    f"Market: `{slug}`\n"
                    f"Drop: `{crash_pct:.1f}%` in 5 seconds\n"
                    f"Range: `${min_p:.4f} - ${max_p:.4f}`\n"
                    f"Time: `{ts}`"
                )
                await broadcast("flash_crash", msg)

async def process_order_book(market_id: str, book: dict):
    slug = book.get("market_slug") or market_id[:16]
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    now = time.monotonic()

    bids = book.get("bids", [])
    asks = book.get("asks", [])

    # Store current book
    market_order_book[market_id] = {"bids": bids, "asks": asks}

    # ── 6. ORDER WALLS ($10k+) ────────────────────────────────────────────
    for side, levels in [("bids", bids), ("asks", asks)]:
        for level in levels:
            size_usd = float(level.get("size", 0)) * float(level.get("price", 1))
            if size_usd >= ORDER_WALL_THRESHOLD:
                msg = (
                    f"🧱 *Order Wall*\n\n"
                    f"Market: `{slug}`\n"
                    f"Side: `{side.upper()}`\n"
                    f"Size: `${size_usd:,.0f}` @ `${float(level.get('price', 0)):.4f}`\n"
                    f"Time: `{ts}`"
                )
                await broadcast("order_wall", msg)
                break

    # ── 7. LIQUIDITY DRAIN (>50% in 10s) ──────────────────────────────────
    # Track book depth over time
    total_bid_depth = sum(float(l.get("size", 0)) for l in bids[:5])
    total_ask_depth = sum(float(l.get("size", 0)) for l in asks[:5])

    if market_id in market_order_book:
        old_book = market_order_book.get(market_id, {})
        old_bids = old_book.get("bids", [])
        old_asks = old_book.get("asks", [])
        old_bid_depth = sum(float(l.get("size", 0)) for l in old_bids[:5])
        old_ask_depth = sum(float(l.get("size", 0)) for l in old_asks[:5])

        if old_bid_depth > 0 and total_bid_depth < old_bid_depth * (1 - LIQUIDITY_DRAIN_PCT / 100):
            msg = (
                f"🚨 *Liquidity Drain (BID SIDE)*\n\n"
                f"Market: `{slug}`\n"
                f"Depth lost: `{(1 - total_bid_depth/old_bid_depth)*100:.1f}%`\n"
                f"Time: `{ts}`"
            )
            await broadcast("liquidity_drain", msg)

        if old_ask_depth > 0 and total_ask_depth < old_ask_depth * (1 - LIQUIDITY_DRAIN_PCT / 100):
            msg = (
                f"🚨 *Liquidity Drain (ASK SIDE)*\n\n"
                f"Market: `{slug}`\n"
                f"Depth lost: `{(1 - total_ask_depth/old_ask_depth)*100:.1f}%`\n"
                f"Time: `{ts}`"
            )
            await broadcast("liquidity_drain", msg)

    # ── 8. BID-ASK COLLAPSE (spread <1%) ──────────────────────────────────
    if bids and asks:
        best_bid = float(bids[0].get("price", 0))
        best_ask = float(asks[0].get("price", 0))
        if best_ask > 0 and best_bid > 0:
            spread_pct = (best_ask - best_bid) / best_ask * 100
            bid_ask_spreads[market_id].append((now, spread_pct))
            if spread_pct < 1.0:
                msg = (
                    f"📊 *Bid-Ask Collapse*\n\n"
                    f"Market: `{slug}`\n"
                    f"Spread: `{spread_pct:.2f}%` (bid ${best_bid:.4f} → ask ${best_ask:.4f})\n"
                    f"Indicates high certainty/low liquidity\n"
                    f"Time: `{ts}`"
                )
                await broadcast("bid_ask_collapse", msg)

    # ── 9. MOMENTUM REVERSAL ──────────────────────────────────────────────
    if market_id in price_history and len(price_history[market_id]) >= 10:
        prices = [p for _, p in list(price_history[market_id])[-10:]]
        if len(prices) >= 5:
            going_up = prices[-1] > prices[-2] > prices[-3]
            going_down = prices[-1] < prices[-2] < prices[-3]
            prev_direction = prices[-5] > prices[-4]
            
            if (going_up and not prev_direction) or (going_down and prev_direction):
                msg = (
                    f"🔀 *Momentum Reversal*\n\n"
                    f"Market: `{slug}`\n"
                    f"Direction changed\n"
                    f"Prices: `{prices[-5]:.4f} → {prices[-1]:.4f}`\n"
                    f"Time: `{ts}`"
                )
                await broadcast("momentum_reversal", msg)

# ── Polymarket WebSocket ──────────────────────────────────────────────────────
async def polymarket_ws():
    backoff = 5
    while True:
        try:
            log.info("Connecting to Polymarket WebSocket…")
            async with websockets.connect(
                POLYMARKET_WS_URL, ping_interval=20, ping_timeout=10, max_size=2**23,
            ) as ws:
                backoff = 5
                log.info("Connected to Polymarket!")
                await ws.send(json.dumps({"auth": {}, "markets": [], "type": "subscribe"}))

                async for raw in ws:
                    try:
                        msgs = json.loads(raw)
                        if not isinstance(msgs, list):
                            msgs = [msgs]
                        for msg in msgs:
                            event = msg.get("event_type") or msg.get("type") or ""
                            market_id = msg.get("market_id") or msg.get("condition_id") or ""
                            if event in ("trade", "orders_matched", "trade_matched"):
                                await process_trade(market_id, msg)
                            elif event in ("price_change", "price_update"):
                                await process_price_update(market_id, msg)
                            elif event in ("order_book", "book_snapshot"):
                                await process_order_book(market_id, msg)
                    except Exception as e:
                        log.warning(f"Dispatch error: {e}")

        except Exception as e:
            log.warning(f"WS error: {e} — reconnecting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    log.info("Polymarket Advanced Monitor v2 starting…")
    await send(OWNER_CHAT_ID, (
        "🟢 *Advanced Monitor Online!*\n\n"
        "Tracking 9 anomaly types:\n"
        "🐳 Whales | 📈 Vol spikes | 💥 Flash crashes\n"
        "🚨 Liquidity drains | 🧱 Order walls\n"
        "🔄 Coordinated trades | ⚡ Price divergences\n"
        "📊 Bid-ask collapse | 🔀 Momentum reversals"
    ))
    subscribers[OWNER_CHAT_ID] = set(ALL_TYPES)
    user_sensitivity[OWNER_CHAT_ID] = "normal"

    await asyncio.gather(poll_telegram(), polymarket_ws())

if __name__ == "__main__":
    asyncio.run(main())
