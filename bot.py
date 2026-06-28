"""
Polymarket Whale Alert — Telegram Bot
Public bot: anyone can /start and subscribe to alerts.
Connects to Polymarket WebSocket and pushes alerts to all subscribers.
"""

import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

import httpx
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("polybot")

# ── Config ────────────────────────────────────────────────────────────────────
TOKEN                = os.getenv("TELEGRAM_TOKEN", "8913424520:AAEfpVp07jdokzhXlAgZjiQxR7bCvWu4qAg")
OWNER_CHAT_ID        = os.getenv("OWNER_CHAT_ID",  "8316516258")
WHALE_THRESHOLD_USD  = float(os.getenv("WHALE_THRESHOLD_USD", "5000"))
VOLUME_SPIKE_MULT    = float(os.getenv("VOLUME_SPIKE_MULT", "3.0"))
ORDER_WALL_THRESHOLD = float(os.getenv("ORDER_WALL_THRESHOLD", "10000"))
POLYMARKET_WS_URL    = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

TELEGRAM_API         = f"https://api.telegram.org/bot{TOKEN}"

# ── Subscriber store (in-memory) ──────────────────────────────────────────────
subscribers: dict[str, set] = {}
user_thresholds: dict[str, float] = {}

ALL_TYPES = {"whale", "volume_spike", "price_divergence", "order_wall"}

# ── Market state ──────────────────────────────────────────────────────────────
volume_windows: dict = defaultdict(lambda: deque(maxlen=60))
last_prices: dict    = defaultdict(dict)

# ── Telegram helpers ──────────────────────────────────────────────────────────
async def send(chat_id: str, text: str, parse_mode="Markdown"):
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            await client.post(f"{TELEGRAM_API}/sendMessage", json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            })
    except Exception as e:
        log.warning(f"Send failed to {chat_id}: {e}")

async def broadcast(alert_type: str, text: str):
    """Send alert to all subscribers who want this alert type."""
    tasks = []
    for chat_id, types in list(subscribers.items()):
        if alert_type in types:
            tasks.append(send(chat_id, text))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

# ── Command handlers ──────────────────────────────────────────────────────────
async def cmd_start(chat_id: str, username: str):
    subscribers[chat_id] = set(ALL_TYPES)
    await send(chat_id, (
        "🔭 *Welcome to Polymarket Whale Monitor!*\n\n"
        "You're now subscribed to *all* alert types:\n"
        "🐳 Whale trades\n"
        "📈 Volume spikes\n"
        "⚡ Price divergences\n"
        "🧱 Order walls\n\n"
        "*Commands:*\n"
        "/status — your current settings\n"
        "/setalert 10000 — set your whale threshold (USD)\n"
        "/alerts on|off whale|volume|price|wall — toggle alert types\n"
        "/stop — unsubscribe from all alerts\n"
        "/help — show this menu"
    ))
    log.info(f"New subscriber: {chat_id} ({username})")

async def cmd_stop(chat_id: str):
    subscribers.pop(chat_id, None)
    await send(chat_id, "👋 You've been unsubscribed. Send /start anytime to rejoin.")

async def cmd_status(chat_id: str):
    if chat_id not in subscribers:
        await send(chat_id, "You're not subscribed. Send /start to begin.")
        return
    types = subscribers[chat_id]
    threshold = user_thresholds.get(chat_id, WHALE_THRESHOLD_USD)
    active = ", ".join(sorted(types)) if types else "none"
    await send(chat_id, (
        f"📊 *Your Settings*\n\n"
        f"Whale threshold: `${threshold:,.0f}`\n"
        f"Active alerts: `{active}`\n"
        f"Total subscribers: `{len(subscribers)}`"
    ))

async def cmd_setalert(chat_id: str, args: list):
    if not args or not args[0].replace(".", "").isdigit():
        await send(chat_id, "Usage: `/setalert 10000` — sets your whale threshold to $10,000")
        return
    amount = float(args[0])
    user_thresholds[chat_id] = amount
    await send(chat_id, f"✅ Your whale threshold is now set to `${amount:,.0f}`")

async def cmd_alerts(chat_id: str, args: list):
    """Toggle specific alert types on or off."""
    mapping = {"whale": "whale", "volume": "volume_spike", "price": "price_divergence", "wall": "order_wall"}
    if len(args) < 2 or args[0] not in ("on", "off") or args[1] not in mapping:
        await send(chat_id, (
            "Usage: `/alerts on whale` or `/alerts off volume`\n\n"
            "Types: `whale` `volume` `price` `wall`"
        ))
        return
    action    = args[0]
    alert_key = mapping[args[1]]
    if chat_id not in subscribers:
        subscribers[chat_id] = set()
    if action == "on":
        subscribers[chat_id].add(alert_key)
        await send(chat_id, f"✅ `{alert_key}` alerts turned ON")
    else:
        subscribers[chat_id].discard(alert_key)
        await send(chat_id, f"🔕 `{alert_key}` alerts turned OFF")

async def cmd_help(chat_id: str):
    await send(chat_id, (
        "🤖 *Polymarket Whale Monitor — Help*\n\n"
        "/start — subscribe to all alerts\n"
        "/stop — unsubscribe\n"
        "/status — view your settings\n"
        "/setalert 5000 — set whale threshold in USD\n"
        "/alerts on whale — turn on whale alerts\n"
        "/alerts off volume — turn off volume spike alerts\n\n"
        "*Alert types:*\n"
        "🐳 `whale` — large single trades\n"
        "📈 `volume` — sudden volume surges\n"
        "⚡ `price` — rapid price moves\n"
        "🧱 `wall` — large order book walls\n\n"
        "Built on Polymarket real-time CLOB data."
    ))

# ── Telegram update polling ───────────────────────────────────────────────────
async def poll_telegram():
    """Long-poll Telegram for incoming messages."""
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
    if not msg:
        return
    chat_id  = str(msg["chat"]["id"])
    username = msg.get("from", {}).get("username", "unknown")
    text     = msg.get("text", "").strip()

    if not text.startswith("/"):
        return

    parts   = text.split()
    command = parts[0].lower().split("@")[0]
    args    = parts[1:]

    if command == "/start":
        await cmd_start(chat_id, username)
    elif command == "/stop":
        await cmd_stop(chat_id)
    elif command == "/status":
        await cmd_status(chat_id)
    elif command == "/setalert":
        await cmd_setalert(chat_id, args)
    elif command == "/alerts":
        await cmd_alerts(chat_id, args)
    elif command == "/help":
        await cmd_help(chat_id)

# ── Anomaly detection ─────────────────────────────────────────────────────────
async def process_trade(market_id: str, trade: dict):
    size  = float(trade.get("size", 0))
    price = float(trade.get("price", 0))
    usd   = size * price
    slug  = trade.get("market_slug") or market_id[:16]
    ts    = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    wallet = trade.get("maker_address") or trade.get("taker_address") or ""

    volume_windows[market_id].append((time.monotonic(), usd))

    for chat_id, types in list(subscribers.items()):
        if "whale" not in types:
            continue
        threshold = user_thresholds.get(chat_id, WHALE_THRESHOLD_USD)
        if usd >= threshold:
            severity = "🚨 CRITICAL" if usd >= threshold * 5 else "🐳 WHALE"
            msg = (
                f"{severity} *Trade Detected*\n\n"
                f"Market: `{slug}`\n"
                f"Size: `${usd:,.0f}` ({size:,.0f} shares @ ${price:.4f})\n"
                f"{'Wallet: `' + wallet[:10] + '…' + wallet[-6:] + '`' + chr(10) if wallet else ''}"
                f"Time: `{ts}`"
            )
            await send(chat_id, msg)

    now      = time.monotonic()
    recent   = [v for t, v in volume_windows[market_id] if now - t <= 10]
    baseline = [v for t, v in volume_windows[market_id] if 10 < now - t <= 60]
    if baseline and recent:
        avg_b = sum(baseline) / len(baseline)
        avg_r = sum(recent) / len(recent)
        if avg_b > 0 and avg_r >= avg_b * VOLUME_SPIKE_MULT:
            msg = (
                f"📈 *Volume Spike*\n\n"
                f"Market: `{slug}`\n"
                f"Recent: `${avg_r:,.0f}/trade` vs baseline `${avg_b:,.0f}/trade`\n"
                f"Ratio: `{avg_r/avg_b:.1f}×`\n"
                f"Time: `{ts}`"
            )
            await broadcast("volume_spike", msg)

async def process_price_update(market_id: str, update: dict):
    outcome_id = update.get("asset_id") or update.get("outcome_id", "unknown")
    price      = float(update.get("price", 0))
    slug       = update.get("market_slug") or market_id[:16]

    if outcome_id in last_prices[market_id]:
        old = last_prices[market_id][outcome_id]
        if old > 0:
            pct = abs(price - old) / old
            if pct >= 0.05:
                ts  = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
                msg = (
                    f"⚡ *Price Divergence*\n\n"
                    f"Market: `{slug}`\n"
                    f"Outcome: `{outcome_id[:12]}…`\n"
                    f"Move: `{pct*100:.1f}%` (${old:.4f} → ${price:.4f})\n"
                    f"Time: `{ts}`"
                )
                await broadcast("price_divergence", msg)
    last_prices[market_id][outcome_id] = price

async def process_order_book(market_id: str, book: dict):
    slug = book.get("market_slug") or market_id[:16]
    ts   = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    for side in ("bids", "asks"):
        for level in book.get(side, []):
            size_usd = float(level.get("size", 0)) * float(level.get("price", 1))
            if size_usd >= ORDER_WALL_THRESHOLD:
                msg = (
                    f"🧱 *Order Wall Detected*\n\n"
                    f"Market: `{slug}`\n"
                    f"Side: `{side.upper()}`\n"
                    f"Size: `${size_usd:,.0f}` @ `${float(level.get('price',0)):.4f}`\n"
                    f"Time: `{ts}`"
                )
                await broadcast("order_wall", msg)
                break

# ── Polymarket WebSocket ──────────────────────────────────────────────────────
async def polymarket_ws():
    backoff = 5
    while True:
        try:
            log.info("Connecting to Polymarket WebSocket…")
            async with websockets.connect(
                POLYMARKET_WS_URL,
                ping_interval=20, ping_timeout=10,
                max_size=2**23,
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
                            event     = msg.get("event_type") or msg.get("type") or ""
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
    log.info("Polymarket Whale Bot starting…")

    await send(OWNER_CHAT_ID, (
        "🟢 *Polymarket Monitor is online!*\n\n"
        f"Whale threshold: `${WHALE_THRESHOLD_USD:,.0f}`\n"
        f"Volume spike: `{VOLUME_SPIKE_MULT}×`\n"
        "Listening to Polymarket real-time feed…"
    ))

    subscribers[OWNER_CHAT_ID] = set(ALL_TYPES)

    await asyncio.gather(
        poll_telegram(),
        polymarket_ws(),
    )

if __name__ == "__main__":
    asyncio.run(main())
