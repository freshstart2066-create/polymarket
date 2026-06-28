"""
POLYMARKET ULTIMATE BOT v5.1 — INTEGRATED WATCHLIST & DIGEST
"""
import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, stdev

import httpx
import websockets
from aiohttp import web

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("polybot")

# ── Config & State ─────────────────────────────────────────────────────────────
TOKEN         = os.getenv("TELEGRAM_TOKEN", "")
OWNER_CHAT_ID = os.getenv("OWNER_CHAT_ID", "")
WHALE_THRESHOLD = float(os.getenv("WHALE_THRESHOLD_USD", "5000"))
POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_API = "https://gamma-api.polymarket.com"
TELEGRAM_API = f"https://api.telegram.org/bot{TOKEN}"
PORT = int(os.getenv("PORT", "10000"))
STATE_FILE = Path("/tmp/polybot_state.json")

# New persistent state containers
subscribers = {}
user_thresholds = {}
user_sensitivity = {}
wallet_pending = {}
user_watchlists = defaultdict(set) 
daily_stats = defaultdict(lambda: {"max_trade": 0.0})

# Existing Market Data Structures
market_trades = defaultdict(lambda: deque(maxlen=200))
market_prices = defaultdict(dict)
market_volumes = defaultdict(lambda: deque(maxlen=100))
market_order_book = defaultdict(dict)
trade_price_history = defaultdict(lambda: deque(maxlen=120))
book_price_history = defaultdict(lambda: deque(maxlen=120))
bid_ask_spreads = defaultdict(lambda: deque(maxlen=60))
volume_profile_data = defaultdict(lambda: defaultdict(float))
order_book_history = defaultdict(lambda: deque(maxlen=60))
wallet_activity = defaultdict(lambda: deque(maxlen=150))
anomaly_scores = defaultdict(float)
sentiment_history = defaultdict(lambda: deque(maxlen=60))
last_alert = defaultdict(float)
asset_slug = {}
subscribed_assets = set()

# ── Persistence & Helpers ──────────────────────────────────────────────────────
def save_state():
    try:
        data = {
            "subscribers": {k: list(v) for k, v in subscribers.items()},
            "user_thresholds": user_thresholds,
            "user_sensitivity": user_sensitivity,
            "user_watchlists": {k: list(v) for k, v in user_watchlists.items()},
        }
        STATE_FILE.write_text(json.dumps(data))
    except Exception as e: log.warning(f"State save failed: {e}")

def load_state() -> bool:
    try:
        if STATE_FILE.exists():
            data = json.loads(STATE_FILE.read_text())
            for k, v in data.get("subscribers", {}).items(): subscribers[k] = set(v)
            for k, v in data.get("user_watchlists", {}).items(): user_watchlists[k] = set(v)
            user_thresholds.update(data.get("user_thresholds", {}))
            user_sensitivity.update(data.get("user_sensitivity", {}))
            return True
    except Exception as e: log.warning(f"State load failed: {e}")
    return False

def slug_for(asset_id: str) -> str: return asset_slug.get(asset_id, asset_id[:20])

# ── New Features Logic ────────────────────────────────────────────────────────
async def daily_whale_digest():
    """Background task running every 24h to broadcast whale stats."""
    while True:
        await asyncio.sleep(86400)
        report = "*Daily Whale Digest*\n\n"
        found = False
        for slug, stats in daily_stats.items():
            if stats['max_trade'] > WHALE_THRESHOLD:
                report += f"🐳 {slug}: Top trade `${stats['max_trade']:,.0f}`\n"
                found = True
        if found:
            for cid in list(subscribers.keys()):
                await send(cid, report)
        daily_stats.clear()

async def broadcast(alert_type: str, text: str, asset_id: str = ""):
    slug = slug_for(asset_id).lower()
    for cid, types in list(subscribers.items()):
        # Watchlist filtering
        if user_watchlists[cid] and slug not in user_watchlists[cid]: continue
        if alert_type in types:
            await send(cid, text)
            
    # Chart logic with filter
    if alert_type in ("flash_crash", "momentum") and asset_id:
        for cid in [c for c, t in list(subscribers.items()) if "chart" in t]:
            if user_watchlists[cid] and slug not in user_watchlists[cid]: continue
            prices = [p for _, p in list(trade_price_history[asset_id])[-40:]]
            if len(prices) >= 2: await send_chart(cid, prices, f"Chart — {slug_for(asset_id)}")

# ── (Insert original functions here: fetch_active_asset_ids, poll_telegram, process_*, etc) ──
# Note: Ensure cmd_watch and cmd_unwatch are added to your poll_telegram command list.

async def main():
    load_state()
    await run_health_server()
    asyncio.create_task(daily_whale_digest()) # TASK INITIATED
    # ... rest of your original main logic ...
    await asyncio.gather(
        supervised("telegram_poll", poll_telegram),
        supervised("polymarket_ws", polymarket_ws),
        supervised("self_ping", self_ping_loop),
        supervised("market_refresh", market_refresh_loop),
    )

if __name__ == "__main__":
    asyncio.run(main())
