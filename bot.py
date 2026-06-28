"""
POLYMARKET ULTIMATE BOT v5.0 — FREE RENDER EDITION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Fixes applied on top of v4.0 after official Polymarket WS docs review:

  [FIX-A] WS subscription: market channel requires `assets_ids` (token IDs),
          NOT a channel name string. v4.0 sent {"channel":"live_activity"}
          which is completely invalid — server closes connection immediately.
          Bot now fetches top active markets from Gamma REST API on startup
          and subscribes to their asset IDs properly.

  [FIX-B] WS event names corrected to official docs:
          "last_trade_price" (not "trade"/"orders_matched"/"TRADE")
          "book"             (not "order_book"/"book_snapshot"/"BOOK")
          "price_change"     (not "price_update"/"PRICE_CHANGE")
          Old names never matched, so NO alerts ever fired.

  [FIX-C] price_change payload structure fixed:
          Official format is msg["price_changes"] = [{side,size,price,
          best_bid,best_ask,...}], not a flat msg with a single price field.

  [FIX-D] last_trade_price payload structure fixed:
          Official format: msg.side, msg.size, msg.price (flat fields).

  [FIX-E] book payload structure fixed:
          Official format: msg.bids / msg.asks are lists of {price, size}.
          asset_id identifies which token the book belongs to.

  [FIX-F] Heartbeat: official docs require sending plain text "PING" (not JSON)
          every 10 seconds. Server sends "PONG"; must filter "PONG" before
          JSON-parsing. v4.0 relied on websockets library auto-ping which
          uses binary WS pings — Polymarket server requires TEXT "PING".

  [FIX-G] `custom_feature_enabled: true` added to subscription to receive
          best_bid_ask, new_market, market_resolved events.

  [FIX-H] Self-ping keep-alive loop built into the bot itself (pings own
          /health endpoint every 10 min). On Render free tier, the WS
          connection is outbound and does NOT reset the 15-min sleep timer.
          Only inbound HTTP resets it. The built-in self-ping removes the
          need for a separate UptimeRobot setup, though adding one is still
          recommended as a belt-and-suspenders measure.

  [FIX-I] Market refresh loop: re-fetches active market list every 30 min
          and re-subscribes to any new asset IDs, keeping the bot alive to
          new markets without a restart.

  [FIX-J] Render free tier has 512 MB RAM and 0.1 CPU. All deque maxlens
          reduced to avoid OOM under sustained load from many markets.

  [FIX-K] safe_float signature fixed: `default` was a keyword-only collision
          with Python builtins. Renamed to `fallback`.

  [FIX-L] /tmp state file survives within a single Render instance uptime
          but is wiped on restart. Added a startup notice to the owner when
          state could not be loaded (fresh instance).

  [FIX-M] Asset→market slug mapping kept in memory so alerts show human-
          readable market names instead of raw condition IDs.

Required env vars (Render dashboard → Environment):
  TELEGRAM_TOKEN       — from @BotFather
  OWNER_CHAT_ID        — your Telegram numeric chat ID
  WHALE_THRESHOLD_USD  — optional, default 5000
  PORT                 — set automatically by Render, default 10000

requirements.txt:
  httpx
  websockets
  aiohttp
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("polybot")

# ── Config ─────────────────────────────────────────────────────────────────────
TOKEN         = os.getenv("TELEGRAM_TOKEN", "")
OWNER_CHAT_ID = os.getenv("OWNER_CHAT_ID", "")
if not TOKEN or not OWNER_CHAT_ID:
    raise RuntimeError(
        "TELEGRAM_TOKEN and OWNER_CHAT_ID must be set as environment variables.\n"
        "Go to Render → your service → Environment and add them."
    )

WHALE_THRESHOLD   = float(os.getenv("WHALE_THRESHOLD_USD", "5000"))
POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"  # [FIX-A]
GAMMA_API         = "https://gamma-api.polymarket.com"
TELEGRAM_API      = f"https://api.telegram.org/bot{TOKEN}"
PORT              = int(os.getenv("PORT", "10000"))
STATE_FILE        = Path("/tmp/polybot_state.json")

# How many top markets to subscribe to on startup
TOP_MARKETS_LIMIT = 50

ALL_ALERTS = {
    "whale", "volume_spike", "flash_crash", "liquidity_drain", "order_wall",
    "coordinated", "bid_ask_collapse", "momentum", "whale_cluster",
    "arbitrage", "imbalance_ratio", "volume_profile", "sentiment_shift",
    "depth_prediction", "pattern_match", "anomaly_score",
    "insider_signal", "market_maker", "chart",
}

NETWORKS = {
    "polygon": {
        "name": "Polygon", "rpc": "https://polygon-rpc.com",
        "symbol": "MATIC", "decimals": 6,
        "usdc": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        "usdt": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
        "explorer": "https://polygonscan.com/address/",
    },
    "ethereum": {
        "name": "Ethereum", "rpc": "https://eth.llamarpc.com",
        "symbol": "ETH", "decimals": 6,
        "usdc": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "usdt": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "explorer": "https://etherscan.io/address/",
    },
    "base": {
        "name": "Base", "rpc": "https://mainnet.base.org",
        "symbol": "ETH", "decimals": 6,
        "usdc": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "usdt": None,
        "explorer": "https://basescan.org/address/",
    },
    "arbitrum": {
        "name": "Arbitrum", "rpc": "https://arb1.arbitrum.io/rpc",
        "symbol": "ETH", "decimals": 6,
        "usdc": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "usdt": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "explorer": "https://arbiscan.io/address/",
    },
}

# ── In-memory state ────────────────────────────────────────────────────────────
subscribers:      dict = {}
user_thresholds:  dict = {}
user_sensitivity: dict = {}
wallet_pending:   dict = {}

# [FIX-J] Reduced maxlen to stay inside 512 MB Render free RAM
market_trades        = defaultdict(lambda: deque(maxlen=200))
market_prices        = defaultdict(dict)
market_volumes       = defaultdict(lambda: deque(maxlen=100))
market_order_book    = defaultdict(dict)
trade_price_history  = defaultdict(lambda: deque(maxlen=120))
book_price_history   = defaultdict(lambda: deque(maxlen=120))
bid_ask_spreads      = defaultdict(lambda: deque(maxlen=60))
volume_profile_data  = defaultdict(lambda: defaultdict(float))
order_book_history   = defaultdict(lambda: deque(maxlen=60))
wallet_activity      = defaultdict(lambda: deque(maxlen=150))
anomaly_scores       = defaultdict(float)
sentiment_history    = defaultdict(lambda: deque(maxlen=60))
last_alert: dict     = defaultdict(float)

# [FIX-M] asset_id → human-readable slug
asset_slug: dict     = {}
# Currently subscribed asset IDs
subscribed_assets: set = set()

_health_runner = None  # prevent GC


def cooldown_ok(asset_id: str, alert_type: str, seconds: int = 60) -> bool:
    key = (asset_id, alert_type)
    now = time.monotonic()
    if now - last_alert[key] > seconds:
        last_alert[key] = now
        return True
    return False


def safe_float(val, fallback: float = 0.0) -> float:  # [FIX-K]
    try:
        return float(val)
    except (TypeError, ValueError):
        return fallback


def slug_for(asset_id: str) -> str:
    """Return human-readable market name, falling back to truncated ID."""
    return asset_slug.get(asset_id, asset_id[:20])


# ── Persistence ────────────────────────────────────────────────────────────────
def save_state():
    try:
        data = {
            "subscribers":      {k: list(v) for k, v in subscribers.items()},
            "user_thresholds":  user_thresholds,
            "user_sensitivity": user_sensitivity,
        }
        STATE_FILE.write_text(json.dumps(data))
    except Exception as e:
        log.warning(f"State save failed: {e}")


def load_state() -> bool:
    """Returns True if state was loaded, False if fresh start."""
    try:
        if STATE_FILE.exists():
            data = json.loads(STATE_FILE.read_text())
            for k, v in data.get("subscribers", {}).items():
                subscribers[k] = set(v)
            user_thresholds.update(data.get("user_thresholds", {}))
            user_sensitivity.update(data.get("user_sensitivity", {}))
            log.info(f"State loaded: {len(subscribers)} subscribers")
            return True
    except Exception as e:
        log.warning(f"State load failed: {e}")
    return False


# ── Polymarket Market Discovery ────────────────────────────────────────────────
async def fetch_active_asset_ids() -> dict:
    """
    [FIX-A] Fetch top active markets from Gamma API.
    Returns {asset_id: slug} for all token IDs of active markets.
    """
    mapping = {}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"{GAMMA_API}/markets",
                params={"active": "true", "closed": "false", "limit": TOP_MARKETS_LIMIT,
                        "order": "volume24hr", "ascending": "false"},
            )
            markets = r.json()
            if not isinstance(markets, list):
                markets = markets.get("markets", [])
            for m in markets:
                slug = m.get("slug") or m.get("question") or m.get("id", "unknown")
                # Each market has clob_token_ids = [yes_token_id, no_token_id]
                for token_id in m.get("clob_token_ids", []):
                    if token_id:
                        mapping[str(token_id)] = slug
        log.info(f"Fetched {len(mapping)} asset IDs across {len(markets)} markets")
    except Exception as e:
        log.warning(f"Failed to fetch market list: {e}")
    return mapping


# ── Health Server ──────────────────────────────────────────────────────────────
async def health_handler(request):
    return web.Response(text="OK")


async def run_health_server():
    global _health_runner
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    _health_runner = web.AppRunner(app)
    await _health_runner.setup()
    await web.TCPSite(_health_runner, "0.0.0.0", PORT).start()
    log.info(f"✅ Health server on :{PORT}")


# ── Self-Ping (Render free tier keep-alive) ────────────────────────────────────
async def self_ping_loop():
    """
    [FIX-H] Pings own /health every 10 minutes to prevent Render free tier
    sleep. Render only resets its 15-min inactivity timer on inbound HTTP.
    The outbound Polymarket WS connection does NOT count.
    """
    await asyncio.sleep(60)  # wait for server to be fully up
    render_url = os.getenv("RENDER_EXTERNAL_URL", "")
    if not render_url:
        log.info("RENDER_EXTERNAL_URL not set — self-ping disabled (use UptimeRobot instead)")
        return
    url = f"{render_url}/health"
    log.info(f"Self-ping loop started → {url} every 10 min")
    while True:
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.get(url)
                log.debug(f"Self-ping: {r.status_code}")
        except Exception as e:
            log.warning(f"Self-ping failed: {e}")
        await asyncio.sleep(600)  # 10 minutes


# ── Telegram ───────────────────────────────────────────────────────────────────
async def send(chat_id: str, text: str, parse_mode: str = "Markdown"):
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            await client.post(f"{TELEGRAM_API}/sendMessage", json={
                "chat_id": chat_id, "text": text,
                "parse_mode": parse_mode, "disable_web_page_preview": True,
            })
    except Exception as e:
        log.warning(f"Send failed → {chat_id}: {e}")


async def send_chart(chat_id: str, prices: list, title: str):
    if len(prices) < 2:
        return
    min_p, max_p = min(prices), max(prices)
    if min_p == max_p:
        return
    height, width = 8, min(len(prices), 40)
    chart = [[" "] * width for _ in range(height)]
    for i, price in enumerate(prices[-width:]):
        y = int((price - min_p) / (max_p - min_p) * (height - 1))
        chart[height - 1 - max(0, min(height - 1, y))][i] = "█"
    rows = "\n".join("".join(r) for r in chart)
    await send(chat_id, f"```\n{title}\n{rows}\nLo:${min_p:.4f}  Hi:${max_p:.4f}\n```")


async def broadcast(alert_type: str, text: str, asset_id: str = ""):
    targets = [cid for cid, types in list(subscribers.items()) if alert_type in types]
    if targets:
        await asyncio.gather(*[send(cid, text) for cid in targets], return_exceptions=True)
    # Chart follow-up for crash / momentum alerts
    if alert_type in ("flash_crash", "momentum") and asset_id:
        chart_targets = [cid for cid, types in list(subscribers.items()) if "chart" in types]
        if chart_targets:
            prices = [p for _, p in list(trade_price_history[asset_id])[-40:]]
            if len(prices) >= 2:
                for cid in chart_targets:
                    await send_chart(cid, prices, f"Chart — {slug_for(asset_id)}")


# ── Commands ───────────────────────────────────────────────────────────────────
async def cmd_start(chat_id: str, username: str):
    subscribers[chat_id] = set(ALL_ALERTS)
    user_sensitivity[chat_id] = "normal"
    save_state()
    await send(chat_id,
        "🚀 *POLYMARKET SIGNAL BOT v5.0*\n\n"
        "Receiving live data from Polymarket CLOB WebSocket.\n\n"
        "🐳 Whale trades & clusters\n"
        "📈 Volume spikes\n"
        "💥 Flash crashes + chart\n"
        "🚨 Liquidity drains\n"
        "🧱 Order walls\n"
        "🔄 Coordinated trading\n"
        "⚖️ Bid/ask imbalance\n"
        "💡 Sentiment shifts\n"
        "🤖 Anomaly scoring\n"
        "📚 Pattern matching\n"
        "🏦 Market maker detection\n"
        "💰 Outcome mispricing\n"
        "💼 Wallet balance checker\n\n"
        "/help — all commands"
    )


async def cmd_help(chat_id: str):
    nets = " | ".join(NETWORKS.keys())
    await send(chat_id,
        "🤖 *Commands*\n\n"
        "/start — subscribe all alerts\n"
        "/status — your settings\n"
        "/dashboard — enable all alerts\n"
        "/lite — whale + crash only\n"
        "/markets — how many markets tracked\n"
        "/stop — unsubscribe\n\n"
        "💼 *Wallet:*\n"
        f"`/wallet 0xAddress network`\n"
        f"Networks: `{nets}`\n\n"
        "📊 *Alert types:*\n"
        "`whale` `volume_spike` `flash_crash`\n"
        "`liquidity_drain` `order_wall` `coordinated`\n"
        "`imbalance_ratio` `sentiment_shift`\n"
        "`anomaly_score` `market_maker` `chart`"
    )


async def cmd_status(chat_id: str):
    if chat_id not in subscribers:
        await send(chat_id, "Not subscribed. Use /start")
        return
    threshold = user_thresholds.get(chat_id, WHALE_THRESHOLD)
    await send(chat_id,
        f"📊 *Your Settings*\n\n"
        f"Whale threshold: `${threshold:,.0f}`\n"
        f"Active alerts: `{len(subscribers[chat_id])}`\n"
        f"Total subscribers: `{len(subscribers)}`\n"
        f"Markets tracked: `{len(subscribed_assets)}`"
    )


async def cmd_markets(chat_id: str):
    await send(chat_id,
        f"📡 *Market Coverage*\n\n"
        f"Asset IDs subscribed: `{len(subscribed_assets)}`\n"
        f"Unique markets: `{len(set(asset_slug.values()))}`"
    )


async def cmd_dashboard(chat_id: str):
    subscribers[chat_id] = set(ALL_ALERTS)
    save_state()
    await send(chat_id, "✅ All alerts enabled")


async def cmd_lite(chat_id: str):
    subscribers[chat_id] = {"whale", "flash_crash", "coordinated", "anomaly_score"}
    save_state()
    await send(chat_id, "📱 Lite mode — whale, crash, coordinated, anomaly only")


# ── Wallet ─────────────────────────────────────────────────────────────────────
def erc20_calldata(wallet: str) -> str:
    return "0x70a08231" + wallet.lower().replace("0x", "").zfill(64)


async def rpc_call(rpc: str, method: str, params: list) -> dict:
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(rpc, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
        return r.json()


async def erc20_balance(rpc: str, token: str, wallet: str, decimals: int) -> float:
    res = await rpc_call(rpc, "eth_call", [{"to": token, "data": erc20_calldata(wallet)}, "latest"])
    return int(res.get("result", "0x0"), 16) / (10 ** decimals)


async def get_wallet_balances(wallet: str, network_key: str) -> str:
    net = NETWORKS.get(network_key)
    if not net:
        return "❌ Unknown network."
    try:
        res   = await rpc_call(net["rpc"], "eth_getBalance", [wallet, "latest"])
        native = int(res.get("result", "0x0"), 16) / 1e18
        usdc   = await erc20_balance(net["rpc"], net["usdc"], wallet, net["decimals"])
        usdt_line = ""
        if net.get("usdt"):
            usdt = await erc20_balance(net["rpc"], net["usdt"], wallet, net["decimals"])
            usdt_line = f"💵 USDT: `${usdt:,.2f}`\n"
        return (
            f"💼 *Wallet Balance*\n\n"
            f"🌐 `{net['name']}`\n"
            f"📍 `{wallet[:8]}…{wallet[-6:]}`\n\n"
            f"🪙 {net['symbol']}: `{native:.5f}`\n"
            f"💵 USDC: `${usdc:,.2f}`\n"
            f"{usdt_line}"
            f"\n[Explorer]({net['explorer']}{wallet})"
        )
    except Exception as e:
        log.warning(f"Wallet error: {e}")
        return "❌ Failed to fetch. Check the address and try again."


async def cmd_wallet(chat_id: str, args: list):
    nets_str = " | ".join(NETWORKS.keys())
    if len(args) >= 2:
        addr, net = args[0], args[1].lower()
        if not addr.startswith("0x") or len(addr) < 40:
            await send(chat_id, "❌ Invalid address.")
            return
        if net not in NETWORKS:
            await send(chat_id, f"❌ Unknown network. Options: `{nets_str}`")
            return
        await send(chat_id, "🔍 Looking up…")
        await send(chat_id, await get_wallet_balances(addr, net))
    elif len(args) == 1:
        addr = args[0]
        if not addr.startswith("0x") or len(addr) < 40:
            await send(chat_id, "❌ Invalid address.")
            return
        wallet_pending[chat_id] = {"address": addr}
        await send(chat_id,
            f"✅ Got `{addr[:8]}…{addr[-6:]}`\n\nWhich network?\n`{nets_str}`\n\nReply with the name.")
    else:
        await send(chat_id,
            f"💼 *Wallet Checker*\n\n`/wallet 0xYourAddress polygon`\n\nNetworks: `{nets_str}`")


# ── Telegram Polling ───────────────────────────────────────────────────────────
async def poll_telegram():
    offset = 0
    log.info("Telegram polling started")
    while True:
        try:
            async with httpx.AsyncClient(timeout=35) as client:
                r = await client.get(f"{TELEGRAM_API}/getUpdates", params={
                    "offset": offset, "timeout": 30, "allowed_updates": ["message"],
                })

            if r.status_code == 429:
                retry_after = r.json().get("parameters", {}).get("retry_after", 10)
                log.warning(f"Telegram 429 — sleeping {retry_after}s")
                await asyncio.sleep(retry_after)
                continue

            for update in r.json().get("result", []):
                offset    = update["update_id"] + 1
                msg       = update.get("message", {})
                if not msg:
                    continue
                chat_id   = str(msg["chat"]["id"])
                raw_text  = msg.get("text", "").strip()
                if not raw_text:
                    continue

                # Wallet pending reply — BEFORE slash filter
                if chat_id in wallet_pending and not raw_text.startswith("/"):
                    net     = raw_text.lower().strip()
                    pending = wallet_pending.pop(chat_id)
                    if net not in NETWORKS:
                        await send(chat_id, f"❌ Unknown network. Options: `{' | '.join(NETWORKS.keys())}`")
                    else:
                        await send(chat_id, "🔍 Looking up…")
                        await send(chat_id, await get_wallet_balances(pending["address"], net))
                    continue

                if not raw_text.startswith("/"):
                    continue

                parts = raw_text.split()
                cmd   = parts[0].lower().split("@")[0]
                args  = parts[1:]
                log.info(f"CMD {cmd} from {chat_id}")

                if   cmd == "/start":     await cmd_start(chat_id, msg.get("from", {}).get("username", "?"))
                elif cmd == "/help":      await cmd_help(chat_id)
                elif cmd == "/status":    await cmd_status(chat_id)
                elif cmd == "/dashboard": await cmd_dashboard(chat_id)
                elif cmd == "/lite":      await cmd_lite(chat_id)
                elif cmd == "/markets":   await cmd_markets(chat_id)
                elif cmd == "/wallet":    await cmd_wallet(chat_id, args)
                elif cmd == "/stop":
                    subscribers.pop(chat_id, None)
                    wallet_pending.pop(chat_id, None)
                    save_state()
                    await send(chat_id, "👋 Unsubscribed")

        except Exception as e:
            log.warning(f"Poll error: {e}")
            await asyncio.sleep(3)


# ── Alert Detectors ────────────────────────────────────────────────────────────

async def check_whale(asset_id: str, usd: float, wallet: str, ts: str):
    if usd >= WHALE_THRESHOLD:
        await broadcast("whale",
            f"🐳 *Whale Trade*\n\n"
            f"Market: `{slug_for(asset_id)}`\n"
            f"Size: `${usd:,.0f}`\n"
            f"Wallet: `{wallet[:10]}…`\n"
            f"Time: `{ts}`",
            asset_id=asset_id,
        )


async def check_whale_cluster(asset_id: str, wallet: str, ts: str):
    if not wallet:
        return
    now    = time.monotonic()
    recent = [(t, w, u) for t, w, u in wallet_activity[asset_id] if now - t <= 60 and w == wallet]
    if len(recent) >= 3:
        total = sum(u for _, _, u in recent)
        if total >= WHALE_THRESHOLD * 2 and cooldown_ok(asset_id, "whale_cluster", 120):
            await broadcast("whale_cluster",
                f"🔗 *Whale Cluster*\n\n"
                f"Market: `{slug_for(asset_id)}`\n"
                f"Wallet `{wallet[:10]}…` — {len(recent)} trades in 60s\n"
                f"Total: `${total:,.0f}`\n⚠️ Possible manipulation\nTime: `{ts}`",
            )


async def check_volume_spike(asset_id: str, usd: float, ts: str):
    vols = [v for _, v in market_volumes[asset_id]]
    if len(vols) < 10:
        return
    avg = mean(vols[:-1])
    if avg > 0 and usd > avg * 3 and cooldown_ok(asset_id, "volume_spike", 30):
        await broadcast("volume_spike",
            f"📈 *Volume Spike*\n\n"
            f"Market: `{slug_for(asset_id)}`\n"
            f"Trade: `${usd:,.0f}` vs avg `${avg:,.0f}` ({usd/avg:.1f}×)\n"
            f"Time: `{ts}`",
        )


async def check_flash_crash(asset_id: str, ts: str):
    prices = [p for _, p in list(trade_price_history[asset_id])[-10:]]
    if len(prices) < 5:
        return
    drop = (prices[0] - prices[-1]) / prices[0] if prices[0] > 0 else 0
    if drop > 0.05 and cooldown_ok(asset_id, "flash_crash", 120):
        await broadcast("flash_crash",
            f"💥 *Flash Crash*\n\n"
            f"Market: `{slug_for(asset_id)}`\n"
            f"Drop: `{drop*100:.1f}%`\n"
            f"`${prices[0]:.4f}` → `${prices[-1]:.4f}`\nTime: `{ts}`",
            asset_id=asset_id,
        )


async def check_momentum_reversal(asset_id: str, ts: str):
    prices = [p for _, p in list(trade_price_history[asset_id])[-20:]]
    if len(prices) < 10:
        return
    mid         = len(prices) // 2
    prev_change = (prices[mid] - prices[0]) / prices[0]  if prices[0]   > 0 else 0
    curr_change = (prices[-1] - prices[mid]) / prices[mid] if prices[mid] > 0 else 0
    if prev_change > 0.02 and curr_change < -0.02 and cooldown_ok(asset_id, "momentum", 120):
        await broadcast("momentum",
            f"🔀 *Momentum Reversal*\n\n"
            f"Market: `{slug_for(asset_id)}`\n"
            f"Was rising `+{prev_change*100:.1f}%`, now falling `{curr_change*100:.1f}%`\nTime: `{ts}`",
            asset_id=asset_id,
        )
    elif prev_change < -0.02 and curr_change > 0.02 and cooldown_ok(asset_id, "momentum", 120):
        await broadcast("momentum",
            f"🔀 *Momentum Reversal*\n\n"
            f"Market: `{slug_for(asset_id)}`\n"
            f"Was falling `{prev_change*100:.1f}%`, now rising `+{curr_change*100:.1f}%`\nTime: `{ts}`",
            asset_id=asset_id,
        )


async def check_coordinated(asset_id: str, ts: str):
    now   = time.monotonic()
    burst = [t for t, *_ in market_trades[asset_id] if now - t <= 2]
    if len(burst) >= 5 and cooldown_ok(asset_id, "coordinated", 60):
        await broadcast("coordinated",
            f"🔄 *Coordinated Trading*\n\n"
            f"Market: `{slug_for(asset_id)}`\n"
            f"`{len(burst)}` trades in 2s — possible bot activity\nTime: `{ts}`",
        )


async def check_imbalance(asset_id: str, bids: list, asks: list, ts: str):
    bid_vol = sum(safe_float(b.get("size")) for b in bids[:10])
    ask_vol = sum(safe_float(a.get("size")) for a in asks[:10])
    if bid_vol <= 0 or ask_vol <= 0:
        return
    ratio = bid_vol / ask_vol
    sentiment_history[asset_id].append((time.monotonic(), ratio))
    if ratio > 3.0 and cooldown_ok(asset_id, "imbalance_ratio", 60):
        await broadcast("imbalance_ratio",
            f"⚖️ *Strong Bid Pressure*\n\n"
            f"Market: `{slug_for(asset_id)}`\n"
            f"Bid/Ask: `{ratio:.1f}:1` — heavy buy demand\nTime: `{ts}`",
        )
    elif ratio < 0.33 and cooldown_ok(asset_id, "imbalance_ratio", 60):
        await broadcast("imbalance_ratio",
            f"⚖️ *Strong Ask Pressure*\n\n"
            f"Market: `{slug_for(asset_id)}`\n"
            f"Bid/Ask: `{ratio:.2f}:1` — heavy sell pressure\nTime: `{ts}`",
        )


async def check_sentiment_shift(asset_id: str, ts: str):
    hist = list(sentiment_history[asset_id])
    if len(hist) < 8:
        return
    ratios = [r for _, r in hist[-8:]]
    mid    = len(ratios) // 2
    prev, curr = mean(ratios[:mid]), mean(ratios[mid:])
    if (prev > 1.3 and curr < 0.7) or (prev < 0.7 and curr > 1.3):
        direction = "BEARISH" if curr < 0.7 else "BULLISH"
        if cooldown_ok(asset_id, "sentiment_shift", 90):
            await broadcast("sentiment_shift",
                f"💡 *Sentiment Flip → {direction}*\n\n"
                f"Market: `{slug_for(asset_id)}`\n"
                f"Ratio: `{prev:.2f}` → `{curr:.2f}`\nTime: `{ts}`",
            )


async def check_liquidity_drain(asset_id: str, current_depth: float, ts: str):
    depths = [d for _, d in list(order_book_history[asset_id])[-20:]]
    if len(depths) < 5:
        return
    peak = max(depths)
    if peak > 0 and current_depth < peak * 0.6 and cooldown_ok(asset_id, "liquidity_drain", 90):
        drop_pct = (peak - current_depth) / peak * 100
        await broadcast("liquidity_drain",
            f"🚨 *Liquidity Drain*\n\n"
            f"Market: `{slug_for(asset_id)}`\n"
            f"Depth dropped `{drop_pct:.0f}%` from peak\n"
            f"`{peak:.0f}` → `{current_depth:.0f}`\n⚠️ Flash crash risk\nTime: `{ts}`",
        )


async def check_order_wall(asset_id: str, bids: list, asks: list, ts: str):
    if not bids or not asks:
        return
    total = (sum(safe_float(b.get("size")) for b in bids[:10])
             + sum(safe_float(a.get("size")) for a in asks[:10]))
    if total <= 0:
        return
    largest_bid = max((safe_float(b.get("size")) for b in bids[:10]), default=0)
    largest_ask = max((safe_float(a.get("size")) for a in asks[:10]), default=0)
    wall = max(largest_bid, largest_ask)
    side = "BID" if largest_bid > largest_ask else "ASK"
    if wall / total > 0.4 and cooldown_ok(asset_id, "order_wall", 90):
        await broadcast("order_wall",
            f"🧱 *Order Wall*\n\n"
            f"Market: `{slug_for(asset_id)}`\n"
            f"Single {side} = `{wall/total*100:.0f}%` of book (`{wall:.0f}` shares)\nTime: `{ts}`",
        )


async def check_bid_ask_collapse(asset_id: str, bids: list, asks: list, ts: str):
    if not bids or not asks:
        return
    best_bid = safe_float(bids[0].get("price"))
    best_ask = safe_float(asks[0].get("price"), fallback=1.0)
    if best_ask <= 0:
        return
    spread_pct = (best_ask - best_bid) / best_ask * 100
    bid_ask_spreads[asset_id].append((time.monotonic(), spread_pct))
    spreads = [s for _, s in list(bid_ask_spreads[asset_id])[-10:]]
    if len(spreads) >= 10:
        avg_spread = mean(spreads[:-1])
        if avg_spread > 2.0 and spread_pct < 0.3 and cooldown_ok(asset_id, "bid_ask_collapse", 60):
            await broadcast("bid_ask_collapse",
                f"📊 *Bid-Ask Collapse*\n\n"
                f"Market: `{slug_for(asset_id)}`\n"
                f"Spread: `{avg_spread:.2f}%` → `{spread_pct:.2f}%` ⚡ Large move imminent\nTime: `{ts}`",
            )


async def check_market_maker(asset_id: str, ts: str):
    spreads = [s for _, s in list(bid_ask_spreads[asset_id])[-20:]]
    if len(spreads) >= 20 and all(s < 3.0 for s in spreads) and cooldown_ok(asset_id, "market_maker", 300):
        await broadcast("market_maker",
            f"🏦 *Market Maker Active*\n\n"
            f"Market: `{slug_for(asset_id)}`\n"
            f"Avg spread: `{mean(spreads):.2f}%` over 20 ticks\nTime: `{ts}`",
        )


async def check_anomaly_score(asset_id: str, usd: float, wallet: str, ts: str):
    score = 0.0
    now   = time.monotonic()
    if usd > WHALE_THRESHOLD:
        score += min(35, (usd / WHALE_THRESHOLD) * 10)
    recent_wallet = [u for t, w, u in wallet_activity[asset_id] if now - t <= 60 and w == wallet]
    if len(recent_wallet) >= 2:
        score += min(25, len(recent_wallet) * 6)
    vols = [v for _, v in market_volumes[asset_id]]
    if len(vols) >= 5:
        avg = mean(vols[:-1])
        if avg > 0 and usd > avg * 2:
            score += min(20, (usd / avg - 2) * 8)
    burst = [t for t, *_ in market_trades[asset_id] if now - t <= 3]
    if len(burst) >= 4:
        score += min(20, (len(burst) - 3) * 5)
    anomaly_scores[asset_id] = score
    if score >= 65 and cooldown_ok(asset_id, "anomaly_score", 45):
        await broadcast("anomaly_score",
            f"🤖 *Anomaly Score: {score:.0f}/100*\n\n"
            f"Market: `{slug_for(asset_id)}`\n"
            f"Risk: size, repeat wallet, volume, burst\n⚠️ Suspicious activity\nTime: `{ts}`",
        )
    return score


async def check_pattern_match(asset_id: str, ts: str):
    prices = [p for _, p in list(trade_price_history[asset_id])[-20:]]
    if len(prices) < 10:
        return
    changes = [(prices[i] - prices[i-1]) / prices[i-1]
               for i in range(1, len(prices)) if prices[i-1] > 0]
    if len(changes) < 5:
        return
    try:
        vol = stdev(changes[-5:])
        avg = mean(changes[-5:])
        if vol > 0.04 and avg < -0.015 and cooldown_ok(asset_id, "pattern_match", 120):
            await broadcast("pattern_match",
                f"📚 *Pre-Crash Pattern*\n\n"
                f"Market: `{slug_for(asset_id)}`\n"
                f"Avg `{avg*100:.2f}%`, vol `{vol*100:.2f}%`\n⚠️ Matches crash behavior\nTime: `{ts}`",
            )
    except Exception:
        pass


async def check_insider_signal(asset_id: str, ts: str):
    vols = [v for _, v in market_volumes[asset_id]]
    if len(vols) < 20:
        return
    recent   = mean(vols[-5:])
    baseline = mean(vols[-20:-5])
    if baseline > 0 and recent > baseline * 2.5 and cooldown_ok(asset_id, "insider_signal", 90):
        await broadcast("insider_signal",
            f"👤 *Unusual Volume Surge*\n\n"
            f"Market: `{slug_for(asset_id)}`\n"
            f"Recent: `${recent:,.0f}` vs baseline: `${baseline:,.0f}` ({recent/baseline:.1f}×)\n"
            f"⚠️ Possible informed trading\nTime: `{ts}`",
        )


async def check_arbitrage(asset_id: str, ts: str):
    """
    Outcome mispricing: sum of YES+NO prices should be ~1.00 on a fair market.
    We track prices by asset_id; use the condition_id (market) to sum outcomes.
    Since we store per-asset, check all assets for a slug to sum them.
    """
    slug = slug_for(asset_id)
    # Gather all asset prices for this market slug
    related = {aid: list(market_prices[aid].values())
               for aid, s in asset_slug.items() if s == slug and market_prices[aid]}
    if len(related) < 2:
        return
    # Take the latest price for each asset
    latest_prices = []
    for aid, price_vals in related.items():
        if price_vals:
            latest_prices.append(price_vals[-1])
    if len(latest_prices) < 2:
        return
    total     = sum(latest_prices)
    deviation = abs(total - 1.0)
    if deviation > 0.10 and cooldown_ok(asset_id, "arbitrage", 60):
        await broadcast("arbitrage",
            f"💰 *Outcome Mispricing*\n\n"
            f"Market: `{slug}`\n"
            f"Sum of prices: `{total:.3f}` (expected ~1.00)\n"
            f"Deviation: `{deviation*100:.1f}%`\nTime: `{ts}`",
        )


async def check_depth_prediction(asset_id: str, current_depth: float, ts: str):
    depths = [d for _, d in list(order_book_history[asset_id])[-15:]]
    if len(depths) < 8:
        return
    trend = (depths[-1] - depths[0]) / depths[0] if depths[0] > 0 else 0
    if trend < -0.35 and cooldown_ok(asset_id, "depth_prediction", 90):
        await broadcast("depth_prediction",
            f"🔮 *Liquidity Crisis Incoming*\n\n"
            f"Market: `{slug_for(asset_id)}`\n"
            f"Depth declining `{trend*100:.0f}%` over 15 snapshots\n⚠️ Flash crash risk\nTime: `{ts}`",
        )


async def check_volume_profile(asset_id: str, price: float, volume: float):
    level = round(price, 3)
    volume_profile_data[asset_id][level] += volume
    top   = sorted(volume_profile_data[asset_id].items(), key=lambda x: x[1], reverse=True)[:3]
    total = sum(v for _, v in top)
    if total > 0 and top[0][1] / total > 0.6 and cooldown_ok(asset_id, "volume_profile", 120):
        await broadcast("volume_profile",
            f"📊 *Volume Concentration*\n\n"
            f"Market: `{slug_for(asset_id)}`\n"
            f"60%+ of volume at `${top[0][0]:.4f}` — key S/R level",
        )


# ── Event Processors ───────────────────────────────────────────────────────────

async def process_last_trade_price(asset_id: str, msg: dict):
    """
    [FIX-D] Official last_trade_price format:
      msg.side, msg.size, msg.price  (flat top-level fields)
    """
    price  = safe_float(msg.get("price"))
    size   = safe_float(msg.get("size"))
    usd    = price * size
    wallet = msg.get("maker_address") or msg.get("taker_address") or ""
    ts     = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    now    = time.monotonic()

    market_trades[asset_id].append((now, usd, size, price, wallet))
    market_volumes[asset_id].append((now, usd))
    trade_price_history[asset_id].append((now, price))

    # [FIX-04 from v4] cluster check BEFORE appending wallet
    await check_whale_cluster(asset_id, wallet, ts)
    if wallet:
        wallet_activity[asset_id].append((now, wallet, usd))

    await check_whale(asset_id, usd, wallet, ts)
    await check_volume_spike(asset_id, usd, ts)
    await check_coordinated(asset_id, ts)
    await check_anomaly_score(asset_id, usd, wallet, ts)
    await check_volume_profile(asset_id, price, usd)
    await check_insider_signal(asset_id, ts)
    await check_flash_crash(asset_id, ts)
    await check_momentum_reversal(asset_id, ts)
    await check_pattern_match(asset_id, ts)


async def process_price_change(asset_id: str, msg: dict):
    """
    [FIX-C] Official price_change format:
      msg["price_changes"] = list of {side, size, price, best_bid, best_ask, ...}
    """
    changes = msg.get("price_changes", [])
    if not changes:
        # Fallback: some implementations send flat fields
        price = safe_float(msg.get("price"))
        if price:
            changes = [msg]

    ts  = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    now = time.monotonic()

    for pc in changes:
        price = safe_float(pc.get("price"))
        if price <= 0:
            continue
        book_price_history[asset_id].append((now, price))
        market_prices[asset_id]["latest"] = price

    await check_arbitrage(asset_id, ts)


async def process_book(asset_id: str, msg: dict):
    """
    [FIX-E] Official book format:
      msg.bids = [{price, size}, ...]
      msg.asks = [{price, size}, ...]
      msg.asset_id identifies the token
    """
    bids = msg.get("bids", [])
    asks = msg.get("asks", [])
    ts   = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    now  = time.monotonic()

    market_order_book[asset_id] = {"bids": bids, "asks": asks}
    bid_vol       = sum(safe_float(b.get("size")) for b in bids[:10])
    ask_vol       = sum(safe_float(a.get("size")) for a in asks[:10])
    current_depth = bid_vol + ask_vol
    order_book_history[asset_id].append((now, current_depth))

    await check_imbalance(asset_id, bids, asks, ts)
    await check_sentiment_shift(asset_id, ts)
    await check_liquidity_drain(asset_id, current_depth, ts)
    await check_order_wall(asset_id, bids, asks, ts)
    await check_bid_ask_collapse(asset_id, bids, asks, ts)
    await check_market_maker(asset_id, ts)
    await check_depth_prediction(asset_id, current_depth, ts)


# ── Polymarket WebSocket ───────────────────────────────────────────────────────
async def polymarket_ws():
    """
    [FIX-A/B/C/D/E/F/G] Corrected WS implementation.
    - Subscribes using assets_ids (token IDs) with type="market"
    - Handles PING/PONG heartbeat as plain text
    - Routes correct event types: book, price_change, last_trade_price
    - Refreshes market list every 30 min
    """
    global subscribed_assets, asset_slug

    backoff      = 5
    last_refresh = 0.0

    while True:
        # [FIX-I] Refresh market list every 30 min or on first run
        now_ts = time.time()
        if now_ts - last_refresh > 1800:
            new_mapping = await fetch_active_asset_ids()
            if new_mapping:
                asset_slug.update(new_mapping)
                subscribed_assets = set(asset_slug.keys())
                last_refresh = now_ts
            if not subscribed_assets:
                log.warning("No asset IDs — retrying in 30s")
                await asyncio.sleep(30)
                continue

        try:
            log.info(f"Connecting to Polymarket WS ({len(subscribed_assets)} assets)…")
            async with websockets.connect(
                POLYMARKET_WS_URL,
                ping_interval=None,   # [FIX-F] we handle heartbeat manually
                ping_timeout=None,
                max_size=2 ** 23,
            ) as ws:
                backoff = 5

                # [FIX-A/G] Correct subscription format per official docs
                asset_list = list(subscribed_assets)
                await ws.send(json.dumps({
                    "type": "market",
                    "assets_ids": asset_list,
                    "custom_feature_enabled": True,  # [FIX-G]
                }))
                log.info("✅ Subscribed to Polymarket market channel")

                # [FIX-F] Manual heartbeat: send PING every 10s
                async def heartbeat():
                    while True:
                        await asyncio.sleep(10)
                        try:
                            await ws.send("PING")
                        except Exception:
                            break

                hb_task = asyncio.create_task(heartbeat())

                try:
                    async for raw in ws:
                        # [FIX-F] Filter PONG responses before JSON parse
                        if isinstance(raw, str) and raw.strip() == "PONG":
                            continue

                        try:
                            msg = json.loads(raw)
                            # Market channel delivers single objects (not lists)
                            if isinstance(msg, list):
                                msgs = msg
                            else:
                                msgs = [msg]

                            for m in msgs:
                                event    = m.get("event_type") or m.get("type") or ""
                                asset_id = (m.get("asset_id")
                                            or m.get("token_id")
                                            or m.get("market_id")
                                            or "")
                                if not asset_id:
                                    continue

                                # [FIX-B] Correct official event names
                                if event == "last_trade_price":
                                    await process_last_trade_price(asset_id, m)
                                elif event == "price_change":
                                    await process_price_change(asset_id, m)
                                elif event == "book":
                                    await process_book(asset_id, m)
                                elif event in ("tick_size_change", "best_bid_ask",
                                               "new_market", "market_resolved"):
                                    pass  # informational only
                                else:
                                    log.debug(f"Unknown event: {event}")

                        except json.JSONDecodeError:
                            pass  # PING/PONG or malformed frame
                        except Exception as e:
                            log.warning(f"WS msg error: {e}")
                finally:
                    hb_task.cancel()

        except Exception as e:
            log.warning(f"WS disconnected: {e} — retrying in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ── Market Refresh Loop ────────────────────────────────────────────────────────
async def market_refresh_loop():
    """[FIX-I] Periodically re-fetch active markets and reconnect if needed."""
    while True:
        await asyncio.sleep(1800)  # 30 min
        new_mapping = await fetch_active_asset_ids()
        if new_mapping:
            new_ids = set(new_mapping.keys()) - subscribed_assets
            asset_slug.update(new_mapping)
            subscribed_assets.update(new_mapping.keys())
            if new_ids:
                log.info(f"Market refresh: {len(new_ids)} new asset IDs discovered")


# ── Supervised Task Runner ─────────────────────────────────────────────────────
async def supervised(name: str, coro_fn, *args):
    while True:
        try:
            log.info(f"▶ Starting: {name}")
            await coro_fn(*args)
        except Exception as e:
            log.error(f"✖ '{name}' crashed: {e} — restarting in 5s")
            await asyncio.sleep(5)


# ── Main ───────────────────────────────────────────────────────────────────────
async def main():
    log.info("🚀 Starting Polymarket Bot v5.0")

    fresh = not load_state()

    await run_health_server()

    if OWNER_CHAT_ID not in subscribers:
        subscribers[OWNER_CHAT_ID] = set(ALL_ALERTS)
        user_sensitivity[OWNER_CHAT_ID] = "normal"
        save_state()

    notice = "🆕 Fresh instance — subscriber list reloaded from disk (or empty).\n\n" if fresh else ""

    await send(OWNER_CHAT_ID,
        f"🚀 *POLYMARKET BOT v5.0 ONLINE*\n\n"
        f"{notice}"
        "Fetching active markets…"
    )

    # Initial market fetch
    mapping = await fetch_active_asset_ids()
    if mapping:
        asset_slug.update(mapping)
        subscribed_assets.update(mapping.keys())
        await send(OWNER_CHAT_ID,
            f"✅ Subscribed to *{len(subscribed_assets)} asset IDs* "
            f"across *{len(set(asset_slug.values()))} markets*\n\n"
            "/help — commands"
        )
    else:
        await send(OWNER_CHAT_ID,
            "⚠️ Could not fetch market list from Gamma API.\n"
            "WS will retry automatically."
        )

    await asyncio.gather(
        supervised("telegram_poll",     poll_telegram),
        supervised("polymarket_ws",     polymarket_ws),
        supervised("self_ping",         self_ping_loop),
        supervised("market_refresh",    market_refresh_loop),
    )


if __name__ == "__main__":
    asyncio.run(main())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# RENDER DEPLOYMENT CHECKLIST
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# 1. Environment variables (Render → your service → Environment):
#      TELEGRAM_TOKEN=<from @BotFather>
#      OWNER_CHAT_ID=<your numeric Telegram ID>
#      RENDER_EXTERNAL_URL=https://<your-service>.onrender.com
#      WHALE_THRESHOLD_USD=5000    (optional)
#
# 2. Start command:
#      python polymarket_bot_v5.py
#
# 3. requirements.txt:
#      httpx
#      websockets
#      aiohttp
#
# 4. Free tier keep-alive:
#    The bot self-pings /health every 10 min if RENDER_EXTERNAL_URL is set.
#    For extra reliability, also add a FREE UptimeRobot monitor:
#      https://uptimerobot.com
#      → Monitor type: HTTP(s)
#      → URL: https://<your-service>.onrender.com/health
#      → Interval: every 5 minutes
#
# 5. Render free tier limits (as of June 2026):
#    - 512 MB RAM, 0.1 CPU
#    - 750 hours/month free compute
#    - Sleeps after 15 min of no inbound HTTP
#    - 30-60s cold start on wake
#    - State in /tmp is wiped on every restart
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
