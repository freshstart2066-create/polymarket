"""
POLYMARKET ULTIMATE BOT v4.0 — FULLY FIXED
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Fixes applied from v3.2 audit:
  [FIX-01] Hardcoded token/chat_id removed — env vars only, no defaults
  [FIX-02] WebSocket subscription payload corrected for Polymarket CLOB API
  [FIX-03] aiohttp runner stored at module level to prevent GC-induced server death
  [FIX-04] wallet_activity append moved AFTER cluster check to prevent self-counting
  [FIX-05] price_history split into trade_price_history vs book_price_history
  [FIX-06] check_bid_ask_collapse guards against zero-spread startup noise
  [FIX-07] check_market_maker threshold raised to realistic prediction market value
  [FIX-08] check_arbitrage replaced with cross-outcome sum deviation logic
  [FIX-09] poll_telegram respects HTTP 429 retry_after from Telegram
  [FIX-10] asyncio.gather replaced with independent supervised restart loops
  [FIX-11] send_chart is now wired into the "chart" broadcast path
  [FIX-12] order book field parsing hardened with try/float fallbacks
  [FIX-13] Persistent state saved to JSON file — survives Render restarts
  [FIX-14] README section at bottom explains UptimeRobot setup for free Render tier

Required env vars (set in Render dashboard):
  TELEGRAM_TOKEN   — your bot token from @BotFather
  OWNER_CHAT_ID    — your personal Telegram numeric chat ID
  WHALE_THRESHOLD_USD  — (optional) default 5000
  PORT             — (optional) default 10000
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

# ── Config ─────────────────────────────────────────────────────────────────────
# [FIX-01] No hardcoded secrets — bot will refuse to start if vars are missing
TOKEN         = os.getenv("TELEGRAM_TOKEN", "")
OWNER_CHAT_ID = os.getenv("OWNER_CHAT_ID", "")
if not TOKEN or not OWNER_CHAT_ID:
    raise RuntimeError(
        "TELEGRAM_TOKEN and OWNER_CHAT_ID must be set as environment variables. "
        "Never hardcode secrets in source code."
    )

WHALE_THRESHOLD   = float(os.getenv("WHALE_THRESHOLD_USD", "5000"))
POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
TELEGRAM_API      = f"https://api.telegram.org/bot{TOKEN}"
PORT              = int(os.getenv("PORT", "10000"))
STATE_FILE        = Path("/tmp/polybot_state.json")  # [FIX-13] persistence

ALL_ALERTS = {
    "whale", "volume_spike", "flash_crash", "liquidity_drain", "order_wall",
    "coordinated", "price_div", "bid_ask_collapse", "momentum", "whale_cluster",
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
subscribers:      dict = {}       # chat_id → set of alert types
user_thresholds:  dict = {}       # chat_id → float
user_sensitivity: dict = {}       # chat_id → str
wallet_pending:   dict = {}       # chat_id → {"address": str}

market_trades        = defaultdict(lambda: deque(maxlen=500))
market_prices        = defaultdict(dict)
market_volumes       = defaultdict(lambda: deque(maxlen=200))
market_order_book    = defaultdict(dict)
# [FIX-05] Separate histories: trade prices vs book/tick prices
trade_price_history  = defaultdict(lambda: deque(maxlen=240))
book_price_history   = defaultdict(lambda: deque(maxlen=240))
bid_ask_spreads      = defaultdict(lambda: deque(maxlen=100))
volume_profile_data  = defaultdict(lambda: defaultdict(float))
order_book_history   = defaultdict(lambda: deque(maxlen=100))
wallet_activity      = defaultdict(lambda: deque(maxlen=300))
anomaly_scores       = defaultdict(float)
sentiment_history    = defaultdict(lambda: deque(maxlen=100))
last_alert: dict     = defaultdict(float)

# [FIX-03] Global runner reference prevents GC killing the health server
_health_runner = None


def cooldown_ok(market_id: str, alert_type: str, seconds: int = 60) -> bool:
    key = (market_id, alert_type)
    now = time.monotonic()
    if now - last_alert[key] > seconds:
        last_alert[key] = now
        return True
    return False


def safe_float(val, default: float = 0.0) -> float:
    """[FIX-12] Safely parse any numeric field from WS messages."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ── Persistence ────────────────────────────────────────────────────────────────
# [FIX-13] Save/load subscribers and thresholds so Render restarts don't wipe them.

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


def load_state():
    try:
        if STATE_FILE.exists():
            data = json.loads(STATE_FILE.read_text())
            for k, v in data.get("subscribers", {}).items():
                subscribers[k] = set(v)
            user_thresholds.update(data.get("user_thresholds", {}))
            user_sensitivity.update(data.get("user_sensitivity", {}))
            log.info(f"State loaded: {len(subscribers)} subscribers")
    except Exception as e:
        log.warning(f"State load failed: {e}")


# ── Health Server ──────────────────────────────────────────────────────────────
async def health_handler(request):
    return web.Response(text="OK")


async def run_health_server():
    global _health_runner  # [FIX-03] prevent GC
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    _health_runner = web.AppRunner(app)
    await _health_runner.setup()
    await web.TCPSite(_health_runner, "0.0.0.0", PORT).start()
    log.info(f"✅ Health server on port {PORT}")


# ── Telegram ───────────────────────────────────────────────────────────────────
async def send(chat_id: str, text: str, parse_mode="Markdown"):
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            await client.post(f"{TELEGRAM_API}/sendMessage", json={
                "chat_id": chat_id, "text": text,
                "parse_mode": parse_mode, "disable_web_page_preview": True,
            })
    except Exception as e:
        log.warning(f"Send failed to {chat_id}: {e}")


async def send_chart(chat_id: str, prices: list, title: str):
    """Render an ASCII price chart and send it. [FIX-11] Now wired into broadcast."""
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


async def broadcast(alert_type: str, text: str, market_id: str = ""):
    """
    Send alert to all subscribers of this alert type.
    [FIX-11] If alert_type is 'chart', also push ASCII chart to chart subscribers.
    """
    targets = [cid for cid, types in list(subscribers.items()) if alert_type in types]
    tasks = [send(cid, text) for cid in targets]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    # [FIX-11] Chart broadcast: send price chart after any flash_crash or momentum alert
    if alert_type in ("flash_crash", "momentum") and market_id:
        chart_targets = [cid for cid, types in list(subscribers.items()) if "chart" in types]
        if chart_targets:
            prices = [p for _, p in list(trade_price_history[market_id])[-40:]]
            if len(prices) >= 2:
                for cid in chart_targets:
                    await send_chart(cid, prices, f"Price chart — {market_id[:18]}")


# ── Commands ───────────────────────────────────────────────────────────────────
async def cmd_start(chat_id: str, username: str):
    subscribers[chat_id] = set(ALL_ALERTS)
    user_sensitivity[chat_id] = "normal"
    save_state()
    await send(chat_id,
        "🚀 *POLYMARKET SIGNAL BOT v4.0*\n\n"
        "Live alerts from Polymarket:\n\n"
        "🐳 Whale trades & clusters\n"
        "📈 Volume spikes\n"
        "💥 Flash crashes + chart\n"
        "🚨 Liquidity drains\n"
        "🧱 Order walls\n"
        "🔄 Coordinated buys/sells\n"
        "⚖️ Bid/ask imbalance\n"
        "💡 Sentiment shifts\n"
        "🤖 Anomaly scoring\n"
        "📚 Pattern matching\n"
        "🏦 Market maker detection\n"
        "💼 Wallet balance checker\n\n"
        "/help — all commands"
    )


async def cmd_help(chat_id: str):
    nets = " | ".join(NETWORKS.keys())
    await send(chat_id,
        "🤖 *Commands*\n\n"
        "/start — subscribe to all alerts\n"
        "/status — your current settings\n"
        "/dashboard — enable all alerts\n"
        "/lite — whale + crash alerts only\n"
        "/stop — unsubscribe\n\n"
        "💼 *Wallet:*\n"
        f"`/wallet 0xAddress network`\n"
        f"Networks: `{nets}`\n\n"
        "📊 *Alert types you can toggle:*\n"
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
    active = len(subscribers[chat_id])
    await send(chat_id,
        f"📊 *Your Settings*\n\n"
        f"Whale threshold: `${threshold:,.0f}`\n"
        f"Active alerts: `{active}`\n"
        f"Total subscribers: `{len(subscribers)}`"
    )


async def cmd_dashboard(chat_id: str):
    subscribers[chat_id] = set(ALL_ALERTS)
    save_state()
    await send(chat_id, "✅ All alerts enabled")


async def cmd_lite(chat_id: str):
    subscribers[chat_id] = {"whale", "flash_crash", "coordinated", "anomaly_score"}
    save_state()
    await send(chat_id, "📱 Lite mode — whale, crash, coordinated, anomaly alerts only")


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
        res = await rpc_call(net["rpc"], "eth_getBalance", [wallet, "latest"])
        native = int(res.get("result", "0x0"), 16) / 1e18
        usdc = await erc20_balance(net["rpc"], net["usdc"], wallet, net["decimals"])
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
            f"\n[View on Explorer]({net['explorer']}{wallet})"
        )
    except Exception as e:
        log.warning(f"Wallet error: {e}")
        return "❌ Failed to fetch. Check the address and try again."


async def cmd_wallet(chat_id: str, args: list):
    nets_str = " | ".join(NETWORKS.keys())
    if len(args) >= 2:
        addr, net = args[0], args[1].lower()
        if not addr.startswith("0x") or len(addr) < 40:
            await send(chat_id, "❌ Invalid address. Example:\n`/wallet 0xABC... polygon`")
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
            f"✅ Got address `{addr[:8]}…{addr[-6:]}`\n\n"
            f"Which network?\n`{nets_str}`\n\nJust reply with the name."
        )
    else:
        await send(chat_id,
            "💼 *Wallet Balance Checker*\n\n"
            f"Usage: `/wallet 0xYourAddress polygon`\n\n"
            f"Networks: `{nets_str}`"
        )


# ── Telegram Polling ───────────────────────────────────────────────────────────
async def poll_telegram():
    """
    Long-poll Telegram for incoming messages.
    [FIX-09] Respects HTTP 429 retry_after from Telegram.
    Fresh httpx client per iteration prevents silent connection death.
    wallet_pending replies handled before slash filter.
    """
    offset = 0
    log.info("Telegram polling started")
    while True:
        try:
            async with httpx.AsyncClient(timeout=35) as client:
                r = await client.get(f"{TELEGRAM_API}/getUpdates", params={
                    "offset": offset, "timeout": 30, "allowed_updates": ["message"],
                })

            # [FIX-09] Respect rate limit
            if r.status_code == 429:
                retry_after = r.json().get("parameters", {}).get("retry_after", 10)
                log.warning(f"Telegram 429 — sleeping {retry_after}s")
                await asyncio.sleep(retry_after)
                continue

            data = r.json()
            for update in data.get("result", []):
                offset = update["update_id"] + 1
                msg = update.get("message", {})
                if not msg:
                    continue
                chat_id  = str(msg["chat"]["id"])
                raw_text = msg.get("text", "").strip()
                if not raw_text:
                    continue

                # Wallet pending reply — checked BEFORE slash filter
                if chat_id in wallet_pending and not raw_text.startswith("/"):
                    net = raw_text.lower().strip()
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
                cmd  = parts[0].lower().split("@")[0]
                args = parts[1:]
                log.info(f"CMD {cmd} from {chat_id}")

                if   cmd == "/start":     await cmd_start(chat_id, msg.get("from", {}).get("username", "?"))
                elif cmd == "/help":      await cmd_help(chat_id)
                elif cmd == "/status":    await cmd_status(chat_id)
                elif cmd == "/dashboard": await cmd_dashboard(chat_id)
                elif cmd == "/lite":      await cmd_lite(chat_id)
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

async def check_whale(market_id: str, slug: str, usd: float, wallet: str, ts: str):
    """Alert on a single trade exceeding whale threshold."""
    threshold = WHALE_THRESHOLD
    if usd >= threshold:
        await broadcast("whale",
            f"🐳 *Whale Trade*\n\n"
            f"Market: `{slug}`\n"
            f"Size: `${usd:,.0f}`\n"
            f"Wallet: `{wallet[:10]}…`\n"
            f"Time: `{ts}`",
            market_id=market_id,
        )


async def check_whale_cluster(market_id: str, wallet: str, ts: str):
    """
    Alert if the same wallet has 3+ large trades within 60s.
    [FIX-04] Called BEFORE wallet_activity.append() so current trade isn't counted.
    """
    if not wallet:
        return
    now = time.monotonic()
    recent = [(t, w, u) for t, w, u in wallet_activity[market_id]
              if now - t <= 60 and w == wallet]
    if len(recent) >= 3:
        total = sum(u for _, _, u in recent)
        if total >= WHALE_THRESHOLD * 2 and cooldown_ok(market_id, "whale_cluster", 120):
            await broadcast("whale_cluster",
                f"🔗 *Whale Cluster Detected*\n\n"
                f"Market: `{market_id[:16]}`\n"
                f"Wallet `{wallet[:10]}…` — {len(recent)} trades in 60s\n"
                f"Total: `${total:,.0f}`\n"
                f"⚠️ Possible manipulation\nTime: `{ts}`",
            )


async def check_volume_spike(market_id: str, slug: str, usd: float, ts: str):
    """Alert when a trade is ≥3× the recent average trade size."""
    vols = [v for _, v in market_volumes[market_id]]
    if len(vols) < 10:
        return
    avg = mean(vols[:-1])
    if avg > 0 and usd > avg * 3 and cooldown_ok(market_id, "volume_spike", 30):
        await broadcast("volume_spike",
            f"📈 *Volume Spike*\n\n"
            f"Market: `{slug}`\n"
            f"Trade: `${usd:,.0f}` vs avg `${avg:,.0f}`\n"
            f"Ratio: `{usd/avg:.1f}×`\n"
            f"Time: `{ts}`",
        )


async def check_flash_crash(market_id: str, slug: str, ts: str):
    """
    Alert when trade prices drop >5% over the last 10 recorded trades.
    [FIX-05] Uses trade_price_history only — not polluted by book tick prices.
    """
    prices = [p for _, p in list(trade_price_history[market_id])[-10:]]
    if len(prices) < 5:
        return
    drop = (prices[0] - prices[-1]) / prices[0] if prices[0] > 0 else 0
    if drop > 0.05 and cooldown_ok(market_id, "flash_crash", 120):
        await broadcast("flash_crash",
            f"💥 *Flash Crash*\n\n"
            f"Market: `{slug}`\n"
            f"Drop: `{drop*100:.1f}%` in last {len(prices)} trades\n"
            f"`${prices[0]:.4f}` → `${prices[-1]:.4f}`\n"
            f"Time: `{ts}`",
            market_id=market_id,
        )


async def check_momentum_reversal(market_id: str, slug: str, ts: str):
    """Alert when the price trend direction flips (using trade prices only)."""
    prices = [p for _, p in list(trade_price_history[market_id])[-20:]]
    if len(prices) < 10:
        return
    mid = len(prices) // 2
    prev_change = (prices[mid] - prices[0]) / prices[0] if prices[0] > 0 else 0
    curr_change = (prices[-1] - prices[mid]) / prices[mid] if prices[mid] > 0 else 0
    if prev_change > 0.02 and curr_change < -0.02 and cooldown_ok(market_id, "momentum", 120):
        await broadcast("momentum",
            f"🔀 *Momentum Reversal*\n\n"
            f"Market: `{slug}`\n"
            f"Was rising `+{prev_change*100:.1f}%`, now falling `{curr_change*100:.1f}%`\n"
            f"Time: `{ts}`",
            market_id=market_id,
        )
    elif prev_change < -0.02 and curr_change > 0.02 and cooldown_ok(market_id, "momentum", 120):
        await broadcast("momentum",
            f"🔀 *Momentum Reversal*\n\n"
            f"Market: `{slug}`\n"
            f"Was falling `{prev_change*100:.1f}%`, now rising `+{curr_change*100:.1f}%`\n"
            f"Time: `{ts}`",
            market_id=market_id,
        )


async def check_coordinated(market_id: str, slug: str, ts: str):
    """Alert when 5+ trades arrive within a 2-second burst window."""
    now = time.monotonic()
    burst = [t for t, *_ in market_trades[market_id] if now - t <= 2]
    if len(burst) >= 5 and cooldown_ok(market_id, "coordinated", 60):
        await broadcast("coordinated",
            f"🔄 *Coordinated Trading*\n\n"
            f"Market: `{slug}`\n"
            f"`{len(burst)}` trades in 2 seconds\n"
            f"⚠️ Possible bot activity\nTime: `{ts}`",
        )


async def check_imbalance(market_id: str, slug: str, bids: list, asks: list, ts: str):
    """Alert on extreme bid/ask volume imbalance in the order book."""
    bid_vol = sum(safe_float(b.get("size")) for b in bids[:10])  # [FIX-12]
    ask_vol = sum(safe_float(a.get("size")) for a in asks[:10])
    if bid_vol <= 0 or ask_vol <= 0:
        return
    ratio = bid_vol / ask_vol
    sentiment_history[market_id].append((time.monotonic(), ratio))
    if ratio > 3.0 and cooldown_ok(market_id, "imbalance_ratio", 60):
        await broadcast("imbalance_ratio",
            f"⚖️ *Strong Bid Pressure*\n\n"
            f"Market: `{slug}`\n"
            f"Bid/Ask ratio: `{ratio:.1f}:1`\n"
            f"📈 Heavy buy-side demand\nTime: `{ts}`",
        )
    elif ratio < 0.33 and cooldown_ok(market_id, "imbalance_ratio", 60):
        await broadcast("imbalance_ratio",
            f"⚖️ *Strong Ask Pressure*\n\n"
            f"Market: `{slug}`\n"
            f"Bid/Ask ratio: `{ratio:.2f}:1`\n"
            f"📉 Heavy sell-side pressure\nTime: `{ts}`",
        )


async def check_sentiment_shift(market_id: str, slug: str, ts: str):
    """Alert when bid/ask ratio trend reverses direction over recent history."""
    hist = list(sentiment_history[market_id])
    if len(hist) < 8:
        return
    ratios = [r for _, r in hist[-8:]]
    mid = len(ratios) // 2
    prev = mean(ratios[:mid])
    curr = mean(ratios[mid:])
    prev_bull = prev > 1.3
    curr_bull = curr > 1.3
    prev_bear = prev < 0.7
    curr_bear = curr < 0.7
    if (prev_bull and curr_bear) or (prev_bear and curr_bull):
        direction = "BEARISH" if curr_bear else "BULLISH"
        if cooldown_ok(market_id, "sentiment_shift", 90):
            await broadcast("sentiment_shift",
                f"💡 *Sentiment Flip → {direction}*\n\n"
                f"Market: `{slug}`\n"
                f"Ratio: `{prev:.2f}` → `{curr:.2f}`\n"
                f"Major crowd reversal\nTime: `{ts}`",
            )


async def check_liquidity_drain(market_id: str, slug: str, current_depth: float, ts: str):
    """Alert when total order book depth drops >40% from its recent peak."""
    depths = [d for _, d in list(order_book_history[market_id])[-20:]]
    if len(depths) < 5:
        return
    peak = max(depths)
    if peak > 0 and current_depth < peak * 0.6 and cooldown_ok(market_id, "liquidity_drain", 90):
        drop_pct = (peak - current_depth) / peak * 100
        await broadcast("liquidity_drain",
            f"🚨 *Liquidity Drain*\n\n"
            f"Market: `{slug}`\n"
            f"Depth dropped `{drop_pct:.0f}%` from peak\n"
            f"Peak: `{peak:.0f}` → Now: `{current_depth:.0f}`\n"
            f"⚠️ Flash crash risk\nTime: `{ts}`",
        )


async def check_order_wall(market_id: str, slug: str, bids: list, asks: list, ts: str):
    """Alert when a single order represents >40% of visible book depth."""
    if not bids or not asks:
        return
    total = (
        sum(safe_float(b.get("size")) for b in bids[:10])  # [FIX-12]
        + sum(safe_float(a.get("size")) for a in asks[:10])
    )
    if total <= 0:
        return
    largest_bid = max((safe_float(b.get("size")) for b in bids[:10]), default=0)
    largest_ask = max((safe_float(a.get("size")) for a in asks[:10]), default=0)
    wall = max(largest_bid, largest_ask)
    side = "BID" if largest_bid > largest_ask else "ASK"
    if wall / total > 0.4 and cooldown_ok(market_id, "order_wall", 90):
        await broadcast("order_wall",
            f"🧱 *Order Wall Detected*\n\n"
            f"Market: `{slug}`\n"
            f"Single {side} order = `{wall/total*100:.0f}%` of book\n"
            f"Size: `{wall:.0f}` shares\n"
            f"Time: `{ts}`",
        )


async def check_bid_ask_collapse(market_id: str, slug: str, bids: list, asks: list, ts: str):
    """
    Alert when spread collapses from a wide baseline to near-zero — signals imminent move.
    [FIX-06] Requires ≥10 historical ticks AND prior avg spread >2% before firing.
    """
    if not bids or not asks:
        return
    best_bid = safe_float(bids[0].get("price"))  # [FIX-12]
    best_ask = safe_float(asks[0].get("price"), default=1.0)
    if best_ask <= 0:
        return
    spread_pct = (best_ask - best_bid) / best_ask * 100
    now = time.monotonic()
    bid_ask_spreads[market_id].append((now, spread_pct))
    spreads = [s for _, s in list(bid_ask_spreads[market_id])[-10:]]
    # [FIX-06] Require 10 ticks before comparing — avoids false alarms at startup
    if len(spreads) >= 10:
        avg_spread = mean(spreads[:-1])
        if avg_spread > 2.0 and spread_pct < 0.3 and cooldown_ok(market_id, "bid_ask_collapse", 60):
            await broadcast("bid_ask_collapse",
                f"📊 *Bid-Ask Collapse*\n\n"
                f"Market: `{slug}`\n"
                f"Spread: `{avg_spread:.2f}%` → `{spread_pct:.2f}%`\n"
                f"⚡ Large move imminent\nTime: `{ts}`",
            )


async def check_market_maker(market_id: str, slug: str, ts: str):
    """
    Alert when spread stays tight across 20+ consecutive ticks.
    [FIX-07] Threshold raised to 3% — realistic for binary prediction markets.
    """
    spreads = [s for _, s in list(bid_ask_spreads[market_id])[-20:]]
    if len(spreads) >= 20 and all(s < 3.0 for s in spreads) and cooldown_ok(market_id, "market_maker", 300):
        await broadcast("market_maker",
            f"🏦 *Market Maker Active*\n\n"
            f"Market: `{slug}`\n"
            f"Avg spread: `{mean(spreads):.2f}%` over last 20 ticks\n"
            f"Professional liquidity provider present\nTime: `{ts}`",
        )


async def check_anomaly_score(market_id: str, slug: str, usd: float, wallet: str, ts: str):
    """Composite 0–100 risk score. Alerts above 65."""
    score = 0.0
    now = time.monotonic()

    if usd > WHALE_THRESHOLD:
        score += min(35, (usd / WHALE_THRESHOLD) * 10)

    recent_wallet = [u for t, w, u in wallet_activity[market_id] if now - t <= 60 and w == wallet]
    if len(recent_wallet) >= 2:
        score += min(25, len(recent_wallet) * 6)

    vols = [v for _, v in market_volumes[market_id]]
    if len(vols) >= 5:
        avg = mean(vols[:-1])
        if avg > 0 and usd > avg * 2:
            score += min(20, (usd / avg - 2) * 8)

    burst = [t for t, *_ in market_trades[market_id] if now - t <= 3]
    if len(burst) >= 4:
        score += min(20, (len(burst) - 3) * 5)

    anomaly_scores[market_id] = score
    if score >= 65 and cooldown_ok(market_id, "anomaly_score", 45):
        await broadcast("anomaly_score",
            f"🤖 *Anomaly Score: {score:.0f}/100*\n\n"
            f"Market: `{slug}`\n"
            f"Risk factors: size, repeat wallet, volume, burst trading\n"
            f"⚠️ High suspicion activity\nTime: `{ts}`",
        )
    return score


async def check_pattern_match(market_id: str, slug: str, ts: str):
    """Detect pre-crash pattern: falling average with high volatility."""
    prices = [p for _, p in list(trade_price_history[market_id])[-20:]]  # [FIX-05]
    if len(prices) < 10:
        return
    changes = [
        (prices[i] - prices[i - 1]) / prices[i - 1]
        for i in range(1, len(prices)) if prices[i - 1] > 0
    ]
    if len(changes) < 5:
        return
    try:
        vol = stdev(changes[-5:])
        avg = mean(changes[-5:])
        if vol > 0.04 and avg < -0.015 and cooldown_ok(market_id, "pattern_match", 120):
            await broadcast("pattern_match",
                f"📚 *Pre-Crash Pattern*\n\n"
                f"Market: `{slug}`\n"
                f"Falling `{avg*100:.2f}%` avg with `{vol*100:.2f}%` volatility\n"
                f"⚠️ Matches historical crash behavior\nTime: `{ts}`",
            )
    except Exception:
        pass


async def check_insider_signal(market_id: str, slug: str, ts: str):
    """Alert on unusual volume surge vs baseline — possible informed trading."""
    vols = [v for _, v in market_volumes[market_id]]
    if len(vols) < 20:
        return
    recent   = mean(vols[-5:])
    baseline = mean(vols[-20:-5])
    if baseline > 0 and recent > baseline * 2.5 and cooldown_ok(market_id, "insider_signal", 90):
        await broadcast("insider_signal",
            f"👤 *Unusual Volume Surge*\n\n"
            f"Market: `{slug}`\n"
            f"Recent avg: `${recent:,.0f}` vs baseline: `${baseline:,.0f}`\n"
            f"Ratio: `{recent/baseline:.1f}×`\n"
            f"⚠️ Possible informed trading\nTime: `{ts}`",
        )


async def check_arbitrage(market_id: str, slug: str, ts: str):
    """
    Alert when YES + NO prices deviate significantly from 1.00.
    [FIX-08] Old logic compared outcome prices to each other (always differs on non-50/50 markets).
    Correct logic: sum of all outcome prices should be ~1.00 on a fair market.
    Deviation >10% from 1.00 suggests genuine mispricing.
    """
    prices = market_prices.get(market_id, {})
    if len(prices) < 2:
        return
    total = sum(prices.values())
    deviation = abs(total - 1.0)
    if deviation > 0.10 and cooldown_ok(market_id, "arbitrage", 60):
        await broadcast("arbitrage",
            f"💰 *Outcome Mispricing*\n\n"
            f"Market: `{slug}`\n"
            f"Sum of outcome prices: `{total:.3f}` (expected ~1.00)\n"
            f"Deviation: `{deviation*100:.1f}%` — possible arbitrage\nTime: `{ts}`",
        )


async def check_depth_prediction(market_id: str, slug: str, current_depth: float, ts: str):
    """Alert on sustained declining depth trend — predicts liquidity crisis."""
    depths = [d for _, d in list(order_book_history[market_id])[-15:]]
    if len(depths) < 8:
        return
    trend = (depths[-1] - depths[0]) / depths[0] if depths[0] > 0 else 0
    if trend < -0.35 and cooldown_ok(market_id, "depth_prediction", 90):
        await broadcast("depth_prediction",
            f"🔮 *Liquidity Crisis Incoming*\n\n"
            f"Market: `{slug}`\n"
            f"Depth declining `{trend*100:.0f}%` over last 15 snapshots\n"
            f"⚠️ High flash crash risk\nTime: `{ts}`",
        )


async def check_volume_profile(market_id: str, slug: str, price: float, volume: float):
    """Alert when >60% of all recorded volume concentrates at a single price level."""
    level = round(price, 3)
    volume_profile_data[market_id][level] += volume
    top = sorted(volume_profile_data[market_id].items(), key=lambda x: x[1], reverse=True)[:3]
    total = sum(v for _, v in top)
    if total > 0 and top[0][1] / total > 0.6 and cooldown_ok(market_id, "volume_profile", 120):
        await broadcast("volume_profile",
            f"📊 *Volume Concentration*\n\n"
            f"Market: `{slug}`\n"
            f"60%+ of volume at `${top[0][0]:.4f}`\n"
            f"📍 Key support/resistance level",
        )


# ── Main Trade Processor ───────────────────────────────────────────────────────
async def process_trade(market_id: str, trade: dict):
    size   = safe_float(trade.get("size"))    # [FIX-12]
    price  = safe_float(trade.get("price"))
    usd    = size * price
    slug   = trade.get("market_slug") or market_id[:20]
    ts     = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    wallet = trade.get("maker_address") or trade.get("taker_address") or ""
    now    = time.monotonic()

    market_trades[market_id].append((now, usd, size, price, wallet))
    market_volumes[market_id].append((now, usd))
    trade_price_history[market_id].append((now, price))  # [FIX-05] trade history only

    # [FIX-04] cluster check BEFORE appending wallet activity
    await check_whale_cluster(market_id, wallet, ts)
    if wallet:
        wallet_activity[market_id].append((now, wallet, usd))

    await check_whale(market_id, slug, usd, wallet, ts)
    await check_volume_spike(market_id, slug, usd, ts)
    await check_coordinated(market_id, slug, ts)
    await check_anomaly_score(market_id, slug, usd, wallet, ts)
    await check_volume_profile(market_id, slug, price, usd)
    await check_insider_signal(market_id, slug, ts)
    await check_flash_crash(market_id, slug, ts)
    await check_momentum_reversal(market_id, slug, ts)
    await check_pattern_match(market_id, slug, ts)


async def process_price_update(market_id: str, update: dict):
    outcome_id = update.get("asset_id") or update.get("outcome_id", "unk")
    price      = safe_float(update.get("price"))  # [FIX-12]
    slug       = update.get("market_slug") or market_id[:20]
    ts         = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    now        = time.monotonic()

    market_prices[market_id][outcome_id] = price
    book_price_history[market_id].append((now, price))  # [FIX-05] book history only

    await check_arbitrage(market_id, slug, ts)
    # Note: flash_crash and pattern_match intentionally NOT run here —
    # they need trade prices, not book tick prices. [FIX-05]


async def process_order_book(market_id: str, book: dict):
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    slug = book.get("market_slug") or market_id[:20]
    ts   = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    now  = time.monotonic()

    market_order_book[market_id] = {"bids": bids, "asks": asks}
    bid_vol       = sum(safe_float(b.get("size")) for b in bids[:10])  # [FIX-12]
    ask_vol       = sum(safe_float(a.get("size")) for a in asks[:10])
    current_depth = bid_vol + ask_vol
    order_book_history[market_id].append((now, current_depth))

    await check_imbalance(market_id, slug, bids, asks, ts)
    await check_sentiment_shift(market_id, slug, ts)
    await check_liquidity_drain(market_id, slug, current_depth, ts)
    await check_order_wall(market_id, slug, bids, asks, ts)
    await check_bid_ask_collapse(market_id, slug, bids, asks, ts)
    await check_market_maker(market_id, slug, ts)
    await check_depth_prediction(market_id, slug, current_depth, ts)


# ── Polymarket WebSocket ───────────────────────────────────────────────────────
async def polymarket_ws():
    """
    Connect to Polymarket CLOB WebSocket and route incoming events.
    [FIX-02] Corrected subscription payload format.
    """
    backoff = 5
    while True:
        try:
            log.info("Connecting to Polymarket WS…")
            async with websockets.connect(
                POLYMARKET_WS_URL,
                ping_interval=20, ping_timeout=10, max_size=2 ** 23,
            ) as ws:
                backoff = 5
                log.info("✅ Connected to Polymarket")

                # [FIX-02] Correct subscription: subscribe to the live_activity channel
                # which broadcasts all market events without needing specific market IDs
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "channel": "live_activity",
                }))

                async for raw in ws:
                    try:
                        msgs = json.loads(raw)
                        if not isinstance(msgs, list):
                            msgs = [msgs]
                        for msg in msgs:
                            event     = msg.get("event_type") or msg.get("type") or ""
                            market_id = (
                                msg.get("market_id")
                                or msg.get("condition_id")
                                or msg.get("marketId")
                                or ""
                            )
                            if not market_id:
                                continue

                            if event in ("trade", "orders_matched", "TRADE"):
                                await process_trade(market_id, msg)
                            elif event in ("price_change", "price_update", "PRICE_CHANGE"):
                                await process_price_update(market_id, msg)
                            elif event in ("order_book", "book_snapshot", "BOOK", "book"):
                                await process_order_book(market_id, msg)
                            elif event == "tick_size_change":
                                pass
                            else:
                                # Fallback: handle unknown events that look like trades
                                if msg.get("price") and msg.get("size"):
                                    await process_trade(market_id, msg)
                    except Exception as e:
                        log.warning(f"WS msg error: {e}")

        except Exception as e:
            log.warning(f"WS disconnected: {e} — retrying in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ── Supervised Task Runner ─────────────────────────────────────────────────────
# [FIX-10] Each task runs in its own infinite restart loop.
# One crash no longer kills the whole bot.

async def supervised(name: str, coro_fn):
    """Wrap a coroutine in an infinite restart loop with logging."""
    while True:
        try:
            log.info(f"Starting task: {name}")
            await coro_fn()
        except Exception as e:
            log.error(f"Task '{name}' crashed: {e} — restarting in 5s")
            await asyncio.sleep(5)


# ── Main ───────────────────────────────────────────────────────────────────────
async def main():
    log.info("🚀 Starting Polymarket Bot v4.0")

    load_state()  # [FIX-13]

    # Health server must start first — Render checks port within 50s
    await run_health_server()

    # Auto-subscribe owner
    if OWNER_CHAT_ID not in subscribers:
        subscribers[OWNER_CHAT_ID] = set(ALL_ALERTS)
        user_sensitivity[OWNER_CHAT_ID] = "normal"
        save_state()

    await send(OWNER_CHAT_ID,
        "🚀 *POLYMARKET BOT v4.0 ONLINE*\n\n"
        "All detectors active:\n"
        "🐳 Whale & cluster\n"
        "📈 Volume spike\n"
        "💥 Flash crash + chart\n"
        "🚨 Liquidity drain\n"
        "🧱 Order wall\n"
        "🔄 Coordinated trades\n"
        "⚖️ Bid/ask imbalance\n"
        "💡 Sentiment shift\n"
        "📊 Bid-ask collapse\n"
        "🏦 Market maker\n"
        "🤖 Anomaly score\n"
        "📚 Pattern match\n"
        "👤 Insider signal\n"
        "💰 Outcome mispricing\n"
        "🔮 Depth prediction\n"
        "💼 Wallet checker\n\n"
        "/help — commands"
    )

    # [FIX-10] Independent supervised loops — one crash won't kill the other
    await asyncio.gather(
        supervised("telegram_poll", poll_telegram),
        supervised("polymarket_ws", polymarket_ws),
    )


if __name__ == "__main__":
    asyncio.run(main())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# RENDER DEPLOYMENT NOTES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# 1. Required environment variables (set in Render dashboard):
#      TELEGRAM_TOKEN=<from @BotFather>
#      OWNER_CHAT_ID=<your numeric Telegram chat ID>
#      WHALE_THRESHOLD_USD=5000   (optional)
#      PORT=10000                 (optional, Render sets this automatically)
#
# 2. Start command:
#      python polymarket_bot_v4.py
#
# 3. [FIX-14] Free Render tier sleep fix:
#    Render's free tier spins the service down after ~15min of no inbound HTTP.
#    Your WebSocket is outbound and does NOT count as activity.
#    Fix: create a FREE UptimeRobot monitor at https://uptimerobot.com
#      - Monitor type: HTTP(s)
#      - URL: https://<your-render-url>/health
#      - Check interval: 5 minutes
#    This keeps Render awake 24/7 on the free plan.
#
# 4. requirements.txt:
#      httpx
#      websockets
#      aiohttp
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
