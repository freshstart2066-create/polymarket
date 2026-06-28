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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
TOKEN           = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
WHALE_THRESHOLD = float(os.getenv("WHALE_THRESHOLD", "100000"))
FLASH_CRASH_PCT = float(os.getenv("FLASH_CRASH_PCT", "5.0"))
MOMENTUM_PCT    = float(os.getenv("MOMENTUM_PCT",    "5.0"))
ALERT_COOLDOWN  = float(os.getenv("ALERT_COOLDOWN",  "60"))
LIQUIDITY_SPREAD_PCT = float(os.getenv("LIQUIDITY_SPREAD_PCT", "3.0"))  # % spread alert
ARB_THRESHOLD_PCT    = float(os.getenv("ARB_THRESHOLD_PCT",    "2.0"))  # % cross-venue gap
CORR_DIVERGE_PCT     = float(os.getenv("CORR_DIVERGE_PCT",     "5.0"))  # % correlated divergence
NEWS_SPIKE_PCT       = float(os.getenv("NEWS_SPIKE_PCT",       "10.0")) # % spike = news event
STATE_FILE      = Path(os.getenv("STATE_FILE", "state.json"))
POLYMARKET_WS   = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLYMARKET_API  = "https://clob.polymarket.com"
TELEGRAM_API    = f"https://api.telegram.org/bot{TOKEN}"
PORT            = int(os.getenv("PORT", "10000"))
REDIS_URL       = os.getenv("REDIS_URL", "")
SELF_URL        = os.getenv("RENDER_EXTERNAL_URL", "")

GEMINI_API_KEY  = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL    = "gemini-2.5-flash"
GEMINI_API      = (
    f"https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_MODEL}:generateContent"
)
AI_MAX_TOKENS   = 1024
AI_MAX_HISTORY  = 20   # messages kept per user for conversation memory

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------
# Subscriptions & preferences
subscribers         = {}                            # chat_id -> set of alert types
user_watchlists     = defaultdict(set)              # chat_id -> {slug, ...}
user_thresholds     = {}                            # chat_id -> USD float
user_sensitivity    = {}                            # chat_id -> float multiplier

# Market data
asset_slug          = {}                            # asset_id -> slug string
subscribed_assets   = set()
market_trades       = defaultdict(lambda: deque(maxlen=200))
market_volumes      = defaultdict(lambda: deque(maxlen=100))
trade_price_history = defaultdict(lambda: deque(maxlen=120))
order_books         = defaultdict(dict)             # asset_id -> {bids, asks}
daily_stats         = defaultdict(lambda: {"max_trade": 0.0})
last_alert          = defaultdict(float)

# ── New feature state ──────────────────────────────────────────────────────

# Alerts history: per chat_id, store last 50 alert dicts
alerts_history      = defaultdict(lambda: deque(maxlen=50))  # chat_id -> deque

# Portfolio tracker: chat_id -> {slug -> {"side": "yes"|"no", "qty": float, "avg_price": float}}
portfolios          = defaultdict(dict)

# Whale wallet tracker: chat_id -> set of wallet addresses
watched_wallets     = defaultdict(set)

# Leaderboard: wallet_address -> {"volume": float, "trade_count": int}
wallet_stats        = defaultdict(lambda: {"volume": 0.0, "trade_count": 0})

# Correlated market pairs: list of (asset_id_a, asset_id_b, expected_sum)
# e.g. YES on "will X win" + YES on "will X lose" should sum to ~1.0
correlated_pairs    = []

# Cross-venue prices for arbitrage: asset_id -> {venue -> price}
cross_venue_prices  = defaultdict(dict)

# Pending user input state machine
wallet_pending      = {}   # chat_id -> "wallet" (waiting for address input)
portfolio_pending   = {}   # chat_id -> step dict for multi-step portfolio add

# AI assistant conversation history: chat_id -> list of {role, content} dicts
ai_conversations    = defaultdict(list)

# ---------------------------------------------------------------------------
# Redis / Persistence
# ---------------------------------------------------------------------------
_redis = None

async def _init_redis():
    global _redis
    if not REDIS_URL:
        return
    try:
        import aioredis
        _redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
        log.info("Redis connected.")
    except Exception as e:
        log.warning(f"Redis init failed (falling back to file): {e}")

def _state_dict():
    return {
        "subscribers":      {k: list(v) for k, v in subscribers.items()},
        "user_watchlists":  {k: list(v) for k, v in user_watchlists.items()},
        "user_thresholds":  user_thresholds,
        "user_sensitivity": user_sensitivity,
        "portfolios":       {k: v for k, v in portfolios.items()},
        "watched_wallets":  {k: list(v) for k, v in watched_wallets.items()},
        "correlated_pairs": correlated_pairs,
    }

def _apply_state(data: dict):
    for k, v in data.get("subscribers", {}).items():
        subscribers[k] = set(v)
    for k, v in data.get("user_watchlists", {}).items():
        user_watchlists[k] = set(v)
    user_thresholds.update(data.get("user_thresholds", {}))
    user_sensitivity.update(data.get("user_sensitivity", {}))
    for k, v in data.get("portfolios", {}).items():
        portfolios[k] = v
    for k, v in data.get("watched_wallets", {}).items():
        watched_wallets[k] = set(v)
    correlated_pairs.extend(data.get("correlated_pairs", []))

async def save_state():
    data = json.dumps(_state_dict())
    if _redis:
        try:
            await _redis.set("whale_bot_state", data)
            return
        except Exception as e:
            log.warning(f"Redis save failed: {e}")
    try:
        STATE_FILE.write_text(data)
    except Exception as e:
        log.warning(f"File save failed: {e}")

async def load_state():
    if _redis:
        try:
            raw = await _redis.get("whale_bot_state")
            if raw:
                _apply_state(json.loads(raw))
                log.info("State loaded from Redis.")
                return True
        except Exception as e:
            log.warning(f"Redis load failed: {e}")
    try:
        if STATE_FILE.exists():
            _apply_state(json.loads(STATE_FILE.read_text()))
            log.info("State loaded from file.")
            return True
    except Exception as e:
        log.warning(f"File load failed: {e}")
    return False

# ---------------------------------------------------------------------------
# Telegram helpers
# ---------------------------------------------------------------------------
async def send(chat_id: str | int, text: str):
    """Send message, fall back to plain text if Markdown causes a 400."""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"{TELEGRAM_API}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
                timeout=10,
            )
            if r.status_code == 400:
                # Markdown parse failed — retry as plain text
                await client.post(
                    f"{TELEGRAM_API}/sendMessage",
                    json={"chat_id": chat_id, "text": text},
                    timeout=10,
                )
    except Exception as e:
        log.warning(f"send() failed for {chat_id}: {e}")

async def send_chart(chat_id: str | int, prices: list[float], caption: str):
    if not prices:
        return
    lo, hi = min(prices), max(prices)
    span = hi - lo or 0.0001
    rows = 8
    chart_lines = []
    for row in range(rows, 0, -1):
        threshold = lo + span * row / rows
        line = "".join("█" if p >= threshold else " " for p in prices)
        chart_lines.append(f"`{line}`")
    text = f"*{caption}*\n" + "\n".join(chart_lines) + f"\nLow `{lo:.4f}` · High `{hi:.4f}`"
    await send(chat_id, text)

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def slug_for(asset_id: str) -> str:
    return asset_slug.get(asset_id, asset_id[:8] + "…")

def threshold_for(chat_id: str | int) -> float:
    return float(user_thresholds.get(str(chat_id), WHALE_THRESHOLD))

def cooldown_ok(key: str, seconds: float = ALERT_COOLDOWN) -> bool:
    now = time.time()
    if now - last_alert[key] < seconds:
        return False
    last_alert[key] = now
    return True

def ts_str() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M UTC")

def _record_alert(alert_type: str, text: str, asset_id: str = ""):
    """Store alert in history for all subscribed users."""
    entry = {"type": alert_type, "text": text, "asset": slug_for(asset_id), "ts": ts_str()}
    for cid in list(subscribers.keys()):
        alerts_history[cid].append(entry)

# ---------------------------------------------------------------------------
# Alert broadcast
# ---------------------------------------------------------------------------
async def broadcast(alert_type: str, text: str, asset_id: str = ""):
    _record_alert(alert_type, text, asset_id)
    slug = slug_for(asset_id).lower()
    for cid, types in list(subscribers.items()):
        if user_watchlists[cid] and slug not in user_watchlists[cid]:
            continue
        if alert_type in types:
            await send(cid, text)

    if alert_type in ("flash_crash", "momentum", "news_spike") and asset_id:
        chart_subs = [c for c, t in list(subscribers.items()) if "chart" in t]
        for cid in chart_subs:
            if user_watchlists[cid] and slug not in user_watchlists[cid]:
                continue
            prices = [p for _, p in list(trade_price_history[asset_id])[-40:]]
            if len(prices) >= 2:
                await send_chart(cid, prices, f"Chart — {slug_for(asset_id)}")

# ---------------------------------------------------------------------------
# Trade processing (core + new features woven in)
# ---------------------------------------------------------------------------
async def process_trade(asset_id: str, price: float, size: float, ts: float,
                        maker_addr: str = "", taker_addr: str = ""):
    usd_value = price * size
    slug = slug_for(asset_id)

    # Record history
    trade_price_history[asset_id].append((ts, price))
    market_trades[asset_id].append({"price": price, "size": size, "ts": ts,
                                    "maker": maker_addr, "taker": taker_addr})
    market_volumes[asset_id].append(usd_value)

    # Daily stats
    if usd_value > daily_stats[asset_id]["max_trade"]:
        daily_stats[asset_id]["max_trade"] = usd_value

    # ── Wallet leaderboard ─────────────────────────────────────────────────
    for addr in filter(None, [maker_addr, taker_addr]):
        wallet_stats[addr]["volume"] += usd_value
        wallet_stats[addr]["trade_count"] += 1

    # ── Whale alert ────────────────────────────────────────────────────────
    if usd_value >= WHALE_THRESHOLD and cooldown_ok(f"whale_{asset_id}"):
        msg = (f"🐳 *Whale Trade* · {ts_str()}\n"
               f"Market: *{slug}*\n"
               f"Size: `{size:,.0f}` @ `{price:.4f}`\n"
               f"Value: `${usd_value:,.0f}`")
        if maker_addr:
            msg += f"\nMaker: `{maker_addr[:10]}…`"
        await broadcast("whale", msg, asset_id)

    # ── Whale wallet tracker alerts ────────────────────────────────────────
    for addr in filter(None, [maker_addr, taker_addr]):
        for cid, wallets in list(watched_wallets.items()):
            if addr.lower() in {w.lower() for w in wallets}:
                role = "maker" if addr == maker_addr else "taker"
                await send(cid,
                    f"👀 *Tracked Wallet Active* · {ts_str()}\n"
                    f"`{addr[:12]}…` ({role})\n"
                    f"Market: *{slug}*\n"
                    f"Size `{size:,.0f}` @ `{price:.4f}` = `${usd_value:,.0f}`")

    # ── Price-based alerts (flash crash / momentum / news spike) ───────────
    history = list(trade_price_history[asset_id])
    if len(history) >= 10:
        recent = [p for _, p in history[-10:]]
        pct = (recent[-1] - recent[0]) / recent[0] * 100

        if pct <= -FLASH_CRASH_PCT and cooldown_ok(f"crash_{asset_id}"):
            await broadcast("flash_crash",
                f"🔻 *Flash Crash* · {ts_str()}\n"
                f"Market: *{slug}*\n"
                f"Drop: `{pct:.1f}%` over last 10 trades", asset_id)

        elif pct >= MOMENTUM_PCT and cooldown_ok(f"momentum_{asset_id}"):
            await broadcast("momentum",
                f"🚀 *Momentum Surge* · {ts_str()}\n"
                f"Market: *{slug}*\n"
                f"Rise: `{pct:.1f}%` over last 10 trades", asset_id)

        # News spike = very large single-candle move
        if abs(pct) >= NEWS_SPIKE_PCT and cooldown_ok(f"news_{asset_id}", 300):
            await broadcast("news_spike",
                f"📰 *News Spike Detected* · {ts_str()}\n"
                f"Market: *{slug}*\n"
                f"Move: `{pct:+.1f}%` — possible breaking news", asset_id)

    # ── Correlated market divergence ───────────────────────────────────────
    for pair in correlated_pairs:
        a, b, expected_sum = pair
        if asset_id not in (a, b):
            continue
        hist_a = list(trade_price_history[a])
        hist_b = list(trade_price_history[b])
        if hist_a and hist_b:
            pa = hist_a[-1][1]
            pb = hist_b[-1][1]
            actual_sum = pa + pb
            divergence = abs(actual_sum - expected_sum) / expected_sum * 100
            if divergence >= CORR_DIVERGE_PCT and cooldown_ok(f"corr_{a}_{b}", 120):
                await broadcast("correlated",
                    f"🔗 *Correlated Markets Diverging* · {ts_str()}\n"
                    f"`{slug_for(a)}` = `{pa:.4f}`\n"
                    f"`{slug_for(b)}` = `{pb:.4f}`\n"
                    f"Sum: `{actual_sum:.4f}` vs expected `{expected_sum:.2f}`\n"
                    f"Divergence: `{divergence:.1f}%`")

# ---------------------------------------------------------------------------
# Order book processing
# ---------------------------------------------------------------------------
async def process_book(asset_id: str, bids: list, asks: list):
    """Store order book snapshot and fire liquidity alerts."""
    order_books[asset_id] = {"bids": bids, "asks": asks, "ts": time.time()}

    if not bids or not asks:
        return

    best_bid = float(bids[0]["price"])
    best_ask = float(asks[0]["price"])
    mid      = (best_bid + best_ask) / 2
    spread   = (best_ask - best_bid) / mid * 100

    # Bid depth (top 5 levels)
    bid_depth = sum(float(b.get("size", 0)) for b in bids[:5])
    ask_depth = sum(float(a.get("size", 0)) for a in asks[:5])

    if spread >= LIQUIDITY_SPREAD_PCT and cooldown_ok(f"liq_{asset_id}", 300):
        slug = slug_for(asset_id)
        await broadcast("liquidity",
            f"💧 *Liquidity Alert* · {ts_str()}\n"
            f"Market: *{slug}*\n"
            f"Spread: `{spread:.2f}%` (bid `{best_bid:.4f}` / ask `{best_ask:.4f}`)\n"
            f"Bid depth: `{bid_depth:,.0f}` · Ask depth: `{ask_depth:,.0f}`",
            asset_id)

# ---------------------------------------------------------------------------
# Arbitrage detection (cross-venue)
# ---------------------------------------------------------------------------
async def update_venue_price(asset_id: str, venue: str, price: float):
    cross_venue_prices[asset_id][venue] = price
    prices = cross_venue_prices[asset_id]
    if len(prices) < 2:
        return
    lo_venue = min(prices, key=prices.get)
    hi_venue = max(prices, key=prices.get)
    lo_price = prices[lo_venue]
    hi_price = prices[hi_venue]
    if lo_price <= 0:
        return
    gap_pct = (hi_price - lo_price) / lo_price * 100
    if gap_pct >= ARB_THRESHOLD_PCT and cooldown_ok(f"arb_{asset_id}", 120):
        slug = slug_for(asset_id)
        await broadcast("arbitrage",
            f"🔀 *Arbitrage Opportunity* · {ts_str()}\n"
            f"Market: *{slug}*\n"
            f"Buy on `{lo_venue}` @ `{lo_price:.4f}`\n"
            f"Sell on `{hi_venue}` @ `{hi_price:.4f}`\n"
            f"Gap: `{gap_pct:.2f}%`",
            asset_id)

# ---------------------------------------------------------------------------
# Polymarket WebSocket listener
# ---------------------------------------------------------------------------
async def polymarket_listener():
    while True:
        try:
            async with websockets.connect(POLYMARKET_WS) as ws:
                log.info("Connected to Polymarket WS")
                if subscribed_assets:
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "markets": list(subscribed_assets),
                    }))
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        for event in (msg if isinstance(msg, list) else [msg]):
                            etype = event.get("event_type") or event.get("type", "")
                            aid   = event.get("asset_id", "")
                            if etype == "trade":
                                await process_trade(
                                    asset_id   = aid,
                                    price      = float(event["price"]),
                                    size       = float(event["size"]),
                                    ts         = float(event.get("timestamp", time.time())),
                                    maker_addr = event.get("maker_address", ""),
                                    taker_addr = event.get("taker_address", ""),
                                )
                                # Update Polymarket venue price for arb detection
                                await update_venue_price(aid, "polymarket", float(event["price"]))
                            elif etype in ("book", "price_change"):
                                bids = event.get("bids", [])
                                asks = event.get("asks", [])
                                if bids or asks:
                                    await process_book(aid, bids, asks)
                    except Exception as e:
                        log.warning(f"WS parse error: {e}")
        except Exception as e:
            log.warning(f"WS error: {e} — reconnecting in 5s")
            await asyncio.sleep(5)

# ---------------------------------------------------------------------------
# Background tasks
# ---------------------------------------------------------------------------
async def daily_whale_digest():
    while True:
        await asyncio.sleep(86400)
        report = "*📊 Daily Whale Digest*\n\n"
        found  = False
        for sid, stats in daily_stats.items():
            if stats["max_trade"] > WHALE_THRESHOLD:
                report += f"🐳 {sid}: Top trade `${stats['max_trade']:,.0f}`\n"
                found = True
        if found:
            for cid in list(subscribers.keys()):
                await send(cid, report)
        daily_stats.clear()

# ---------------------------------------------------------------------------
# Commands — original
# ---------------------------------------------------------------------------
async def cmd_start(chat_id):
    subscribers.setdefault(str(chat_id), {
        "whale", "flash_crash", "momentum", "news_spike",
        "arbitrage", "correlated", "liquidity"
    })
    await save_state()
    await send(chat_id,
        "👋 *Whale Alert Bot v2*\n\n"
        "🤖 *AI Assistant*\n"
        "Just type any message and I'll answer — trading questions, "
        "beginner explainers, general knowledge, or just a chat.\n"
        "Or use `/ask <question>` · `/clearai` to reset memory\n\n"
        "*Alert commands*\n"
        "/alerts `<types>` — toggle alert types\n"
        "/threshold `<usd>` — whale threshold\n"
        "/watch `/unwatch` `<slug>` — watchlist\n"
        "/watchlist · /status\n\n"
        "*Market intelligence*\n"
        "/book `<slug>` — order book snapshot\n"
        "/correlate `<slug_a> <slug_b>` — link markets\n\n"
        "*Social*\n"
        "/followwallet `<address>` — track a wallet\n"
        "/unfollowwallet `<address>`\n"
        "/leaderboard — top wallets by volume\n\n"
        "*Portfolio*\n"
        "/addposition `<slug> <yes|no> <qty> <price>`\n"
        "/portfolio — view P&L\n"
        "/removeposition `<slug>`\n\n"
        "*History*\n"
        "/history `[n]` — last N alerts (default 10)\n\n"
        "/stop — unsubscribe"
    )

async def cmd_stop(chat_id):
    subscribers.pop(str(chat_id), None)
    await save_state()
    await send(chat_id, "👋 Unsubscribed. /start to return.")

async def cmd_watch(chat_id, slug):
    if not slug:
        await send(chat_id, "Usage: /watch `<slug>`"); return
    user_watchlists[str(chat_id)].add(slug.lower())
    await save_state()
    await send(chat_id, f"✅ Tracking *{slug}*")

async def cmd_unwatch(chat_id, slug):
    if not slug:
        await send(chat_id, "Usage: /unwatch `<slug>`"); return
    user_watchlists[str(chat_id)].discard(slug.lower())
    await save_state()
    await send(chat_id, f"❌ Stopped tracking *{slug}*")

async def cmd_watchlist(chat_id):
    wl = user_watchlists.get(str(chat_id), set())
    await send(chat_id,
        "📋 *Watchlist:*\n" + "\n".join(f"• {s}" for s in sorted(wl))
        if wl else "Watchlist empty — receiving alerts for all markets.")

async def cmd_threshold(chat_id, value: str):
    try:
        usd = float(value.replace(",", "").replace("$", ""))
        user_thresholds[str(chat_id)] = usd
        await save_state()
        await send(chat_id, f"✅ Whale threshold → `${usd:,.0f}`")
    except ValueError:
        await send(chat_id, "Usage: /threshold `50000`")

async def cmd_alerts(chat_id, types_str: str):
    valid = {"whale","flash_crash","momentum","chart","news_spike",
             "arbitrage","correlated","liquidity"}
    chosen = {t.strip().lower() for t in types_str.split(",") if t.strip().lower() in valid}
    if not chosen:
        await send(chat_id, f"Valid types: {', '.join(sorted(valid))}"); return
    subscribers[str(chat_id)] = chosen
    await save_state()
    await send(chat_id, f"✅ Alerts: {', '.join(sorted(chosen))}")

async def cmd_status(chat_id):
    cid   = str(chat_id)
    types = subscribers.get(cid, set())
    wl    = user_watchlists.get(cid, set())
    thr   = user_thresholds.get(cid, WHALE_THRESHOLD)
    ww    = watched_wallets.get(cid, set())
    pos   = portfolios.get(cid, {})
    await send(chat_id,
        f"*Your settings*\n"
        f"Alerts: {', '.join(sorted(types)) or 'none'}\n"
        f"Watchlist: {', '.join(sorted(wl)) or 'all markets'}\n"
        f"Threshold: `${float(thr):,.0f}`\n"
        f"Tracked wallets: {len(ww)}\n"
        f"Portfolio positions: {len(pos)}")

# ---------------------------------------------------------------------------
# Commands — 📖 Order book snapshot
# ---------------------------------------------------------------------------
async def cmd_book(chat_id, slug: str):
    if not slug:
        await send(chat_id, "Usage: /book `<slug>`"); return
    # Find asset_id by slug
    aid = next((k for k, v in asset_slug.items() if v.lower() == slug.lower()), None)
    if not aid or aid not in order_books:
        await send(chat_id, f"No order book data for *{slug}* yet. Is it in your watchlist?")
        return
    book = order_books[aid]
    bids = book.get("bids", [])[:5]
    asks = book.get("asks", [])[:5]
    age  = int(time.time() - book.get("ts", time.time()))

    lines = [f"📖 *Order Book — {slug}* (updated {age}s ago)\n"]
    lines.append("*Asks (sell)*")
    for a in reversed(asks):
        lines.append(f"  `{float(a['price']):.4f}` × `{float(a.get('size',0)):,.0f}`")
    lines.append("── mid ──")
    lines.append("*Bids (buy)*")
    for b in bids:
        lines.append(f"  `{float(b['price']):.4f}` × `{float(b.get('size',0)):,.0f}`")

    if bids and asks:
        spread = (float(asks[0]["price"]) - float(bids[0]["price"])) / float(asks[0]["price"]) * 100
        lines.append(f"\nSpread: `{spread:.3f}%`")

    await send(chat_id, "\n".join(lines))

# ---------------------------------------------------------------------------
# Commands — 🐳 Whale wallet tracker
# ---------------------------------------------------------------------------
async def cmd_followwallet(chat_id, address: str):
    if not address:
        await send(chat_id, "Usage: /followwallet `<0x…address>`"); return
    watched_wallets[str(chat_id)].add(address.lower())
    await save_state()
    await send(chat_id, f"👀 Now tracking wallet `{address[:12]}…`")

async def cmd_unfollowwallet(chat_id, address: str):
    if not address:
        await send(chat_id, "Usage: /unfollowwallet `<0x…address>`"); return
    watched_wallets[str(chat_id)].discard(address.lower())
    await save_state()
    await send(chat_id, f"✅ Stopped tracking `{address[:12]}…`")

# ---------------------------------------------------------------------------
# Commands — 🏆 Leaderboard
# ---------------------------------------------------------------------------
async def cmd_leaderboard(chat_id):
    if not wallet_stats:
        await send(chat_id, "No wallet data yet — leaderboard builds as trades arrive."); return
    top = sorted(wallet_stats.items(), key=lambda x: x[1]["volume"], reverse=True)[:10]
    lines = ["🏆 *Top Whale Wallets*\n"]
    for i, (addr, stats) in enumerate(top, 1):
        medal = ["🥇","🥈","🥉"].get(i-1, f"{i}.")  # type: ignore[call-overload]
        medal = ["🥇","🥈","🥉"][i-1] if i <= 3 else f"{i}."
        lines.append(f"{medal} `{addr[:10]}…`\n"
                     f"   Vol: `${stats['volume']:,.0f}` · Trades: `{stats['trade_count']}`")
    await send(chat_id, "\n".join(lines))

# ---------------------------------------------------------------------------
# Commands — 📊 Alerts history
# ---------------------------------------------------------------------------
async def cmd_history(chat_id, n_str: str):
    try:
        n = min(int(n_str), 50) if n_str.strip() else 10
    except ValueError:
        n = 10
    history = list(alerts_history[str(chat_id)])[-n:]
    if not history:
        await send(chat_id, "No alerts recorded yet."); return
    lines = [f"📊 *Last {len(history)} alerts*\n"]
    for a in reversed(history):
        lines.append(f"[{a['ts']}] *{a['type']}* — {a['asset']}")
    await send(chat_id, "\n".join(lines))

# ---------------------------------------------------------------------------
# Commands — 💼 Portfolio tracker
# ---------------------------------------------------------------------------
async def cmd_addposition(chat_id, args_str: str):
    """Usage: /addposition <slug> <yes|no> <qty> <avg_price>"""
    parts = args_str.split()
    if len(parts) < 4:
        await send(chat_id,
            "Usage: /addposition `<slug> <yes|no> <qty> <avg_price>`\n"
            "Example: `/addposition trump-wins yes 1000 0.55`"); return
    slug_key, side, qty_s, price_s = parts[0], parts[1].lower(), parts[2], parts[3]
    if side not in ("yes", "no"):
        await send(chat_id, "Side must be `yes` or `no`"); return
    try:
        qty   = float(qty_s)
        price = float(price_s)
    except ValueError:
        await send(chat_id, "qty and avg_price must be numbers"); return

    portfolios[str(chat_id)][slug_key] = {"side": side, "qty": qty, "avg_price": price}
    await save_state()
    cost = qty * price
    await send(chat_id,
        f"✅ Position saved\n"
        f"*{slug_key}* — {side.upper()} `{qty:,.0f}` shares @ `{price:.4f}`\n"
        f"Cost basis: `${cost:,.2f}`")

async def cmd_portfolio(chat_id):
    cid = str(chat_id)
    pos = portfolios.get(cid, {})
    if not pos:
        await send(chat_id, "No positions. Add one with /addposition."); return

    lines = ["💼 *Portfolio*\n"]
    total_cost = 0.0
    total_value = 0.0

    for slug_key, p in pos.items():
        # Get latest price from price history if available
        aid = next((k for k, v in asset_slug.items() if v.lower() == slug_key.lower()), None)
        current_price = None
        if aid and trade_price_history[aid]:
            current_price = trade_price_history[aid][-1][1]
            if p["side"] == "no":
                current_price = 1.0 - current_price  # invert for NO shares

        cost  = p["qty"] * p["avg_price"]
        total_cost += cost

        if current_price is not None:
            value = p["qty"] * current_price
            pnl   = value - cost
            pnl_pct = pnl / cost * 100 if cost else 0
            total_value += value
            arrow = "📈" if pnl >= 0 else "📉"
            lines.append(
                f"{arrow} *{slug_key}* ({p['side'].upper()})\n"
                f"  `{p['qty']:,.0f}` @ `{p['avg_price']:.4f}` → `{current_price:.4f}`\n"
                f"  P&L: `${pnl:+,.2f}` (`{pnl_pct:+.1f}%`)")
        else:
            total_value += cost
            lines.append(
                f"❓ *{slug_key}* ({p['side'].upper()})\n"
                f"  `{p['qty']:,.0f}` @ `{p['avg_price']:.4f}` — no live price yet")

    total_pnl = total_value - total_cost
    total_pct = total_pnl / total_cost * 100 if total_cost else 0
    lines.append(f"\n*Total P&L: `${total_pnl:+,.2f}` (`{total_pct:+.1f}%`)*")
    await send(chat_id, "\n".join(lines))

async def cmd_removeposition(chat_id, slug_key: str):
    if not slug_key:
        await send(chat_id, "Usage: /removeposition `<slug>`"); return
    portfolios[str(chat_id)].pop(slug_key.lower(), None)
    await save_state()
    await send(chat_id, f"✅ Removed position: *{slug_key}*")

# ---------------------------------------------------------------------------
# Commands — 🔗 Correlate markets
# ---------------------------------------------------------------------------
async def cmd_correlate(chat_id, args_str: str):
    """Link two markets that should sum to ~1.0 (complementary outcomes)."""
    parts = args_str.split()
    if len(parts) < 2:
        await send(chat_id,
            "Usage: /correlate `<slug_a> <slug_b> [expected_sum]`\n"
            "Default expected_sum = 1.0 (complementary markets)\n"
            "Example: `/correlate trump-wins trump-loses 1.0`"); return
    slug_a, slug_b = parts[0].lower(), parts[1].lower()
    expected = float(parts[2]) if len(parts) > 2 else 1.0

    # Find asset ids
    aid_a = next((k for k, v in asset_slug.items() if v.lower() == slug_a), None)
    aid_b = next((k for k, v in asset_slug.items() if v.lower() == slug_b), None)
    if not aid_a or not aid_b:
        await send(chat_id, f"Couldn't find asset IDs for `{slug_a}` or `{slug_b}`.\n"
                            "Make sure both are in your watchlist."); return

    # Remove existing pair if present
    correlated_pairs[:] = [p for p in correlated_pairs if not (p[0]==aid_a and p[1]==aid_b)]
    correlated_pairs.append([aid_a, aid_b, expected])
    await save_state()
    await send(chat_id,
        f"🔗 Linked *{slug_a}* + *{slug_b}*\n"
        f"Expected sum: `{expected:.2f}` — you'll be alerted on divergence ≥{CORR_DIVERGE_PCT}%")

# ---------------------------------------------------------------------------
# 🤖 AI Assistant (Claude-powered, market-context-aware)
# ---------------------------------------------------------------------------

def _build_market_context(chat_id: str) -> str:
    """Snapshot of live bot data injected into the AI system prompt."""
    lines = ["You are an expert Polymarket trading assistant embedded in a whale-alert Telegram bot.",
             "You have access to the following live data from the bot right now.\n"]

    # --- Active markets & prices ---
    if asset_slug:
        lines.append("## Live Market Prices (last trade)")
        for aid, slug in list(asset_slug.items())[:20]:   # cap at 20 for token budget
            hist = list(trade_price_history[aid])
            if hist:
                last_price = hist[-1][1]
                vol = sum(market_volumes[aid]) if market_volumes[aid] else 0
                lines.append(f"- {slug}: YES={last_price:.4f}  NO={1-last_price:.4f}  "
                              f"vol≈${vol:,.0f}")
    else:
        lines.append("## Live Market Prices\n(No trades received yet)")

    # --- Order books ---
    if order_books:
        lines.append("\n## Order Book Snapshots (best bid/ask)")
        for aid, book in list(order_books.items())[:10]:
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            if bids and asks:
                bb = float(bids[0]["price"])
                ba = float(asks[0]["price"])
                spread = (ba - bb) / ba * 100
                lines.append(f"- {slug_for(aid)}: bid={bb:.4f} ask={ba:.4f} spread={spread:.2f}%")

    # --- User portfolio ---
    pos = portfolios.get(chat_id, {})
    if pos:
        lines.append("\n## User Portfolio")
        for slug_key, p in pos.items():
            aid = next((k for k, v in asset_slug.items() if v.lower() == slug_key.lower()), None)
            current = None
            if aid and trade_price_history[aid]:
                current = trade_price_history[aid][-1][1]
                if p["side"] == "no":
                    current = 1.0 - current
            cost = p["qty"] * p["avg_price"]
            if current is not None:
                pnl = p["qty"] * current - cost
                lines.append(f"- {slug_key} ({p['side'].upper()}) "
                              f"qty={p['qty']:,.0f} avg={p['avg_price']:.4f} "
                              f"current={current:.4f} PnL=${pnl:+,.2f}")
            else:
                lines.append(f"- {slug_key} ({p['side'].upper()}) "
                              f"qty={p['qty']:,.0f} avg={p['avg_price']:.4f} (no live price)")

    # --- Recent alerts ---
    recent = list(alerts_history[chat_id])[-10:]
    if recent:
        lines.append("\n## Recent Alerts (last 10)")
        for a in recent:
            lines.append(f"- [{a['ts']}] {a['type']}: {a['asset']}")

    # --- Leaderboard top 5 ---
    if wallet_stats:
        top5 = sorted(wallet_stats.items(), key=lambda x: x[1]["volume"], reverse=True)[:5]
        lines.append("\n## Top Wallets by Volume")
        for addr, s in top5:
            lines.append(f"- {addr[:12]}…  vol=${s['volume']:,.0f}  trades={s['trade_count']}")

    lines.append(
        "\n## Who you are"
        "\nYou are a friendly, knowledgeable assistant inside a Polymarket whale-alert "
        "Telegram bot. You can help with ANYTHING — trading questions, general knowledge, "
        "math, coding, creative tasks, life advice, or just a chat. You are not limited "
        "to trading topics."
        "\n\n## When trading / market questions come up"
        "\n- Use the live prices, order book, portfolio, and alert data above."
        "\n- Explain prediction markets and trading concepts in plain English for beginners "
        "— never assume prior knowledge unless the user shows it."
        "\n- For beginners: define jargon (e.g. 'YES share', 'spread', 'liquidity') the "
        "first time you use it."
        "\n- Give balanced, honest analysis. Never hype or guarantee outcomes."
        "\n\n## General behaviour"
        "\n- Be concise by default — this is Telegram, so aim for ≤150 words. Go longer "
        "only if the question genuinely needs it (e.g. a tutorial)."
        "\n- Format numbers clearly: $ for USD, % for percentages."
        "\n- If you don't have enough data to answer precisely, say so and explain what "
        "you'd need."
        "\n- Keep a warm, conversational tone. It's fine to be a little playful."
    )
    return "\n".join(lines)


async def cmd_ask(chat_id: str | int, question: str):
    """Handle /ask <question> or any plain-text message — powered by Gemini."""
    cid = str(chat_id)

    if not GEMINI_API_KEY:
        await send(chat_id,
            "⚠️ AI assistant not configured.\n"
            "Set the `GEMINI_API_KEY` environment variable on Render to enable it.\n"
            "Get a free key at: aistudio.google.com")
        return

    if not question.strip():
        await send(chat_id,
            "🤖 Ask me anything — trading, markets, general knowledge, or just chat!\n"
            "Example: `/ask what is a prediction market?`\n"
            "Or just send a message without a command and I'll reply.")
        return

    await send(chat_id, "🤖 Thinking...")

    cid_history = list(ai_conversations[cid])
    system_prompt = _build_market_context(cid)

    # Gemini uses "contents" array with "role" + "parts"
    # System prompt is prepended as the first user turn (Gemini REST doesn't
    # have a dedicated system field in the basic generateContent endpoint)
    contents = []

    # Inject system context as a priming exchange so it doesn't count as
    # the live user question but still guides every reply
    contents.append({
        "role": "user",
        "parts": [{"text": system_prompt}]
    })
    contents.append({
        "role": "model",
        "parts": [{"text": "Understood. I'm ready to help."}]
    })

    # Replay conversation history
    for msg in cid_history[-AI_MAX_HISTORY:]:
        gemini_role = "model" if msg["role"] == "assistant" else "user"
        contents.append({
            "role": gemini_role,
            "parts": [{"text": msg["content"]}]
        })

    # Append the new question
    contents.append({
        "role": "user",
        "parts": [{"text": question}]
    })

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                GEMINI_API,
                params={"key": GEMINI_API_KEY},
                headers={"content-type": "application/json"},
                json={
                    "contents": contents,
                    "generationConfig": {
                        "maxOutputTokens": AI_MAX_TOKENS,
                        "temperature": 0.7,
                    },
                },
                timeout=30,
            )
            data = resp.json()

        if resp.status_code != 200:
            err = data.get("error", {}).get("message", "unknown error")
            log.warning(f"Gemini API error {resp.status_code}: {err}")
            await send(chat_id, f"⚠️ AI error: {err}")
            return

        # Extract text from Gemini response structure
        try:
            reply = data["candidates"][0]["content"]["parts"][0]["text"].strip()
        except (KeyError, IndexError):
            reply = ""

        if not reply:
            await send(chat_id, "⚠️ AI returned an empty response. Please try again.")
            return

        # Save to conversation history using standard role names
        cid_history.append({"role": "user",      "content": question})
        cid_history.append({"role": "assistant",  "content": reply})
        ai_conversations[cid] = deque(cid_history, maxlen=AI_MAX_HISTORY)

        await send(chat_id, f"🤖 {reply}")

    except httpx.TimeoutException:
        await send(chat_id, "⚠️ AI request timed out. Try again in a moment.")
    except Exception as e:
        log.warning(f"cmd_ask error: {e}")
        await send(chat_id, "⚠️ Something went wrong with the AI assistant.")


async def cmd_clearai(chat_id: str | int):
    """Wipe conversation memory for this user."""
    ai_conversations[str(chat_id)].clear()
    await send(chat_id, "🧹 AI conversation history cleared.")


# ---------------------------------------------------------------------------
# Telegram long-polling
# ---------------------------------------------------------------------------
async def poll_telegram():
    offset = 0
    async with httpx.AsyncClient() as client:
        while True:
            try:
                r = await client.get(
                    f"{TELEGRAM_API}/getUpdates",
                    params={"timeout": 30, "offset": offset},
                    timeout=40,
                )
                updates = r.json().get("result", [])
                for update in updates:
                    offset = update["update_id"] + 1
                    msg     = update.get("message", {})
                    text    = msg.get("text", "").strip()
                    chat_id = msg.get("chat", {}).get("id")
                    if not text or not chat_id:
                        continue

                    parts = text.split()
                    cmd   = parts[0].lower().split("@")[0]
                    args  = parts[1:]
                    astr  = " ".join(args)

                    if   cmd == "/start":            await cmd_start(chat_id)
                    elif cmd == "/stop":             await cmd_stop(chat_id)
                    elif cmd == "/watch":            await cmd_watch(chat_id, astr)
                    elif cmd == "/unwatch":          await cmd_unwatch(chat_id, astr)
                    elif cmd == "/watchlist":        await cmd_watchlist(chat_id)
                    elif cmd == "/threshold":        await cmd_threshold(chat_id, astr)
                    elif cmd == "/alerts":           await cmd_alerts(chat_id, astr)
                    elif cmd == "/status":           await cmd_status(chat_id)
                    elif cmd == "/book":             await cmd_book(chat_id, astr)
                    elif cmd == "/followwallet":     await cmd_followwallet(chat_id, astr)
                    elif cmd == "/unfollowwallet":   await cmd_unfollowwallet(chat_id, astr)
                    elif cmd == "/leaderboard":      await cmd_leaderboard(chat_id)
                    elif cmd == "/history":          await cmd_history(chat_id, astr)
                    elif cmd == "/addposition":      await cmd_addposition(chat_id, astr)
                    elif cmd == "/portfolio":        await cmd_portfolio(chat_id)
                    elif cmd == "/removeposition":   await cmd_removeposition(chat_id, astr)
                    elif cmd == "/correlate":        await cmd_correlate(chat_id, astr)
                    elif cmd == "/ask":              await cmd_ask(chat_id, astr)
                    elif cmd == "/clearai":          await cmd_clearai(chat_id)
                    elif not cmd.startswith("/"):
                        # Plain text (no command) → AI assistant
                        await cmd_ask(chat_id, text)
                    # Unknown /commands are silently ignored

            except Exception as e:
                log.warning(f"poll_telegram error: {e}")
                await asyncio.sleep(5)

# ---------------------------------------------------------------------------
# HTTP keep-alive server (Render free tier)
# ---------------------------------------------------------------------------
async def handle_health(request):
    return web.Response(text=json.dumps({
        "status": "ok",
        "subscribers": len(subscribers),
        "tracked_markets": len(asset_slug),
        "tracked_wallets": sum(len(v) for v in watched_wallets.values()),
    }), content_type="application/json")

async def run_http_server():
    app = web.Application()
    app.router.add_get("/", handle_health)
    app.router.add_get("/health", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info(f"HTTP server on port {PORT}")

async def self_ping():
    if not SELF_URL:
        return
    url = f"{SELF_URL}/health"
    while True:
        await asyncio.sleep(14 * 60)
        try:
            async with httpx.AsyncClient() as client:
                await client.get(url, timeout=10)
            log.debug("Self-ping OK")
        except Exception as e:
            log.warning(f"Self-ping failed: {e}")

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main():
    await _init_redis()
    await load_state()
    log.info("🐳 Whale Alert Bot v2 starting…")
    await asyncio.gather(
        run_http_server(),
        self_ping(),
        poll_telegram(),
        polymarket_listener(),
        daily_whale_digest(),
    )

if __name__ == "__main__":
    asyncio.run(main())
