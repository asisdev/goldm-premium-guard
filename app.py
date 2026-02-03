import os, time, json, asyncio, logging
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

import requests
import pyotp
from fastapi import FastAPI
from neo_api_client import NeoAPI   # Kotak client

# ======================================================================
# CONFIG (READ FROM RENDER ENVIRONMENT VARIABLES)
# ======================================================================
LOOKBACK_MIN   = int(os.getenv("LOOKBACK_MIN", "5"))
UNDER_MOVE_PCT = float(os.getenv("UNDER_MOVE_PCT", "0.20"))
OPT_STALE_PCT  = float(os.getenv("OPT_STALE_PCT",  "0.10"))
RUN_INTERVAL_S = int(os.getenv("RUN_INTERVAL_S", "60"))
TOP_K          = int(os.getenv("TOP_K", "5"))
SILENCE_MIN    = int(os.getenv("ALERT_SILENCE_MIN", "5"))

TT_WEBHOOK_URL = os.getenv("TT_WEBHOOK_URL")
TT_API_TOKEN   = os.getenv("TT_API_TOKEN")

NEO_CONSUMER_KEY = os.getenv("NEO_CONSUMER_KEY")
NEO_USERNAME     = os.getenv("NEO_USERNAME")
NEO_PASSWORD     = os.getenv("NEO_PASSWORD")
NEO_TOTP_SECRET  = os.getenv("NEO_TOTP_SECRET")

IST = timezone(timedelta(hours=5, minutes=30))

app = FastAPI()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("goldm-engine")

# Buffers for percentage move
under_buf = deque(maxlen=3600)
hist = defaultdict(lambda: deque(maxlen=3600))

last_alert_ts: Optional[datetime] = None

# ======================================================================
# KOTAK CLIENT (Simplified)
# ======================================================================
class KotakClient:
    def __init__(self):
        miss = [k for k,v in {
            "NEO_CONSUMER_KEY": NEO_CONSUMER_KEY,
            "NEO_USERNAME": NEO_USERNAME,
            "NEO_PASSWORD": NEO_PASSWORD,
            "NEO_TOTP_SECRET": NEO_TOTP_SECRET
        }.items() if not v]

        if miss:
            raise RuntimeError(f"Missing env vars: {', '.join(miss)}")

        self.client = None
        self.logged_in = False

    def login(self):
        totp = pyotp.TOTP(NEO_TOTP_SECRET).now()
        self.client = NeoAPI(environment="prod", access_token=None, neo_fin_key=None,
                             consumer_key=NEO_CONSUMER_KEY)
        self.client.login(mobilenumber=NEO_USERNAME, password=NEO_PASSWORD)
        self.client.session_2fa(OTP=str(totp))
        self.logged_in = True
        log.info("Kotak Neo login OK.")

    def ensure(self):
        if not self.logged_in:
            self.login()

    def scrip_search(self, query: str):
        self.ensure()
        if hasattr(self.client, "scrip_search"):
            return self.client.scrip_search(searchstr=query)
        return []

    def quotes(self, symbols: List[str]):
        self.ensure()
        if not symbols:
            return {}
        q = self.client.quotes_neo_symbol(neo_symbol=symbols, quote_type="all")
        out = {}
        for item in q:
            sym = item.get("tradingsymbol")
            if sym:
                out[sym] = {
                    "ltp": item.get("last_price"),
                    "oi": item.get("oi"),
                }
        return out

# ======================================================================
# HELPERS
# ======================================================================
def pct_change(buf: deque, lookback_min: int):
    if len(buf) < 2:
        return None
    cutoff = time.time() - lookback_min * 60
    while buf and buf[0][0] < cutoff:
        buf.popleft()
    if len(buf) < 2:
        return None
    t_old, p_old = buf[0]
    t_new, p_new = buf[-1]
    if not p_old or not p_new:
        return None
    return 100.0 * (p_new - p_old) / p_old

def should_alert(u, ce, pe):
    if u is None or ce is None or pe is None:
        return False
    return abs(u) >= UNDER_MOVE_PCT and abs(ce) <= OPT_STALE_PCT and abs(pe) <= OPT_STALE_PCT

def send_to_tradetron(payload: dict):
    if not TT_WEBHOOK_URL or not TT_API_TOKEN:
        log.info("Tradetron webhook not set.")
        return
    data = {"key": TT_API_TOKEN, **payload}
    try:
        r = requests.post(TT_WEBHOOK_URL, json=data, timeout=3)
        log.info(f"TT status: {r.status_code}")
    except Exception as e:
        log.error(f"TT error: {e}")

# ======================================================================
# DISCOVERY (Simplified)
# ======================================================================
def discover_goldm(broker: KotakClient):
    resp = broker.scrip_search("GOLDM")
    fut = None
    ce = []
    pe = []

    for x in resp:
        ts = x.get("tradingsymbol", "")
        typ = x.get("instrument_type", "").upper()
        if "GOLDM" not in ts:
            continue
        if typ == "FUT" and fut is None:
            fut = ts
        if typ == "CE":
            ce.append(ts)
        if typ == "PE":
            pe.append(ts)

    return fut, ce, pe

# ======================================================================
# MAIN ENGINE
# ======================================================================
async def engine():
    global last_alert_ts
    broker = KotakClient()

    fut, ce_list, pe_list = discover_goldm(broker)
    if not fut or not ce_list or not pe_list:
        log.error("Discovery failed.")
        return

    log.info(f"FUT={fut} CE={len(ce_list)} PE={len(pe_list)}")

    while True:
        now = datetime.now(IST)
        now_ts = time.time()

        # Underlying
        u = broker.quotes([fut]).get(fut, {}).get("ltp")
        if u:
            under_buf.append((now_ts, float(u)))

        # CE/PE quotes
        quotes = broker.quotes(ce_list + pe_list)

        # Update histories
        for sym, meta in quotes.items():
            if meta.get("ltp"):
                hist[sym].append((now_ts, float(meta["ltp"])))

        # Pick top OI
        ce_sorted = sorted([s for s in ce_list if quotes.get(s, {}).get("oi")], 
                           key=lambda s: quotes[s]["oi"], reverse=True)
        pe_sorted = sorted([s for s in pe_list if quotes.get(s, {}).get("oi")], 
                           key=lambda s: quotes[s]["oi"], reverse=True)

        ce_pick = ce_sorted[0] if ce_sorted else None
        pe_pick = pe_sorted[0] if pe_sorted else None

        u_pct  = pct_change(under_buf, LOOKBACK_MIN)
        ce_pct = pct_change(hist[ce_pick], LOOKBACK_MIN) if ce_pick else None
        pe_pct = pct_change(hist[pe_pick], LOOKBACK_MIN) if pe_pick else None

        log.info(f"Pick CE={ce_pick} PE={pe_pick} | U%={u_pct} CE%={ce_pct} PE%={pe_pct}")

        if should_alert(u_pct, ce_pct, pe_pct):
            if not last_alert_ts or now - last_alert_ts >= timedelta(minutes=SILENCE_MIN):
                payload = {
                    "event": "goldm_non_responsive",
                    "u_pct": u_pct,
                    "ce_pct": ce_pct,
                    "pe_pct": pe_pct,
                    "ce_symbol": ce_pick,
                    "pe_symbol": pe_pick,
                    "ts": now.isoformat()
                }
                send_to_tradetron(payload)
                last_alert_ts = now

        await asyncio.sleep(RUN_INTERVAL_S)

# ======================================================================
# STARTUP + HEALTH
# ======================================================================
@app.on_event("startup")
async def _startup():
    asyncio.create_task(engine())

@app.get("/health")
def health():
    return {"ok": True, "time": datetime.now(IST).isoformat()}

