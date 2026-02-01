import os, time, json, math, asyncio, logging
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Tuple

import requests
from fastapi import FastAPI

# ---------- CONFIG ----------
BROKER = os.getenv("BROKER", "KOTAK").upper()        # KOTAK or ZERODHA
LOOKBACK_MIN = int(os.getenv("LOOKBACK_MIN", "5"))
UNDER_MOVE_PCT = float(os.getenv("UNDER_MOVE_PCT", "0.20"))  # alert threshold
OPT_STALE_PCT  = float(os.getenv("OPT_STALE_PCT",  "0.10"))  # considered "not moving"
RUN_INTERVAL_S = int(os.getenv("RUN_INTERVAL_S", "60"))
TOP_K = int(os.getenv("TOP_K", "5"))  # top-K by OI to consider for momentum pick

TT_WEBHOOK_URL = os.getenv("TT_WEBHOOK_URL", "")
TT_API_TOKEN   = os.getenv("TT_API_TOKEN", "")

IST = timezone(timedelta(hours=5, minutes=30))
app = FastAPI()
log = logging.getLogger("uvicorn")
logging.basicConfig(level=logging.INFO)

# Rolling buffers for 5-min momentum per symbol
# structure: sym -> deque[(ts, ltp)]
hist: Dict[str, deque] = defaultdict(lambda: deque(maxlen=3600))
under_buf = deque(maxlen=3600)  # underlying FUT

last_alert_ts: Optional[datetime] = None
SILENCE_MIN = int(os.getenv("ALERT_SILENCE_MIN", "5"))

# Zerodha cache
_cached_instruments = None
_cached_expiry = None
_goldm_monthly_opts: List[dict] = []

# Kotak: user watchlist (comma separated symbols for the current monthly)
KOTAK_WATCHLIST = [s.strip() for s in os.getenv("KOTAK_WATCHLIST", "").split(",") if s.strip()]

# ---------- BROKER CLIENTS ----------
class KotakClient:
    def __init__(self):
        self.consumer_key = os.getenv("NEO_CONSUMER_KEY")
        self.username = os.getenv("NEO_USERNAME")
        self.password = os.getenv("NEO_PASSWORD")
        self.totp_secret = os.getenv("NEO_TOTP_SECRET")
        if not all([self.consumer_key, self.username, self.password, self.totp_secret]):
            raise RuntimeError("Missing Kotak Neo env vars")

        from neo_api_client import NeoAPI  # v2 installed from Git in build
        self.NeoAPI = NeoAPI
        self.client = None
        self.session_created = False

    def login(self):
        import pyotp
        totp_code = pyotp.TOTP(self.totp_secret).now()
        self.client = self.NeoAPI(environment='prod', access_token=None, neo_fin_key=None,
                                  consumer_key=self.consumer_key)
        self.client.login(mobilenumber=self.username, password=self.password)
        self.client.session_2fa(OTP=str(totp_code))
        self.session_created = True
        log.info("Kotak Neo login successful.")

    def ensure_login(self):
        if not self.session_created:
            self.login()

    def quotes(self, symbols: List[str], quote_type: str = "all") -> Dict[str, dict]:
        """
        symbols: list of Neo symbols (from your feed/watchlist)
        quote_type supports 'oi', 'ltp', 'all' as per SDK docs.
        Returns mapping: symbol -> {ltp, oi, bid, ask}
        """
        self.ensure_login()
        out = {}
        try:
            # Many SDKs accept list of dicts or strings. v2 docs show both "quotes" and "quotes_neo_symbol".
            # We'll try quotes_neo_symbol if available, else generic quotes().
            if hasattr(self.client, "quotes_neo_symbol"):
                q = self.client.quotes_neo_symbol(neo_symbol=symbols, quote_type=quote_type)
            else:
                q = self.client.quotes(instrument_tokens=[{"instrument_token": s, "exchange_segment": "cde_fo"} for s in symbols],
                                       quote_type=quote_type)

            # Normalize based on common keys (ltp, oi etc.)
            for item in q:
                sym = item.get("tradingsymbol") or item.get("symbol") or item.get("instrument_token")
                ltp = item.get("last_price") or item.get("ltp")
                oi  = item.get("oi") or item.get("open_interest")
                bid = item.get("best_bid_price") or item.get("bid")
                ask = item.get("best_ask_price") or item.get("ask")
                if sym:
                    out[sym] = {"ltp": float(ltp) if ltp is not None else None,
                                "oi": int(oi) if oi is not None else None,
                                "bid": float(bid) if bid is not None else None,
                                "ask": float(ask) if ask is not None else None}
        except Exception as e:
            log.error(f"Kotak quotes error: {e}")
        return out


class ZerodhaClient:
    def __init__(self):
        self.api_key = os.getenv("KITE_API_KEY")
        self.api_secret = os.getenv("KITE_API_SECRET")
        self.access_token = os.getenv("KITE_ACCESS_TOKEN")
        if not all([self.api_key, self.api_secret, self.access_token]):
            raise RuntimeError("Missing Zerodha env vars (KITE_API_KEY/SECRET/TOKEN)")
        from kiteconnect import KiteConnect
        self.kite = KiteConnect(api_key=self.api_key)
        self.kite.set_access_token(self.access_token)

    def instruments(self, exchange=None):
        global _cached_instruments
        if _cached_instruments is None:
            _cached_instruments = self.kite.instruments(exchange="MCX" if exchange else None)
        return _cached_instruments

    def goldm_monthly_opts(self) -> Tuple[List[dict], Optional[datetime]]:
        """
        Return list of GOLDM monthly CE/PE instruments for the nearest monthly expiry.
        """
        global _goldm_monthly_opts, _cached_expiry
        if _goldm_monthly_opts and _cached_expiry:
            return _goldm_monthly_opts, _cached_expiry
        inst = self.instruments(exchange="MCX")
        opts = [i for i in inst
                if i["instrument_type"] in ("CE","PE") and i["tradingsymbol"].startswith("GOLDM")]
        expiries = sorted({i["expiry"] for i in opts})
        if not expiries:
            return [], None
        nearest = expiries[0]
        monthlies = [i for i in opts if i["expiry"]==nearest]
        _goldm_monthly_opts, _cached_expiry = monthlies, nearest
        return monthlies, nearest

    def quote_full(self, tradingsymbols: List[str]) -> Dict[str, dict]:
        """
        Use GET /quote to get OI + LTP in one call. 
        Zerodha expects "MCX:SYMBOL" format keys.
        """
        if not tradingsymbols:
            return {}
        keys = [f"MCX:{ts}" for ts in tradingsymbols]
        q = self.kite.quote(keys)  # includes OI and depth
        out = {}
        for k, v in q.items():
            ts = k.split(":")[1]
            ltp = v.get("last_price")
            oi  = v.get("oi") or v.get("open_interest")
            bid = v.get("depth", {}).get("buy", [{}])[0].get("price")
            ask = v.get("depth", {}).get("sell", [{}])[0].get("price")
            out[ts] = {"ltp": float(ltp) if ltp is not None else None,
                       "oi": int(oi) if oi is not None else None,
                       "bid": float(bid) if bid else None,
                       "ask": float(ask) if ask else None}
        return out

    def fut_nearest(self) -> Optional[str]:
        inst = self.instruments(exchange="MCX")
        futs = [i for i in inst if i["instrument_type"]=="FUT" and i["tradingsymbol"].startswith("GOLDM")]
        if not futs:
            return None
        futs_sorted = sorted(futs, key=lambda x: x["expiry"])
        return futs_sorted[0]["tradingsymbol"]

    def ltp(self, symbols: List[str]) -> Dict[str, float]:
        if not symbols: return {}
        keys = [f"MCX:{s}" for s in symbols]
        q = self.kite.ltp(keys)
        return {k.split(":")[1]: float(v["last_price"]) for k, v in q.items()}

# ---------- UTILS ----------
def pct_change(buf: deque, lookback_min: int) -> Optional[float]:
    if len(buf) < 2:
        return None
    cutoff = time.time() - lookback_min*60
    # Drop too-old
    while buf and buf[0][0] < cutoff:
        buf.popleft()
    if len(buf) < 2:
        return None
    old_ts, old_px = buf[0]
    new_ts, new_px = buf[-1]
    if old_px in (None, 0) or new_px is None:
        return None
    return 100.0 * (new_px - old_px) / old_px

def should_alert(u_pct, ce_pct, pe_pct):
    if u_pct is None or ce_pct is None or pe_pct is None:
        return False
    return (abs(u_pct) >= UNDER_MOVE_PCT) and (abs(ce_pct) <= OPT_STALE_PCT) and (abs(pe_pct) <= OPT_STALE_PCT)

def post_to_tradetron(payload: dict):
    if not TT_WEBHOOK_URL or not TT_API_TOKEN:
        log.warning("Tradetron webhook not configured; skipping.")
        return
    headers = {"Content-Type": "application/json"}
    data = {"key": TT_API_TOKEN, **payload}
    try:
        r = requests.post(TT_WEBHOOK_URL, headers=headers, data=json.dumps(data), timeout=3)
        log.info(f"TT webhook status={r.status_code}")
    except Exception as e:
        log.error(f"TT webhook error: {e}")

def best_by_oi_and_momo(side: str, quotes: Dict[str, dict], top_k: int) -> Optional[str]:
    """
    side: 'CE' or 'PE'
    quotes: mapping tradingsymbol -> {ltp, oi}
    - Rank by OI (desc) -> take top_k
    - Among them, choose symbol with highest positive % change over LOOKBACK_MIN
    - If momentum not available yet, fall back to highest OI
    """
    # Filter only that side by parsing 'CE'/'PE' from symbol
    filtered = [(ts, meta) for ts, meta in quotes.items() if ts.endswith(side)]
    filtered = [(ts, meta) for ts, meta in filtered if meta.get("oi") is not None]
    if not filtered:
        return None
    # sort by OI desc
    filtered.sort(key=lambda x: x[1]["oi"], reverse=True)
    top = filtered[:max(1, top_k)]

    # Among top, compute momo
    best_sym, best_momo = None, -1e9
    any_momo = False
    for ts, meta in top:
        buf = hist[ts]
        momo = pct_change(buf, LOOKBACK_MIN)
        if momo is not None:
            any_momo = True
            if momo > best_momo:
                best_momo = momo
                best_sym = ts

    if any_momo and best_sym:
        return best_sym
    # else fallback to top OI
    return top[0][0]

# ---------- ENGINE ----------
async def engine_loop():
    if BROKER == "ZERODHA":
        broker = ZerodhaClient()
        fut_sym = broker.fut_nearest()
        if not fut_sym:
            log.error("No GOLDM FUT found.")
            return
        log.info(f"Underlying FUT: {fut_sym}")

    else:
        broker = KotakClient()
        if not KOTAK_WATCHLIST:
            log.error("KOTAK_WATCHLIST env is empty. Provide comma-separated option symbols for current monthly.")
            return
        # For underlying FUT in Kotak mode, ask user to supply FUT symbol to track
        FUT_SYM = os.getenv("KOTAK_FUT_SYMBOL")
        if not FUT_SYM:
            log.error("Set KOTAK_FUT_SYMBOL for the GOLDM FUT to track underlying movement.")
            return

    while True:
        try:
            now = datetime.now(IST)
            now_ts = time.time()

            # 1) Get underlying LTP
            if BROKER == "ZERODHA":
                u = broker.ltp([fut_sym]).get(fut_sym)
            else:
                q_u = broker.quotes([FUT_SYM], quote_type="ltp").get(FUT_SYM, {})
                u = q_u.get("ltp")

            if u is not None:
                under_buf.append((now_ts, float(u)))

            # 2) Get options snapshot -> OI + LTP
            if BROKER == "ZERODHA":
                opts, exp = broker.goldm_monthly_opts()
                symbols = [i["tradingsymbol"] for i in opts]
                q = broker.quote_full(symbols)
            else:
                # Kotak: user-provided watchlist
                q = broker.quotes(KOTAK_WATCHLIST, quote_type="all")

            # update per-symbol history buffers
            for ts, meta in q.items():
                ltp = meta.get("ltp")
                if ltp is not None:
                    hist[ts].append((now_ts, float(ltp)))

            # 3) Pick CE/PE best by OI + momentum
            ce_pick = best_by_oi_and_momo("CE", q, TOP_K)
            pe_pick = best_by_oi_and_momo("PE", q, TOP_K)

            # 4) Compute % changes for alert check
            u_pct  = pct_change(under_buf, LOOKBACK_MIN)
            ce_pct = pct_change(hist[ce_pick], LOOKBACK_MIN) if ce_pick else None
            pe_pct = pct_change(hist[pe_pick], LOOKBACK_MIN) if pe_pick else None

            log.info(f"Pick CE={ce_pick} PE={pe_pick} | U%={u_pct} CE%={ce_pct} PE%={pe_pct}")

            global last_alert_ts
            if should_alert(u_pct, ce_pct, pe_pct):
                if (last_alert_ts is None) or (now - last_alert_ts >= timedelta(minutes=SILENCE_MIN)):
                    payload = {
                        "event": "goldm_non_responsive",
                        "u_pct": round(u_pct, 4) if u_pct is not None else None,
                        "ce_pct": round(ce_pct,4) if ce_pct is not None else None,
                        "pe_pct": round(pe_pct,4) if pe_pct is not None else None,
                        "lookback_min": LOOKBACK_MIN,
                        "thresholds": {"under": UNDER_MOVE_PCT, "opt": OPT_STALE_PCT},
                        "ce_symbol": ce_pick, "pe_symbol": pe_pick,
                        "ts": now.isoformat()
                    }
                    post_to_tradetron(payload)
                    last_alert_ts = now

        except Exception as e:
            log.error(f"Loop error: {e}")

        await asyncio.sleep(RUN_INTERVAL_S)

@app.on_event("startup")
async def _startup():
    asyncio.create_task(engine_loop())

@app.get("/health")
def health():
    return {"ok": True, "time": datetime.now(IST).isoformat()}