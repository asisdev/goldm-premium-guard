import os, time, json, asyncio, logging, csv, io
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Tuple

import requests
from fastapi import FastAPI

# ========== CONFIG: set values in Render → Environment (do NOT hardcode) ==========
BROKER = "KOTAK"  # fixed to Kotak Neo for this build

LOOKBACK_MIN   = int(os.getenv("LOOKBACK_MIN", "5"))
UNDER_MOVE_PCT = float(os.getenv("UNDER_MOVE_PCT", "0.20"))
OPT_STALE_PCT  = float(os.getenv("OPT_STALE_PCT",  "0.10"))
RUN_INTERVAL_S = int(os.getenv("RUN_INTERVAL_S", "60"))
TOP_K          = int(os.getenv("TOP_K", "5"))
SILENCE_MIN    = int(os.getenv("ALERT_SILENCE_MIN", "5"))

# ---- Tradetron webhook (Render → Environment) ----
TT_WEBHOOK_URL = os.getenv("TT_WEBHOOK_URL")   # <<<CHANGE_ME: TRADETRON_WEBHOOK_URL>>> e.g., https://api.tradetron.tech/api
TT_API_TOKEN   = os.getenv("TT_API_TOKEN")     # <<<CHANGE_ME: TRADETRON_API_TOKEN>>>

# ---- Kotak credentials (Render → Environment) ----
NEO_CONSUMER_KEY = os.getenv("NEO_CONSUMER_KEY")  # <<<CHANGE_ME: KOTAK_CONSUMER_KEY>>>
NEO_USERNAME     = os.getenv("NEO_USERNAME")      # <<<CHANGE_ME: KOTAK_LOGIN_USER_OR_MOBILE>>>
NEO_PASSWORD     = os.getenv("NEO_PASSWORD")      # <<<CHANGE_ME: KOTAK_LOGIN_PASSWORD>>>
NEO_TOTP_SECRET  = os.getenv("NEO_TOTP_SECRET")   # <<<CHANGE_ME: KOTAK_TOTP_SECRET_BASE32>>>

# Optional: base URL override if Kotak ever changes hostnames (usually not needed)
NEO_BASE_URL     = os.getenv("NEO_BASE_URL", "https://napi.kotaksecurities.com")  # default based on devportal [3](https://tradenvesteasy.com/fetch-live-option-chain-using-kotak-neo-api/)

IST = timezone(timedelta(hours=5, minutes=30))
app = FastAPI()
log = logging.getLogger("uvicorn")
logging.basicConfig(level=logging.INFO)

# Rolling price buffers
under_buf = deque(maxlen=3600)                  # underlying FUT
hist: Dict[str, deque] = defaultdict(lambda: deque(maxlen=3600))  # per option symbol history
last_alert_ts: Optional[datetime] = None

# Cached discovery
_discovered = {
    "fut": None,            # GOLDM FUT tradingsymbol (front month)
    "opts_monthly": [],     # list of monthly CE/PE tradingsymbols
    "expiry": None          # nearest monthly expiry datetime
}

# ---------------- Kotak Client (v2 SDK) ----------------
class KotakClient:
    def __init__(self):
        miss = [k for k, v in {
            "NEO_CONSUMER_KEY": NEO_CONSUMER_KEY,
            "NEO_USERNAME": NEO_USERNAME,
            "NEO_PASSWORD": NEO_PASSWORD,
            "NEO_TOTP_SECRET": NEO_TOTP_SECRET
        }.items() if not v]
        if miss:
            raise RuntimeError(f"Missing Kotak env vars: {', '.join(miss)}")

        from neo_api_client import NeoAPI  # installed during build
        self.NeoAPI = NeoAPI
        self.client = None
        self.logged_in = False

    def login(self):
        import pyotp
        totp_code = pyotp.TOTP(NEO_TOTP_SECRET).now()
        self.client = self.NeoAPI(environment='prod', access_token=None, neo_fin_key=None,
                                  consumer_key=NEO_CONSUMER_KEY)
        # Login + 2FA TOTP
        self.client.login(mobilenumber=NEO_USERNAME, password=NEO_PASSWORD)
        self.client.session_2fa(OTP=str(totp_code))
        self.logged_in = True
        log.info("Kotak Neo login successful.")

    def ensure(self):
        if not self.logged_in:
            self.login()

    # ---- Quotes API (supports OI) ----
    def quotes(self, symbols: List[str], quote_type: str = "all") -> Dict[str, dict]:
        """
        Returns {symbol: {ltp, oi, bid, ask}}. 'quote_type' can include 'oi' / 'all'.
        As per v2 SDK docs, OI is supported. [1](https://niftyinvest.com/option-chain/GOLDM)
        """
        self.ensure()
        out = {}
        if not symbols:
            return out
        # chunk large lists to avoid payload limits
        CHUNK = 80
        for i in range(0, len(symbols), CHUNK):
            batch = symbols[i:i+CHUNK]
            try:
                if hasattr(self.client, "quotes_neo_symbol"):
                    q = self.client.quotes_neo_symbol(neo_symbol=batch, quote_type=quote_type)
                else:
                    tokens = [{"instrument_token": s, "exchange_segment": "cde_fo"} for s in batch]
                    q = self.client.quotes(instrument_tokens=tokens, quote_type=quote_type)
                for item in q:
                    sym = item.get("tradingsymbol") or item.get("symbol") or item.get("instrument_token")
                    ltp = item.get("last_price") or item.get("ltp")
                    oi  = item.get("oi") or item.get("open_interest")
                    bid = item.get("best_bid_price") or item.get("bid")
                    ask = item.get("best_ask_price") or item.get("ask")
                    if sym:
                        out[sym] = {
                            "ltp": float(ltp) if ltp is not None else None,
                            "oi": int(oi) if oi is not None else None,
                            "bid": float(bid) if bid is not None else None,
                            "ask": float(ask) if ask is not None else None
                        }
            except Exception as e:
                log.error(f"Kotak quotes error: {e}")
        return out

    # ---- Masterscrip / Discovery ----
    def discover_goldm(self) -> Tuple[Optional[str], List[str], Optional[datetime]]:
        """
        Auto-discovers GOLDM Futures (front month) and current monthly CE/PE symbols.
        Strategy:
          1) Try SDK 'scrip_search' for 'GOLDM' to get instruments (fast).
          2) If not available, call Masterscrip 'file-paths' and parse CSV(s). [2](https://upstox.com/option-chain/multi-commodity-exchange/)
        We return (fut_symbol, [options_monthly...], nearest_monthly_expiry)
        """
        self.ensure()

        # 1) Try scrip_search if SDK provides it
        try:
            if hasattr(self.client, "scrip_search"):
                res = self.client.scrip_search(searchstr="GOLDM")
                # Normalize to list of dicts
                instruments = res if isinstance(res, list) else []
                futs = []
                opts = []
                for r in instruments:
                    ts = r.get("tradingsymbol") or r.get("symbol") or ""
                    seg = r.get("exchange_segment") or r.get("segment") or ""
                    inst_type = (r.get("instrument_type") or r.get("type") or "").upper()
                    expiry = r.get("expiry") or r.get("expirty") or r.get("exp")
                    if not ts or "GOLDM" not in ts: 
                        continue
                    if seg in ("cde_fo", "MCX", "MCX-OPT", "MCX-FUT"):
                        if inst_type == "FUT":
                            futs.append((ts, expiry))
                        elif inst_type in ("CE", "PE"):
                            opts.append((ts, inst_type, expiry))
                fut_symbol = nearest_by_expiry_symbol(futs)
                opt_syms, nearest_exp = filter_monthly_series(opts)
                if fut_symbol and opt_syms:
                    return fut_symbol, opt_syms, nearest_exp
        except Exception as e:
            log.warning(f"scrip_search unavailable or failed: {e}")

        # 2) Fallback: Masterscrip file paths -> download CSVs and parse. [2](https://upstox.com/option-chain/multi-commodity-exchange/)
        try:
            # Get CSV file paths
            url = f"{NEO_BASE_URL}/script-details/1.0/masterscrip/file-paths"  # guide mentions this route
            # Depending on auth, some installations allow GET without token; otherwise require header/cookie.
            # We try a direct GET first; if 401, you may need to proxy via SDK (vendor-specific).
            r = requests.get(url, timeout=8)
            r.raise_for_status()
            data = r.json()
            # Heuristic: take commodity derivatives CSV link(s)
            csv_urls = []
            for k, v in (data.items() if isinstance(data, dict) else []):
                if isinstance(v, str) and ("MCX" in v or "commodity" in v.lower()):
                    csv_urls.append(v)
                elif isinstance(v, list):
                    csv_urls += [x for x in v if isinstance(x, str) and ("MCX" in x or "commodity" in x.lower())]
            instruments = []
            for csv_url in csv_urls:
                try:
                    cr = requests.get(csv_url, timeout=15)
                    cr.raise_for_status()
                    content = cr.content.decode("utf-8", errors="ignore")
                    reader = csv.DictReader(io.StringIO(content))
                    instruments.extend(list(reader))
                except Exception as e:
                    log.warning(f"masterscrip CSV fetch failed: {e}")

            # Parse instruments
            futs, opts = [], []
            for r in instruments:
                ts = (r.get("tradingsymbol") or r.get("symbol") or r.get("ts") or "").strip()
                if not ts or "GOLDM" not in ts:
                    continue
                seg = (r.get("segment") or r.get("exchange_segment") or r.get("exchange") or "").upper()
                inst_type = (r.get("instrument_type") or r.get("type") or "").upper()
                expiry = r.get("expiry") or r.get("exp") or r.get("maturity")
                if inst_type == "FUT":
                    futs.append((ts, expiry))
                elif inst_type in ("CE", "PE"):
                    opts.append((ts, inst_type, expiry))
            fut_symbol = nearest_by_expiry_symbol(futs)
            opt_syms, nearest_exp = filter_monthly_series(opts)
            if fut_symbol and opt_syms:
                return fut_symbol, opt_syms, nearest_exp

        except Exception as e:
            log.error(f"Masterscrip discovery failed: {e}")

        return None, [], None

# ---------- Discovery helpers ----------
def parse_expiry_dt(x) -> Optional[datetime]:
    try:
        # Try common formats: YYYY-MM-DD or DD-MMM-YYYY etc.
        x = str(x).strip()
        for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(x, fmt)
            except Exception:
                pass
        return None
    except Exception:
        return None

def nearest_by_expiry_symbol(pairs: List[Tuple[str, str]]) -> Optional[str]:
    # pairs: [(tradingsymbol, expiry_str), ...]
    future = []
    now = datetime.now(IST)
    for ts, e in pairs:
        dt = parse_expiry_dt(e)
        if dt and dt >= now:
            future.append((ts, dt))
    if not future:
        return None
    future.sort(key=lambda t: t[1])
    return future[0][0]

def filter_monthly_series(opts: List[Tuple[str, str, str]]) -> Tuple[List[str], Optional[datetime]]:
    """
    From all GOLDM options, pick the nearest monthly expiry set and return their tradingsymbols.
    """
    by_exp = defaultdict(list)
    now = datetime.now(IST)
    for ts, typ, e in opts:
        dt = parse_expiry_dt(e)
        if dt and dt >= now:
            by_exp[dt.date()].append(ts)
    if not by_exp:
        return [], None
    nearest = sorted(by_exp.keys())[0]
    # Return only that expiry's symbols
    syms = by_exp[nearest]
    return syms, datetime.combine(nearest, datetime.min.time()).replace(tzinfo=IST)

# ---------- Core utilities ----------
def pct_change(buf: deque, lookback_min: int) -> Optional[float]:
    if len(buf) < 2:
        return None
    cutoff = time.time() - lookback_min * 60
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
    data = {"key": TT_API_TOKEN, **payload}   # TT expects your token as 'key' + custom fields
    try:
        r = requests.post(TT_WEBHOOK_URL, headers=headers, data=json.dumps(data), timeout=3)
        log.info(f"TT webhook status={r.status_code}")
    except Exception as e:
        log.error(f"TT webhook error: {e}")

def best_by_oi_and_momo(side_suffix: str, quotes: Dict[str, dict], top_k: int) -> Optional[str]:
    """
    side_suffix: 'CE' or 'PE'
    - Rank by OI desc
    - Take top_k
    - Among them, pick highest positive 5-min momentum
    - Fallback to top OI if momentum not available
    """
    filtered = [(ts, meta) for ts, meta in quotes.items()
                if ts.endswith(side_suffix) and (meta.get("oi") is not None)]
    if not filtered:
        return None
    filtered.sort(key=lambda x: x[1]["oi"], reverse=True)
    top = filtered[:max(1, top_k)]
    best_sym, best_momo, any_momo = None, -1e9, False
    for ts, _ in top:
        momo = pct_change(hist[ts], LOOKBACK_MIN)
        if momo is not None:
            any_momo = True
            if momo > best_momo:
                best_momo = momo
                best_sym = ts
    return best_sym if any_momo and best_sym else top[0][0]

# ---------- Main engine ----------
async def engine_loop():
    broker = KotakClient()

    # 1) Discover GOLDM FUT + monthly options universe automatically
    fut_sym, opt_syms, exp = broker.discover_goldm()
    if not fut_sym or not opt_syms:
        log.error("Auto-discovery failed (GOLDM). Check masterscrip/scrip_search availability.")
        return
    _discovered["fut"], _discovered["opts_monthly"], _discovered["expiry"] = fut_sym, opt_syms, exp
    log.info(f"Discovered FUT={fut_sym}, monthly options={len(opt_syms)}, expiry={exp}")

    global last_alert_ts
    while True:
        try:
            now = datetime.now(IST)
            now_ts = time.time()

            # Underlying FUT LTP
            u_q = broker.quotes([fut_sym], quote_type="ltp").get(fut_sym, {})
            u = u_q.get("ltp")
            if u is not None:
                under_buf.append((now_ts, float(u)))

            # Options snapshot: OI + LTP
            q = broker.quotes(opt_syms, quote_type="all")
            # Maintain price histories
            for ts, meta in q.items():
                ltp = meta.get("ltp")
                if ltp is not None:
                    hist[ts].append((now_ts, float(ltp)))

            # Pick CE/PE by OI+momentum
            ce_pick = best_by_oi_and_momo("CE", q, TOP_K)
            pe_pick = best_by_oi_and_momo("PE", q, TOP_K)

            u_pct  = pct_change(under_buf, LOOKBACK_MIN)
            ce_pct = pct_change(hist[ce_pick], LOOKBACK_MIN) if ce_pick else None
            pe_pct = pct_change(hist[pe_pick], LOOKBACK_MIN) if pe_pick else None

            log.info(f"Pick CE={ce_pick} PE={pe_pick} | U%={u_pct} CE%={ce_pct} PE%={pe_pct}")

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
