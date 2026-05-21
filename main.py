from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from typing import Dict, Any
import httpx
import time

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Yahoo headers
YF_HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# Simple in-memory cache
CACHE = {}

def _cache_get(key, ttl):
    item = CACHE.get(key)

    if not item:
        return None

    value, ts = item

    if time.time() - ts > ttl:
        del CACHE[key]
        return None

    return value

def _cache_set(key, value):
    CACHE[key] = (value, time.time())

# Simple logger
def log_agent(agent, action, target=""):
    print(f"[{agent}] {action} {target}")
async def yf_quote(ticker: str) -> Dict[str, Any]:
    """Use Yahoo v8 chart endpoint (open, no auth) and extract meta + last bar as a quote.
    Augment with quoteSummary for marketCap/PE when possible (best-effort)."""
    key = f"quote:{ticker}"
    cached = _cache_get(key, 30)
    if cached: return cached
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1d&interval=1m&includePrePost=false"
    async with httpx.AsyncClient(timeout=10, headers=YF_HEADERS) as c:
        r = await c.get(url)
        r.raise_for_status()
        d = r.json()
    chart = (d.get("chart", {}).get("result") or [None])[0]
    meta = (chart or {}).get("meta", {}) if chart else {}
    price = meta.get("regularMarketPrice")
    prev = meta.get("chartPreviousClose") or meta.get("previousClose")
    change = (price - prev) if (price is not None and prev) else None
    change_pct = (change / prev * 100) if (change is not None and prev) else None
    out = {
        "ticker": ticker,
        "price": price,
        "change": change,
        "changePct": change_pct,
        "prevClose": prev,
        "open": (meta.get("regularMarketDayLow")),  # placeholder if unavailable
        "dayHigh": meta.get("regularMarketDayHigh"),
        "dayLow": meta.get("regularMarketDayLow"),
        "volume": meta.get("regularMarketVolume"),
        "marketCap": None,
        "peRatio": None,
        "epsTrailing": None,
        "fiftyTwoWeekHigh": meta.get("fiftyTwoWeekHigh"),
        "fiftyTwoWeekLow": meta.get("fiftyTwoWeekLow"),
        "currency": meta.get("currency"),
        "exchange": meta.get("exchangeName") or meta.get("fullExchangeName"),
        "name": meta.get("longName") or meta.get("shortName"),
    }
    # Best-effort enrichment via quoteSummary
    try:
        url2 = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=summaryDetail,price,defaultKeyStatistics"
        async with httpx.AsyncClient(timeout=8, headers=YF_HEADERS) as c:
            r2 = await c.get(url2)
        if r2.status_code == 200:
            j = r2.json()
            res = (((j.get("quoteSummary") or {}).get("result") or [None]) or [None])[0] or {}
            sd = res.get("summaryDetail", {}) or {}
            pr = res.get("price", {}) or {}
            ks = res.get("defaultKeyStatistics", {}) or {}
            def _v(x): return x.get("raw") if isinstance(x, dict) else x
            out["marketCap"] = _v(sd.get("marketCap")) or _v(pr.get("marketCap"))
            out["peRatio"]   = _v(sd.get("trailingPE"))
            out["epsTrailing"] = _v(ks.get("trailingEps"))
            out["open"]      = _v(sd.get("open")) or out["open"]
    except Exception:
        pass
    _cache_set(key, out)
    log_agent("CRAWLER", "fetched quote", ticker)
    return out
