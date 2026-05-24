from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from typing import Dict, Any, List, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import asyncio
import httpx
import json
import time
from datetime import datetime, date

import yfinance as yf
from yfinance import Search as YfSearch

app = FastAPI(title="ORION")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CUSTOM_TICKERS_FILE = DATA_DIR / "custom_tickers.json"

YF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://finance.yahoo.com",
}

CACHE: Dict[str, Any] = {}
_executor = ThreadPoolExecutor(max_workers=8)

DEFAULT_UNIVERSE = [
    {"ticker": "NVDA", "name": "NVIDIA Corporation", "sector": "Semiconductors"},
    {"ticker": "AAPL", "name": "Apple Inc", "sector": "Consumer Tech"},
    {"ticker": "MSFT", "name": "Microsoft Corporation", "sector": "Software"},
    {"ticker": "AMZN", "name": "Amazon.com Inc", "sector": "E-Commerce"},
    {"ticker": "META", "name": "Meta Platforms", "sector": "Internet"},
    {"ticker": "GOOGL", "name": "Alphabet Inc", "sector": "Internet"},
    {"ticker": "TSLA", "name": "Tesla Inc", "sector": "Automotive"},
    {"ticker": "AMD", "name": "Advanced Micro Devices", "sector": "Semiconductors"},
    {"ticker": "NFLX", "name": "Netflix Inc", "sector": "Streaming"},
    {"ticker": "CRM", "name": "Salesforce Inc", "sector": "Software"},
]

CUSTOM_TICKERS: List[Dict] = []


def _cache_get(key: str, ttl: int = 30):
    item = CACHE.get(key)
    if not item:
        return None
    value, ts = item
    if time.time() - ts > ttl:
        del CACHE[key]
        return None
    return value


def _cache_set(key: str, value: Any):
    CACHE[key] = (value, time.time())


def _load_custom_tickers():
    global CUSTOM_TICKERS
    if not CUSTOM_TICKERS_FILE.exists():
        CUSTOM_TICKERS = []
        return
    try:
        CUSTOM_TICKERS = json.loads(CUSTOM_TICKERS_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        CUSTOM_TICKERS = []


def _save_custom_tickers():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CUSTOM_TICKERS_FILE.write_text(json.dumps(CUSTOM_TICKERS, indent=2))


@app.on_event("startup")
def startup():
    _load_custom_tickers()


def get_universe() -> List[Dict]:
    seen = set()
    result = []
    for c in DEFAULT_UNIVERSE + CUSTOM_TICKERS:
        t = c["ticker"]
        if t not in seen:
            seen.add(t)
            result.append(c)
    return result


def _fmt_cap(cap: Optional[float]) -> str:
    if not cap or cap <= 0:
        return ""
    if cap >= 1e12:
        return f"${cap / 1e12:.2f}T"
    if cap >= 1e9:
        return f"${cap / 1e9:.1f}B"
    if cap >= 1e6:
        return f"${cap / 1e6:.1f}M"
    return f"${cap:,.0f}"


def _fetch_quote_sync(ticker: str) -> Dict[str, Any]:
    """Fetch normalized quote via Yahoo (yfinance). No third-party API keys."""
    ticker = ticker.upper()
    key = f"quote:{ticker}"
    cached = _cache_get(key, 45)
    if cached:
        return cached

    try:
        t = yf.Ticker(ticker)
        fi = dict(t.fast_info)
        info = t.info or {}
    except Exception as e:
        print(f"QUOTE ERROR {ticker}: {e}")
        return {"ticker": ticker}

    price = fi.get("lastPrice")
    prev = fi.get("regularMarketPreviousClose") or fi.get("previousClose")
    change = (price - prev) if price is not None and prev else info.get("regularMarketChange")
    change_pct = None
    if change is not None and prev:
        change_pct = (change / prev) * 100
    elif info.get("regularMarketChangePercent") is not None:
        change_pct = info.get("regularMarketChangePercent")

    cap = fi.get("marketCap") or info.get("marketCap")
    pe = info.get("trailingPE")
    fpe = info.get("forwardPE")

    q = {
        "ticker": ticker,
        "price": price,
        "change": change,
        "changePct": change_pct,
        "marketCap": cap,
        "peRatio": pe,
        "forwardPE": fpe,
        "volume": fi.get("lastVolume") or info.get("volume"),
        "open": fi.get("open") or info.get("regularMarketOpen"),
        "high": fi.get("dayHigh") or info.get("regularMarketDayHigh"),
        "low": fi.get("dayLow") or info.get("regularMarketDayLow"),
        "fiftyTwoWeekHigh": fi.get("yearHigh") or info.get("fiftyTwoWeekHigh"),
        "fiftyTwoWeekLow": fi.get("yearLow") or info.get("fiftyTwoWeekLow"),
        "name": info.get("longName") or info.get("shortName") or ticker,
        "sector": info.get("sector") or info.get("industry") or "Unknown",
        "dividendYield": info.get("dividendYield"),
    }
    _cache_set(key, q)
    return q


async def fetch_quotes(tickers: List[str]) -> Dict[str, Dict]:
    tickers = [t.upper() for t in tickers if t]
    if not tickers:
        return {}
    loop = asyncio.get_running_loop()
    futures = [loop.run_in_executor(_executor, _fetch_quote_sync, t) for t in tickers]
    results = await asyncio.gather(*futures, return_exceptions=True)
    out = {}
    for t, r in zip(tickers, results):
        if isinstance(r, dict):
            out[t] = r
    return out


async def yf_chart(ticker: str, rng: str = "6mo") -> Dict:
    key = f"chart:{ticker}:{rng}"
    cached = _cache_get(key, 300)
    if cached:
        return cached
    try:
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
            f"?range={rng}&interval=1d&includePrePost=false"
        )
        async with httpx.AsyncClient(timeout=12, headers=YF_HEADERS) as client:
            r = await client.get(url)
        data = r.json()
        result = data["chart"]["result"][0]
        timestamps = result.get("timestamp", [])
        closes = result["indicators"]["quote"][0].get("close", [])
        out = {"timestamps": timestamps, "closes": closes}
        _cache_set(key, out)
        return out
    except Exception as e:
        print(f"CHART ERROR: {e}")
        return {"timestamps": [], "closes": []}


def _fetch_filings_sync(ticker: str) -> List[Dict]:
    key = f"filings:{ticker}"
    cached = _cache_get(key, 3600)
    if cached is not None:
        return cached
    try:
        raw = yf.Ticker(ticker).sec_filings or []
    except Exception as e:
        print(f"FILINGS ERROR {ticker}: {e}")
        raw = []
    filings = []
    for f in raw[:12]:
        d = f.get("date")
        if isinstance(d, date):
            date_str = d.isoformat()
        else:
            date_str = str(d) if d else ""
        filings.append({
            "form": f.get("type", "—"),
            "date": date_str,
            "description": f.get("title", ""),
            "url": f.get("edgarUrl") or f.get("exhibits", {}).get(f.get("type", ""), ""),
        })
    _cache_set(key, filings)
    return filings


def _fetch_news_sync(ticker: str) -> List[Dict]:
    key = f"news:{ticker}"
    cached = _cache_get(key, 300)
    if cached is not None:
        return cached
    try:
        raw = yf.Ticker(ticker).news or []
    except Exception as e:
        print(f"NEWS ERROR {ticker}: {e}")
        raw = []
    items = []
    for n in raw[:20]:
        c = n.get("content") or n
        title = c.get("title")
        if not title:
            continue
        link = (
            (c.get("clickThroughUrl") or {}).get("url")
            or (c.get("canonicalUrl") or {}).get("url")
            or f"https://finance.yahoo.com/quote/{ticker}/news/"
        )
        provider = (c.get("provider") or {}).get("displayName", "Yahoo Finance")
        items.append({
            "title": title,
            "link": link,
            "source": provider,
            "published": c.get("pubDate") or c.get("displayTime") or "",
        })
    _cache_set(key, items)
    return items


def _search_yahoo_sync(query: str) -> List[Dict]:
    try:
        s = YfSearch(query, max_results=8, news_count=0, raise_errors=False)
        out = []
        for q in s.quotes:
            sym = q.get("symbol")
            if not sym:
                continue
            out.append({
                "ticker": sym.upper(),
                "name": q.get("longname") or q.get("shortname") or sym,
                "sector": q.get("quoteType", "Equity"),
                "exchange": q.get("exchange"),
            })
        return out
    except Exception as e:
        print(f"SEARCH ERROR: {e}")
        return []


def _compute_signals(q: Dict) -> Dict:
    price = q.get("price") or 0
    pe = q.get("peRatio")
    cap = q.get("marketCap") or 0
    hi52 = q.get("fiftyTwoWeekHigh") or 0
    lo52 = q.get("fiftyTwoWeekLow") or 0
    chg_pct = q.get("changePct") or 0

    score = 50
    signals = []

    if pe and pe < 20:
        score += 10
        signals.append({
            "type": "VALUATION",
            "label": f"Attractive P/E of {pe:.1f}x — below market average",
            "confidence": 0.78,
            "severity": "info",
        })
    elif pe and pe > 40:
        score -= 8
        signals.append({
            "type": "VALUATION",
            "label": f"Elevated P/E of {pe:.1f}x — premium priced",
            "confidence": 0.72,
            "severity": "warn",
        })

    if hi52 and lo52 and price:
        range_pct = (price - lo52) / (hi52 - lo52) * 100 if hi52 != lo52 else 50
        if range_pct > 80:
            score += 8
            signals.append({
                "type": "MOMENTUM",
                "label": f"Near 52W high — strong momentum ({range_pct:.0f}th percentile)",
                "confidence": 0.81,
                "severity": "info",
            })
        elif range_pct < 20:
            score -= 5
            signals.append({
                "type": "MOMENTUM",
                "label": "Near 52W low — potential value or continued weakness",
                "confidence": 0.65,
                "severity": "warn",
            })

    if chg_pct and chg_pct > 2:
        score += 6
        signals.append({
            "type": "PRICE ACTION",
            "label": f"Strong session: +{chg_pct:.2f}% today",
            "confidence": 0.70,
            "severity": "info",
        })
    elif chg_pct and chg_pct < -2:
        score -= 6
        signals.append({
            "type": "PRICE ACTION",
            "label": f"Weak session: {chg_pct:.2f}% today — watch for follow-through",
            "confidence": 0.70,
            "severity": "warn",
        })

    if cap and cap > 1e12:
        score += 5
        signals.append({
            "type": "SIZE",
            "label": f"Mega-cap {_fmt_cap(cap)} — institutional liquidity premium",
            "confidence": 0.90,
            "severity": "info",
        })

    score = max(10, min(95, score))
    return {"score": score, "signals": signals}


def _build_analysis(q: Dict) -> Dict:
    ticker = q.get("ticker", "")
    price = q.get("price") or 0
    pe = q.get("peRatio")
    fpe = q.get("forwardPE")
    cap = q.get("marketCap") or 0
    hi52 = q.get("fiftyTwoWeekHigh") or 0
    lo52 = q.get("fiftyTwoWeekLow") or 0
    chg_pct = q.get("changePct") or 0
    name = q.get("name") or ticker
    sector = q.get("sector") or "Unknown"
    div = q.get("dividendYield")

    range_pct = (
        (price - lo52) / (hi52 - lo52) * 100
        if hi52 and lo52 and hi52 != lo52
        else 50
    )

    score = 50
    if pe and pe < 20:
        score += 15
    elif pe and pe > 50:
        score -= 15
    if range_pct > 70:
        score += 10
    elif range_pct < 30:
        score += 5
    if chg_pct and chg_pct > 0:
        score += 5
    score = max(10, min(95, score))

    reco = "BUY" if score >= 70 else "HOLD" if score >= 55 else "SELL"
    conviction = round(score / 10)

    bull = [
        f"Trading at {range_pct:.0f}th percentile of 52W range — "
        f"{'near highs showing momentum' if range_pct > 60 else 'potential value entry point'}",
    ]
    if cap and cap > 0:
        bull.append(
            f"Market cap {_fmt_cap(cap)} — "
            f"{'mega-cap stability and institutional coverage' if cap > 1e11 else 'growth profile with room to scale'}"
        )
    if pe and pe < 25:
        bull.append(f"Reasonable trailing P/E of {pe:.1f}x — valuation not stretched")
    if fpe and pe and fpe < pe:
        bull.append(f"Forward P/E of {fpe:.1f}x below trailing {pe:.1f}x — earnings growth expected")
    if div:
        bull.append(f"Dividend yield of {div * 100:.2f}% provides income floor")

    bear = []
    if hi52 and price:
        bear.append(
            f"Near 52W high at ${price:.2f} — limited upside to ${hi52:.2f}"
            if range_pct > 80
            else f"Below 52W high of ${hi52:.2f} — recovery thesis unproven"
        )
    if pe and pe > 30:
        bear.append(f"Elevated P/E of {pe:.1f}x requires continued earnings execution")
    bear.append(f"Macro rate environment poses headwind for {sector} sector valuations")

    cap_str = _fmt_cap(cap) if cap else "N/A"
    pe_str = f"trailing P/E of {pe:.1f}x and " if pe else ""
    thesis = (
        f"{name} ({ticker}) is trading at ${price:.2f}, at the {range_pct:.0f}th percentile "
        f"of its 52-week range (${lo52:.2f}–${hi52:.2f}). "
        f"With {pe_str}market cap {cap_str}, the stock "
        f"{'presents a compelling risk/reward' if score >= 60 else 'warrants caution at current levels'}."
    )

    return {
        "generated_at": str(datetime.utcnow()),
        "ticker": ticker,
        "name": name,
        "score": score,
        "memo": {
            "recommendation": reco,
            "conviction": conviction,
            "thesis": thesis,
            "bull_case": bull,
            "bear_case": bear,
            "catalysts": [
                f"Next earnings cycle for {name}",
                f"Sector rotation into {sector} on macro clarity",
                "Institutional rebalancing and index inclusion flows",
            ],
            "risks": [
                f"P/E of {pe:.1f}x leaves little room for earnings misses"
                if pe
                else "Limited valuation visibility without positive trailing earnings",
                "Macro slowdown could compress multiples sector-wide",
                f"52W range ${lo52:.2f}–${hi52:.2f} — break below ${lo52:.2f} signals trend reversal"
                if lo52
                else "Monitor support levels on elevated volatility",
            ],
        },
    }


# ---------- ROUTES ----------


@app.get("/api/orion/health")
async def health():
    return {"status": "ok", "universe": len(get_universe())}


@app.get("/")
async def spa():
    index = BASE_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    return {"status": "ORION backend online — place index.html next to main.py"}


@app.get("/api/orion/universe")
async def universe():
    return {"companies": get_universe()}


@app.get("/api/orion/universe/add")
async def add_ticker(ticker: str):
    ticker = ticker.upper().strip()
    if not ticker:
        return {"status": "invalid", "ticker": ticker}

    existing = {c["ticker"] for c in get_universe()}
    if ticker in existing:
        return {"status": "already_exists", "ticker": ticker}

    q = await fetch_quotes([ticker])
    info = q.get(ticker, {})
    if not info.get("price") and not info.get("name"):
        return {"status": "not_found", "ticker": ticker}

    company = {
        "ticker": ticker,
        "name": info.get("name") or ticker,
        "sector": info.get("sector") or "Unknown",
    }
    CUSTOM_TICKERS.append(company)
    _save_custom_tickers()
    return {"status": "added", "company": company}


@app.get("/api/orion/search")
async def search(q: str):
    q = (q or "").strip()
    if not q:
        return {"results": []}

    q_upper = q.upper()
    local = [
        c
        for c in get_universe()
        if q_upper in c["ticker"] or q_upper in c["name"].upper()
    ]

    loop = asyncio.get_running_loop()
    remote = await loop.run_in_executor(_executor, _search_yahoo_sync, q)

    seen = {c["ticker"] for c in local}
    merged = list(local)
    for r in remote:
        if r["ticker"] not in seen:
            seen.add(r["ticker"])
            merged.append(r)
    return {"results": merged[:15]}


@app.get("/api/orion/quotes")
async def quotes(tickers: str):
    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    data = await fetch_quotes(ticker_list)
    out = []
    for t in ticker_list:
        q = data.get(t, {"ticker": t})
        out.append({
            "ticker": t,
            "price": q.get("price"),
            "change": q.get("change"),
            "changePct": q.get("changePct"),
            "marketCap": q.get("marketCap"),
            "peRatio": q.get("peRatio"),
            "volume": q.get("volume"),
            "fiftyTwoWeekHigh": q.get("fiftyTwoWeekHigh"),
            "fiftyTwoWeekLow": q.get("fiftyTwoWeekLow"),
            "name": q.get("name"),
            "sector": q.get("sector"),
        })
    return {"quotes": out}


@app.get("/api/orion/quote/{ticker}")
async def quote(ticker: str):
    data = await fetch_quotes([ticker.upper()])
    q = data.get(ticker.upper(), {})
    return {
        "ticker": ticker.upper(),
        "price": q.get("price"),
        "change": q.get("change"),
        "changePct": q.get("changePct"),
        "marketCap": q.get("marketCap"),
        "peRatio": q.get("peRatio"),
        "forwardPE": q.get("forwardPE"),
        "volume": q.get("volume"),
        "open": q.get("open"),
        "high": q.get("high"),
        "low": q.get("low"),
        "fiftyTwoWeekHigh": q.get("fiftyTwoWeekHigh"),
        "fiftyTwoWeekLow": q.get("fiftyTwoWeekLow"),
        "name": q.get("name"),
        "sector": q.get("sector"),
        "dividendYield": q.get("dividendYield"),
    }


@app.get("/api/orion/chart/{ticker}")
async def chart(ticker: str, rng: str = "6mo"):
    return await yf_chart(ticker.upper(), rng)


@app.get("/api/orion/dashboard/{ticker}")
async def dashboard(ticker: str):
    ticker = ticker.upper()
    data = await fetch_quotes([ticker])
    q = data.get(ticker, {"ticker": ticker})
    c = await yf_chart(ticker)

    loop = asyncio.get_running_loop()
    filings = await loop.run_in_executor(_executor, _fetch_filings_sync, ticker)

    quote_out = {
        "ticker": ticker,
        "price": q.get("price"),
        "change": q.get("change"),
        "changePct": q.get("changePct"),
        "marketCap": q.get("marketCap"),
        "peRatio": q.get("peRatio"),
        "volume": q.get("volume"),
        "fiftyTwoWeekHigh": q.get("fiftyTwoWeekHigh"),
        "fiftyTwoWeekLow": q.get("fiftyTwoWeekLow"),
        "name": q.get("name"),
        "sector": q.get("sector"),
    }

    return {
        "quote": quote_out,
        "chart": c,
        "signals": _compute_signals(q),
        "filings": filings,
    }


@app.get("/api/orion/analysis/{ticker}")
async def analysis(ticker: str):
    ticker = ticker.upper()
    data = await fetch_quotes([ticker])
    q = data.get(ticker, {"ticker": ticker})
    return _build_analysis(q)


@app.get("/api/orion/news")
async def news(ticker: str = None):
    if not ticker:
        return {"news": []}
    loop = asyncio.get_running_loop()
    items = await loop.run_in_executor(_executor, _fetch_news_sync, ticker.upper())
    return {"news": items}


@app.get("/api/orion/agents/activity")
async def agents():
    return {
        "events": [
            {
                "agent": "CRAWLER",
                "action": "Synced quotes via Yahoo Finance",
                "target": "UNIVERSE",
                "level": "info",
                "ts": str(datetime.utcnow()),
            },
            {
                "agent": "SIGNAL",
                "action": "Computed signal scores from P/E + 52W range",
                "target": "SELECTED",
                "level": "info",
                "ts": str(datetime.utcnow()),
            },
            {
                "agent": "SYNTHESIS",
                "action": "Built valuation memo from live fundamentals",
                "target": "SELECTED",
                "level": "info",
                "ts": str(datetime.utcnow()),
            },
        ]
    }


@app.get("/api/orion/memo/{ticker}")
async def memo(ticker: str):
    return await analysis(ticker)


if __name__ == "__main__":
    import os
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
