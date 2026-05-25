from contextlib import asynccontextmanager
from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import asyncio
import httpx
import json
import re
import os
import time
from datetime import datetime, date
from urllib.parse import quote as url_quote

import yfinance as yf
from yfinance import Search as YfSearch

try:
    from top100_data import STOCKS as TOP100_EMBED
except ImportError:
    TOP100_EMBED = []

try:
    from orion_assistant import run_assistant_chat, build_single_ticker_verdict
except ImportError:
    run_assistant_chat = None  # type: ignore
    build_single_ticker_verdict = None  # type: ignore

try:
    from orion_report import build_stocks_report, build_industry_report, report_to_pdf
except ImportError:
    build_stocks_report = None  # type: ignore
    build_industry_report = None  # type: ignore
    report_to_pdf = None  # type: ignore


def _load_top100():
    global TOP_100
    TOP_100 = []
    if TOP100_FILE.exists():
        try:
            TOP_100 = json.loads(TOP100_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            print(f"TOP100 file error: {e}")
    if not TOP_100:
        TOP_100 = list(TOP100_EMBED)
        print(f"TOP100 using embedded list ({len(TOP_100)} stocks)")


@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_custom_tickers()
    _load_top100()
    print(f"ORION ready — universe={len(get_universe())} top100={len(TOP_100)}")
    yield
    _executor.shutdown(wait=False, cancel_futures=True)


app = FastAPI(title="ORION", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://samvruthc.github.io",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "http://localhost:5500",
        "http://127.0.0.1:5500",
        "*",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CUSTOM_TICKERS_FILE = DATA_DIR / "custom_tickers.json"
TOP100_FILE = DATA_DIR / "top100.json"

if DATA_DIR.is_dir():
    app.mount("/data", StaticFiles(directory=str(DATA_DIR)), name="data")

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
TOP_100: List[Dict] = []


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


def _quote_from_chart_meta(ticker: str, meta: Dict[str, Any]) -> Dict[str, Any]:
    price = meta.get("regularMarketPrice")
    prev = (
        meta.get("regularMarketPreviousClose")
        or meta.get("previousClose")
        or meta.get("chartPreviousClose")
    )
    chg = meta.get("regularMarketChange")
    chg_pct = meta.get("regularMarketChangePercent")
    if chg_pct is None and price is not None and prev:
        chg_pct = ((price - prev) / prev) * 100
    if chg is None and price is not None and prev:
        chg = price - prev
    return {
        "ticker": ticker,
        "price": price,
        "change": chg,
        "changePct": chg_pct,
        "volume": meta.get("regularMarketVolume"),
        "fiftyTwoWeekHigh": meta.get("fiftyTwoWeekHigh"),
        "fiftyTwoWeekLow": meta.get("fiftyTwoWeekLow"),
        "name": meta.get("longName") or meta.get("shortName") or ticker,
        "sector": meta.get("exchangeName") or "Unknown",
    }


def _fetch_chart_quote_sync(ticker: str) -> Dict[str, Any]:
    """Single-ticker quote via Yahoo v8 chart (reliable on servers)."""
    ticker = ticker.upper()
    key = f"quote:{ticker}"
    cached = _cache_get(key, 45)
    if cached:
        return cached

    sym = url_quote(ticker, safe="")
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
        "?range=1d&interval=1d&includePrePost=false"
    )
    q: Dict[str, Any] = {"ticker": ticker}
    try:
        with httpx.Client(timeout=15, headers=YF_HEADERS) as client:
            r = client.get(url)
        if r.status_code == 200:
            result = r.json().get("chart", {}).get("result", [])
            if result:
                q = _quote_from_chart_meta(ticker, result[0].get("meta") or {})
    except Exception as e:
        print(f"CHART QUOTE ERROR {ticker}: {e}")

    if q.get("price") is None:
        try:
            t = yf.Ticker(ticker)
            fi = dict(t.fast_info)
            info = t.info or {}
            price = fi.get("lastPrice") or info.get("currentPrice") or info.get("regularMarketPrice")
            prev = fi.get("regularMarketPreviousClose") or fi.get("previousClose")
            if price is not None:
                q["price"] = price
                if prev:
                    q["change"] = price - prev
                    q["changePct"] = ((price - prev) / prev) * 100
            q["marketCap"] = fi.get("marketCap") or info.get("marketCap")
            q["peRatio"] = info.get("trailingPE")
            q["name"] = info.get("longName") or info.get("shortName") or q.get("name") or ticker
            q["sector"] = info.get("sector") or info.get("industry") or q.get("sector")
            q["volume"] = fi.get("lastVolume") or info.get("volume")
            q["fiftyTwoWeekHigh"] = fi.get("yearHigh") or info.get("fiftyTwoWeekHigh")
            q["fiftyTwoWeekLow"] = fi.get("yearLow") or info.get("fiftyTwoWeekLow")
        except Exception as e:
            print(f"YF FALLBACK ERROR {ticker}: {e}")
    else:
        try:
            info = yf.Ticker(ticker).info or {}
            q["marketCap"] = info.get("marketCap")
            q["peRatio"] = info.get("trailingPE")
            q["sector"] = info.get("sector") or info.get("industry") or q.get("sector")
        except Exception:
            pass

    _cache_set(key, q)
    return q


def _fetch_quote_sync(ticker: str) -> Dict[str, Any]:
    return _fetch_chart_quote_sync(ticker)


async def _fetch_chart_quote_async(ticker: str) -> Dict[str, Any]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _fetch_chart_quote_sync, ticker)


async def fetch_quotes(tickers: List[str]) -> Dict[str, Dict]:
    """Batch quotes: Yahoo spark first, chart fallback, optional yfinance fundamentals."""
    tickers = [t.upper() for t in tickers if t]
    if not tickers:
        return {}

    out = await _fetch_spark_quotes(tickers)
    missing = [t for t in tickers if not out.get(t) or out[t].get("price") is None]
    for t in missing[:30]:
        cq = await _fetch_chart_quote_async(t)
        if cq.get("price") is not None:
            out[t] = {**out.get(t, {}), **cq}

    still = [t for t in tickers if not out.get(t) or out[t].get("price") is None]
    if still:
        loop = asyncio.get_running_loop()
        tasks = [loop.run_in_executor(_executor, _fetch_chart_quote_sync, t) for t in still[:12]]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for t, r in zip(still[:12], results):
            if isinstance(r, dict) and r.get("price") is not None:
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


def _signal_score_only(q: Dict) -> float:
    return _compute_signals(q)["score"]


def _recommendation_label(score: float) -> str:
    if score >= 70:
        return "BUY"
    if score >= 55:
        return "HOLD"
    return "SELL"


def _matches_industry(meta: Dict, quote: Dict, industry: str) -> bool:
    needle = industry.lower().strip()
    if not needle:
        return False
    for field in (meta.get("industry"), quote.get("sector"), meta.get("sector")):
        if field and needle in str(field).lower():
            return True
    return False


async def _fetch_spark_quotes(tickers: List[str]) -> Dict[str, Dict]:
    """Fast batch prices via Yahoo spark (no per-ticker yfinance)."""
    out: Dict[str, Dict] = {}
    async with httpx.AsyncClient(timeout=20, headers=YF_HEADERS) as client:
        for i in range(0, len(tickers), 40):
            batch = tickers[i : i + 40]
            symbols = ",".join(batch)
            url = (
                "https://query1.finance.yahoo.com/v7/finance/spark"
                f"?symbols={symbols}&range=1d&interval=1d"
            )
            try:
                r = await client.get(url)
                if r.status_code != 200:
                    raise RuntimeError(f"spark HTTP {r.status_code}")
                payload = r.json()
                for item in payload.get("spark", {}).get("result", []):
                    sym = item.get("symbol")
                    if not sym:
                        continue
                    resp = (item.get("response") or [{}])[0]
                    meta = resp.get("meta") or {}
                    price = meta.get("regularMarketPrice")
                    prev = (
                        meta.get("regularMarketPreviousClose")
                        or meta.get("previousClose")
                        or meta.get("chartPreviousClose")
                    )
                    chg_pct = meta.get("regularMarketChangePercent")
                    chg = meta.get("regularMarketChange")
                    if chg_pct is None and price is not None and prev:
                        chg_pct = (price - prev) / prev * 100
                    if chg is None and price is not None and prev:
                        chg = price - prev
                    out[sym] = {
                        "ticker": sym,
                        "price": price,
                        "change": chg,
                        "changePct": chg_pct,
                        "name": meta.get("longName") or meta.get("shortName") or sym,
                        "volume": meta.get("regularMarketVolume"),
                        "fiftyTwoWeekHigh": meta.get("fiftyTwoWeekHigh"),
                        "fiftyTwoWeekLow": meta.get("fiftyTwoWeekLow"),
                    }
            except Exception as e:
                print(f"SPARK ERROR batch {i}: {e}")
                try:
                    url2 = (
                        "https://query2.finance.yahoo.com/v7/finance/spark"
                        f"?symbols={symbols}&range=1d&interval=1d"
                    )
                    r2 = await client.get(url2)
                    if r2.status_code == 200:
                        for item in r2.json().get("spark", {}).get("result", []):
                            sym = item.get("symbol")
                            if not sym:
                                continue
                            meta = (item.get("response") or [{}])[0].get("meta") or {}
                            price = meta.get("regularMarketPrice")
                            prev = (
                                meta.get("regularMarketPreviousClose")
                                or meta.get("previousClose")
                                or meta.get("chartPreviousClose")
                            )
                            chg_pct = meta.get("regularMarketChangePercent")
                            chg = meta.get("regularMarketChange")
                            if chg_pct is None and price is not None and prev:
                                chg_pct = (price - prev) / prev * 100
                            if chg is None and price is not None and prev:
                                chg = price - prev
                            out[sym] = {
                                "ticker": sym,
                                "price": price,
                                "change": chg,
                                "changePct": chg_pct,
                                "name": meta.get("longName") or meta.get("shortName") or sym,
                                "volume": meta.get("regularMarketVolume"),
                                "fiftyTwoWeekHigh": meta.get("fiftyTwoWeekHigh"),
                                "fiftyTwoWeekLow": meta.get("fiftyTwoWeekLow"),
                            }
                except Exception as e2:
                    print(f"SPARK Q2 ERROR: {e2}")
    return out


async def _fetch_top100_quotes() -> Dict[str, Dict]:
    key = "top100:quotes"
    cached = _cache_get(key, 120)
    if cached:
        return cached

    if not TOP_100:
        _load_top100()

    tickers = [s["ticker"] for s in TOP_100]
    merged = await _fetch_spark_quotes(tickers)

    missing = [t for t in tickers if not merged.get(t) or merged[t].get("price") is None]
    for t in missing[:20]:
        cq = await _fetch_chart_quote_async(t)
        if cq.get("price") is not None:
            merged[t] = {**merged.get(t, {}), **cq}

    by_ticker = {s["ticker"]: s for s in TOP_100}
    out = {}
    for t in tickers:
        q = merged.get(t, {})
        meta = by_ticker.get(t, {})
        if q.get("price") is None:
            continue
        out[t] = {
            **q,
            "ticker": t,
            "name": q.get("name") or meta.get("name") or t,
            "industry": meta.get("industry") or q.get("sector") or "Unknown",
            "peRatio": q.get("peRatio"),
        }
    _cache_set(key, out)
    return out


def _enrich_pick(ticker: str, q: Dict, meta: Dict) -> Dict:
    score = _signal_score_only(q)
    chg = q.get("changePct") or 0
    return {
        "ticker": ticker,
        "name": q.get("name") or meta.get("name") or ticker,
        "industry": meta.get("industry") or q.get("sector") or "Unknown",
        "price": q.get("price"),
        "changePct": chg,
        "marketCap": q.get("marketCap"),
        "peRatio": q.get("peRatio"),
        "score": score,
        "recommendation": _recommendation_label(score),
        "rationale": _pick_rationale(q, score),
    }


def _pick_rationale(q: Dict, score: float) -> str:
    pe = q.get("peRatio")
    chg = q.get("changePct") or 0
    parts = []
    if pe and pe < 22:
        parts.append(f"attractive P/E ({pe:.1f}x)")
    elif pe and pe > 35:
        parts.append(f"premium P/E ({pe:.1f}x)")
    if chg > 1.5:
        parts.append(f"+{chg:.1f}% session momentum")
    elif chg < -1.5:
        parts.append(f"{chg:.1f}% pullback")
    hi, lo, price = q.get("fiftyTwoWeekHigh"), q.get("fiftyTwoWeekLow"), q.get("price")
    if hi and lo and price and hi != lo:
        pct = (price - lo) / (hi - lo) * 100
        if pct > 75:
            parts.append("near 52W high")
        elif pct < 30:
            parts.append("near 52W low")
    if not parts:
        parts.append(f"ORION signal {score:.0f}/100")
    return "; ".join(parts).capitalize()


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

    key_insight = bull[0] if bull else thesis[:200]
    if range_pct > 85:
        key_insight = (
            f"Price is {range_pct:.0f}% through its 52-week range — crowded long; "
            f"limited upside to ${hi52:.2f} unless estimates rise."
        )
    elif range_pct < 25:
        key_insight = (
            f"Near the low end of its 52-week band — asymmetric rebound potential "
            f"toward ${hi52:.2f} if earnings hold."
        )

    entry = lo52 * 1.03 if lo52 else price * 0.9
    if reco == "BUY":
        action = f"Scale in toward ${entry:.2f}; avoid chasing above ${price * 1.04:.2f}."
    elif reco == "SELL":
        action = f"Avoid new money at ${price:.2f}; revisit near ${entry:.2f} only if fundamentals improve."
    else:
        action = f"Hold only at ${price:.2f}; add on pullback toward ${entry:.2f}, not on strength."

    catalysts = [
        f"Next earnings / guidance for {name} (margins, outlook, capital return)",
        f"{sector} sector flows vs interest rates and demand",
        f"Technical: hold above ${lo52:.2f} support; ${hi52:.2f} is the key ceiling" if hi52 and lo52 else "Technical trend vs 20-day average",
        "Estimate revisions and institutional positioning",
    ]

    risks = list(bear)
    if pe and pe > 35:
        risks.append(f"At {pe:.1f}x P/E, multiple compression on any growth scare.")
    if lo52:
        risks.append(f"Close below ${lo52:.2f} invalidates a constructive range view.")
    risks.append("Macro shock (rates, recession) can override stock-specific strength.")

    return {
        "generated_at": str(datetime.utcnow()),
        "ticker": ticker,
        "name": name,
        "score": score,
        "memo": {
            "recommendation": reco,
            "conviction": conviction,
            "thesis": thesis,
            "keyInsight": key_insight,
            "action": action,
            "bull_case": bull,
            "bear_case": bear,
            "catalysts": catalysts,
            "risks": risks[:5],
        },
    }


# ---------- ROUTES ----------


@app.get("/api/orion/health")
async def health():
    return {
        "status": "ok",
        "universe": len(get_universe()),
        "features": {
            "chat": run_assistant_chat is not None,
            "verdict": build_single_ticker_verdict is not None,
            "brain": "v2",
            "reports": build_stocks_report is not None,
        },
    }


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

    # Exact ticker in query (e.g. "NVDA" or "is nvda a buy") — validate via live quote
    tick_guess = re.findall(r"\b[A-Z]{1,5}(?:-[A-Z])?\b", q_upper)
    if re.fullmatch(r"[A-Z]{1,5}(?:-[A-Z])?", q_upper):
        tick_guess = [q_upper] + tick_guess
    for sym in tick_guess[:4]:
        if sym in seen:
            continue
        try:
            data = await fetch_quotes([sym])
            info = data.get(sym, {})
            if info.get("price") is not None or info.get("name"):
                merged.insert(
                    0,
                    {
                        "ticker": sym,
                        "name": info.get("name") or sym,
                        "sector": info.get("sector") or "Equity",
                    },
                )
                seen.add(sym)
        except Exception:
            pass

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


# ---------- REPORTS (multi-stock / industry + PDF) ----------


class ReportRequestIn(BaseModel):
    mode: str = Field(..., description="'stocks' or 'industry'")
    tickers: List[str] = Field(default_factory=list)
    industry: Optional[str] = None
    limit: int = Field(default=12, ge=1, le=20)


async def _generate_report_payload(body: ReportRequestIn) -> Dict[str, Any]:
    if build_stocks_report is None or build_industry_report is None:
        return {"error": "Report module unavailable", "report": None}

    mode = (body.mode or "").strip().lower()
    if mode == "stocks":
        tickers = list(dict.fromkeys(t.strip().upper() for t in body.tickers if t and t.strip()))[:15]
        if not tickers:
            return {"error": "Provide at least one ticker", "report": None}
        quotes = await fetch_quotes(tickers)
        report = build_stocks_report(tickers, quotes, _build_analysis)
        if not report.get("sections"):
            return {"error": "Could not load quotes for any ticker", "report": None}
        return {"report": report}

    if mode == "industry":
        industry = (body.industry or "").strip()
        if not industry:
            return {"error": "Industry name required", "report": None}
        if not TOP_100:
            _load_top100()
        try:
            quotes = await asyncio.wait_for(_fetch_top100_quotes(), timeout=28.0)
        except asyncio.TimeoutError:
            quotes = {}
        meta_by = {s["ticker"]: s for s in TOP_100}
        picks: List[Dict] = []
        for t, q in quotes.items():
            meta = meta_by.get(t, {})
            if not _matches_industry(meta, q, industry) or q.get("price") is None:
                continue
            picks.append(_enrich_pick(t, q, meta))
        picks.sort(key=lambda x: x.get("score") or 0, reverse=True)
        picks = picks[: body.limit]
        if not picks:
            return {"error": f"No names found for industry “{industry}”", "report": None}
        tickers = [p["ticker"] for p in picks]
        subset = {t: quotes.get(t, {}) for t in tickers}
        report = build_industry_report(industry, picks, subset, _build_analysis)
        return {"report": report}

    return {"error": "mode must be 'stocks' or 'industry'", "report": None}


@app.post("/api/orion/report")
async def generate_report(body: ReportRequestIn = Body(...)):
    return await _generate_report_payload(body)


@app.post("/api/orion/report/pdf")
async def generate_report_pdf(body: ReportRequestIn = Body(...)):
    if report_to_pdf is None:
        return Response(content=b"PDF unavailable", status_code=503)
    payload = await _generate_report_payload(body)
    if payload.get("error") or not payload.get("report"):
        return Response(
            content=(payload.get("error") or "Report failed").encode(),
            status_code=400,
            media_type="text/plain",
        )
    pdf_bytes = report_to_pdf(payload["report"])
    fname = f"orion-report-{datetime.utcnow().strftime('%Y%m%d-%H%M')}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.post("/api/orion/report/pdf/render")
async def render_report_pdf(body: dict = Body(...)):
    """Render PDF from an existing report JSON (e.g. client-built report)."""
    if report_to_pdf is None:
        return Response(content=b"PDF unavailable", status_code=503)
    report = body.get("report")
    if not report or not report.get("sections"):
        return Response(content=b"Missing report sections", status_code=400)
    pdf_bytes = report_to_pdf(report)
    fname = f"orion-report-{datetime.utcnow().strftime('%Y%m%d-%H%M')}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


# ---------- TOP 100 · TRENDING · INDUSTRY ----------


@app.get("/api/orion/top100")
async def top100_list():
    return {"stocks": TOP_100, "count": len(TOP_100)}


@app.get("/api/orion/industries")
async def industries():
    if not TOP_100:
        _load_top100()
    inds = sorted({s.get("industry", "Unknown") for s in TOP_100 if s.get("industry")})
    return {"industries": inds}


@app.get("/api/orion/trending")
async def trending(limit: int = 12):
    """Hot picks: top movers by % change among top 100."""
    limit = max(1, min(limit, 25))
    if not TOP_100:
        _load_top100()
    try:
        quotes = await asyncio.wait_for(_fetch_top100_quotes(), timeout=28.0)
    except asyncio.TimeoutError:
        quotes = {}
    meta_by = {s["ticker"]: s for s in TOP_100}
    picks = []
    for t, q in quotes.items():
        if q.get("price") is None:
            continue
        picks.append(_enrich_pick(t, q, meta_by.get(t, {})))
    picks.sort(key=lambda x: x.get("changePct") or 0, reverse=True)
    return {
        "picks": picks[:limit],
        "universe": "Top 100 US large caps",
        "updatedAt": str(datetime.utcnow()),
    }


@app.get("/api/orion/recommendations")
async def recommendations(industry: str, limit: int = 8):
    """Industry recommendations ranked by ORION signal score."""
    limit = max(1, min(limit, 15))
    if not industry.strip():
        return {"error": "industry query required", "picks": []}

    try:
        quotes = await asyncio.wait_for(_fetch_top100_quotes(), timeout=28.0)
    except asyncio.TimeoutError:
        quotes = {}
    meta_by = {s["ticker"]: s for s in TOP_100}
    candidates = []
    for t, q in quotes.items():
        meta = meta_by.get(t, {})
        if not _matches_industry(meta, q, industry):
            continue
        if q.get("price") is None:
            continue
        candidates.append(_enrich_pick(t, q, meta))

    candidates.sort(key=lambda x: x.get("score") or 0, reverse=True)
    return {
        "industry": industry,
        "picks": candidates[:limit],
        "scanned": len(candidates),
        "updatedAt": str(datetime.utcnow()),
    }


# ---------- PORTFOLIO ----------


class PortfolioPositionIn(BaseModel):
    ticker: str
    shares: float = Field(gt=0)
    avgCost: float = Field(ge=0)


class PortfolioEvaluateIn(BaseModel):
    positions: List[PortfolioPositionIn] = []


class ChatMessageIn(BaseModel):
    message: str = Field(..., min_length=1, max_length=2000)
    ticker: Optional[str] = None
    history: Optional[List[Dict[str, str]]] = None


async def _evaluate_portfolio(positions: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not positions:
        return {
            "positions": [],
            "summary": {
                "totalValue": 0,
                "totalCost": 0,
                "totalPnL": 0,
                "totalPnLPct": None,
                "dayPnL": 0,
                "positionCount": 0,
            },
            "updatedAt": str(datetime.utcnow()),
        }

    tickers = list({p["ticker"].upper() for p in positions})
    quotes = await fetch_quotes(tickers)

    evaluated: List[Dict[str, Any]] = []
    total_value = 0.0
    total_cost = 0.0
    total_day_pnl = 0.0

    for p in positions:
        t = p["ticker"].upper()
        shares = float(p["shares"])
        avg_cost = float(p.get("avgCost", p.get("avg_cost", 0)))
        q = quotes.get(t, {})
        price = q.get("price") or 0.0
        change = q.get("change") or 0.0

        market_value = shares * price
        cost_basis = shares * avg_cost
        unrealized_pnl = market_value - cost_basis
        unrealized_pnl_pct = (unrealized_pnl / cost_basis * 100) if cost_basis > 0 else None
        day_pnl = shares * change

        total_value += market_value
        total_cost += cost_basis
        total_day_pnl += day_pnl

        evaluated.append({
            "ticker": t,
            "name": q.get("name") or t,
            "shares": shares,
            "avgCost": avg_cost,
            "price": price,
            "marketValue": market_value,
            "costBasis": cost_basis,
            "unrealizedPnL": unrealized_pnl,
            "unrealizedPnLPct": unrealized_pnl_pct,
            "dayPnL": day_pnl,
            "changePct": q.get("changePct"),
            "weight": 0.0,
        })

    if total_value > 0:
        for row in evaluated:
            row["weight"] = row["marketValue"] / total_value * 100

    total_pnl = total_value - total_cost
    total_pnl_pct = (total_pnl / total_cost * 100) if total_cost > 0 else None

    return {
        "positions": evaluated,
        "summary": {
            "totalValue": total_value,
            "totalCost": total_cost,
            "totalPnL": total_pnl,
            "totalPnLPct": total_pnl_pct,
            "dayPnL": total_day_pnl,
            "positionCount": len(evaluated),
        },
        "updatedAt": str(datetime.utcnow()),
    }


async def _fetch_news_async(ticker: str) -> List[Dict]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, _fetch_news_sync, ticker.upper())


@app.get("/api/orion/verdict/{ticker}")
async def orion_verdict(ticker: str, q: str = ""):
    """
    ORION brain — direct BUY/HOLD/AVOID for one ticker.
    Example: /api/orion/verdict/SNPS?q=is+SNPS+a+good+buy
    """
    if build_single_ticker_verdict is None:
        return {"error": "assistant_unavailable", "reply": "Deploy orion_assistant.py on the server."}
    if not TOP_100:
        _load_top100()
    question = (q or "").strip() or f"Is {ticker.upper()} a good buy?"
    return await build_single_ticker_verdict(
        ticker.upper(),
        question,
        top100=TOP_100 or list(TOP100_EMBED),
        fetch_quotes_fn=fetch_quotes,
        fetch_news_fn=_fetch_news_async,
        compute_signals_fn=_compute_signals,
        build_analysis_fn=_build_analysis,
    )


@app.get("/api/orion/chat/suggestions")
async def chat_suggestions(ticker: Optional[str] = None):
    t = (ticker or "NVDA").upper()
    return {
        "suggestions": [
            f"What is the valuation and risk profile for {t}?",
            f"Summarize {t} with latest news and confidence score",
            f"Compare {t} to industry peers today",
            "What are semiconductor industry trends right now?",
            "Top movers in the top 100 today",
            "Is the P/E attractive and what are key downside risks?",
        ],
    }


@app.get("/api/orion/chat")
async def chat_research_get(message: str, ticker: Optional[str] = None):
    """GET fallback for clients that cannot POST (or older proxies)."""
    return await chat_research(ChatMessageIn(message=message, ticker=ticker))


@app.post("/api/orion/chat")
async def chat_research(body: ChatMessageIn = Body(...)):
    """AI stock research assistant — live quotes, valuation, risk, news, confidence."""
    if run_assistant_chat is None:
        return {
            "error": "assistant_unavailable",
            "reply": "Research assistant module not loaded on server.",
        }

    explicit = (body.ticker or "").upper().strip() or None
    return await run_assistant_chat(
        body.message,
        ticker=explicit,
        history=body.history,
        universe=get_universe(),
        top100=TOP_100 or list(TOP100_EMBED),
        fetch_quotes_fn=fetch_quotes,
        fetch_news_fn=_fetch_news_async,
        compute_signals_fn=_compute_signals,
        build_analysis_fn=_build_analysis,
        fetch_top100_quotes_fn=_fetch_top100_quotes,
    )


@app.post("/api/orion/portfolio/evaluate")
async def portfolio_evaluate(payload: PortfolioEvaluateIn = Body(...)):
    """Live P&L for a list of holdings (shares + average cost per share)."""
    positions = [
        {"ticker": p.ticker.upper(), "shares": p.shares, "avgCost": p.avgCost}
        for p in payload.positions
    ]
    return await _evaluate_portfolio(positions)


@app.get("/api/orion/portfolio/evaluate")
async def portfolio_evaluate_get(
    tickers: str,
    shares: str,
    costs: str,
):
    """GET fallback: tickers=NVDA,AAPL shares=10,5 costs=100,200"""
    t_list = [x.strip().upper() for x in tickers.split(",") if x.strip()]
    s_list = [float(x) for x in shares.split(",") if x.strip()]
    c_list = [float(x) for x in costs.split(",") if x.strip()]
    if len(t_list) != len(s_list) or len(t_list) != len(c_list):
        return {"error": "tickers, shares, and costs must have same length"}
    positions = [
        {"ticker": t, "shares": s, "avgCost": c}
        for t, s, c in zip(t_list, s_list, c_list)
    ]
    return await _evaluate_portfolio(positions)


if __name__ == "__main__":
    import uvicorn

    # Programmatic start avoids the uvicorn Click CLI (fixes Railway crashes in click/core.py).
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        log_level="info",
        access_log=True,
    )
