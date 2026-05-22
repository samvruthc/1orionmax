from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any
import httpx
import time
import random
from datetime import datetime

app = FastAPI()

# ---------- CORS ----------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- CONFIG ----------
YF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://finance.yahoo.com/",
    "Origin": "https://finance.yahoo.com",
}

CACHE = {}

UNIVERSE = [
    {"ticker": "NVDA", "name": "NVIDIA", "sector": "Semiconductors"},
    {"ticker": "AAPL", "name": "Apple", "sector": "Consumer Tech"},
    {"ticker": "MSFT", "name": "Microsoft", "sector": "Software"},
    {"ticker": "AMZN", "name": "Amazon", "sector": "E-Commerce"},
    {"ticker": "META", "name": "Meta Platforms", "sector": "Internet"},
    {"ticker": "GOOGL", "name": "Alphabet", "sector": "Internet"},
    {"ticker": "TSLA", "name": "Tesla", "sector": "Automotive"},
]

# ---------- CACHE ----------
def _cache_get(key, ttl=30):
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


# ---------- FUNDAMENTALS ----------
# ---------- FUNDAMENTALS ----------
async def yf_fundamentals(ticker: str):
    key = f"fund:{ticker}"

    cached = _cache_get(key, 300)

    if cached:
        return cached

    try:
        url = (
            f"https://query2.finance.yahoo.com/v10/finance/"
            f"quoteSummary/{ticker}"
            f"?modules=price,defaultKeyStatistics,summaryDetail"
        )

        async with httpx.AsyncClient(
            timeout=10,
            headers=YF_HEADERS
        ) as client:
            r = await client.get(url)

        data = r.json()

        result = data.get("quoteSummary", {}).get("result")

        if not result:
            return {}

        result = result[0]

        price = result.get("price", {})
        stats = result.get("defaultKeyStatistics", {})
        summary = result.get("summaryDetail", {})

        out = {
            "marketCap": price.get("marketCap", {}).get("raw"),
            "peRatio": (
                price.get("trailingPE", {}).get("raw") or
                summary.get("trailingPE", {}).get("raw")
            ),
            "volume": price.get("regularMarketVolume", {}).get("raw"),
            "fiftyTwoWeekHigh": summary.get("fiftyTwoWeekHigh", {}).get("raw"),
            "fiftyTwoWeekLow": summary.get("fiftyTwoWeekLow", {}).get("raw"),
        }

        _cache_set(key, out)

        return out

    except Exception as e:
        print("FUNDAMENTALS ERROR:", e)
        return {}

# ---------- LIVE QUOTE ----------
async def yf_quote(ticker: str) -> Dict[str, Any]:
    key = f"quote:{ticker}"

    cached = _cache_get(key)

    if cached:
        return cached

    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1d&interval=1m"

        async with httpx.AsyncClient(
            timeout=10,
            headers=YF_HEADERS
        ) as client:
            r = await client.get(url)

        data = r.json()

        result = data["chart"]["result"][0]
        meta = result["meta"]

        price = meta.get("regularMarketPrice")
        prev = meta.get("previousClose")

        change = None
        change_pct = None

        if price and prev:
            change = price - prev
            change_pct = (change / prev) * 100

        # fundamentals
        fund = await yf_fundamentals(ticker)

        out = {
            "ticker": ticker,
            "price": price,
            "change": change,
            "changePct": change_pct,
            "marketCap": fund.get("marketCap"),
            "peRatio": fund.get("peRatio"),
            "volume": fund.get("volume"),
            "fiftyTwoWeekHigh": meta.get("fiftyTwoWeekHigh") or fund.get("fiftyTwoWeekHigh"),
            "fiftyTwoWeekLow": meta.get("fiftyTwoWeekLow") or fund.get("fiftyTwoWeekLow"),
        }

        _cache_set(key, out)

        return out

    except Exception as e:
        print("QUOTE ERROR:", e)

        return {
            "ticker": ticker,
            "price": None,
            "change": None,
            "changePct": None,
            "marketCap": None,
            "peRatio": None,
            "volume": None,
            "fiftyTwoWeekHigh": None,
            "fiftyTwoWeekLow": None,
        }


# ---------- CHART ----------
async def yf_chart(ticker: str, rng="6mo"):
    key = f"chart:{ticker}:{rng}"

    cached = _cache_get(key, 300)

    if cached:
        return cached

    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range={rng}&interval=1d"

        async with httpx.AsyncClient(
            timeout=10,
            headers=YF_HEADERS
        ) as client:
            r = await client.get(url)

        data = r.json()

        result = data["chart"]["result"][0]

        timestamps = result.get("timestamp", [])

        closes = (
            result.get("indicators", {})
            .get("quote", [{}])[0]
            .get("close", [])
        )

        out = {
            "timestamps": timestamps,
            "closes": closes
        }

        _cache_set(key, out)

        return out

    except Exception as e:
        print("CHART ERROR:", e)

        return {
            "timestamps": [],
            "closes": []
        }


# ---------- ROUTES ----------
@app.get("/")
async def root():
    return {"status": "ORION backend online"}


@app.get("/api/orion/universe")
async def universe():
    return {"companies": UNIVERSE}


@app.get("/api/orion/quote/{ticker}")
async def quote(ticker: str):
    return await yf_quote(ticker.upper())


@app.get("/api/orion/quotes")
async def quotes(tickers: str):
    tickers_list = tickers.split(",")

    data = []

    for t in tickers_list:
        try:
            q = await yf_quote(t.strip().upper())
            data.append(q)

        except Exception as e:
            print("QUOTES ERROR:", e)

    return {"quotes": data}


@app.get("/api/orion/chart/{ticker}")
async def chart(ticker: str, rng: str = "6mo"):
    return await yf_chart(ticker.upper(), rng)


@app.get("/api/orion/dashboard/{ticker}")
async def dashboard(ticker: str):
    q = await yf_quote(ticker.upper())
    c = await yf_chart(ticker.upper())

    return {
        "quote": q,
        "chart": c,

        "signals": {
            "score": random.randint(45, 90),

            "signals": [
                {
                    "type": "MOMENTUM",
                    "label": "Strong institutional inflows detected",
                    "confidence": 0.82,
                    "severity": "info"
                },
                {
                    "type": "VOLATILITY",
                    "label": "Elevated implied volatility",
                    "confidence": 0.61,
                    "severity": "warn"
                }
            ]
        },

        "filings": [
            {
                "form": "10-Q",
                "date": "2026-05-18",
                "description": "Quarterly earnings filing",
                "url": "https://www.sec.gov/"
            },
            {
                "form": "8-K",
                "date": "2026-05-10",
                "description": "Material corporate update",
                "url": "https://www.sec.gov/"
            }
        ]
    }


@app.get("/api/orion/news")
async def news(ticker: str = None):
    base = ticker or "Markets"

    return {
        "news": [
            {
                "title": f"{base} rallies after strong AI demand",
                "link": "https://finance.yahoo.com/",
                "source": "Yahoo Finance",
                "published": str(datetime.utcnow())
            },
            {
                "title": f"{base} analysts raise price targets",
                "link": "https://finance.yahoo.com/",
                "source": "Bloomberg",
                "published": str(datetime.utcnow())
            }
        ]
    }


@app.get("/api/orion/agents/activity")
async def agents():
    events = [
        {
            "agent": "CRAWLER",
            "action": "Fetched live market data",
            "target": "NASDAQ",
            "level": "info",
            "ts": str(datetime.utcnow())
        },
        {
            "agent": "SIGNAL",
            "action": "Generated bullish momentum signal",
            "target": "NVDA",
            "level": "info",
            "ts": str(datetime.utcnow())
        },
        {
            "agent": "SYNTHESIS",
            "action": "Updated institutional memo",
            "target": "AAPL",
            "level": "info",
            "ts": str(datetime.utcnow())
        }
    ]

    return {"events": events}


@app.get("/api/orion/memo/{ticker}")
async def memo(ticker: str):
    return {
        "generated_at": str(datetime.utcnow()),

        "memo": {
            "recommendation": "BUY",
            "conviction": 8,

            "thesis": (
                f"{ticker} continues demonstrating "
                f"strong growth momentum driven by AI demand."
            ),

            "bull_case": [
                "Revenue growth accelerating",
                "Strong balance sheet",
                "Institutional accumulation"
            ],

            "bear_case": [
                "Valuation remains elevated",
                "Macro slowdown risk"
            ],

            "catalysts": [
                "Upcoming earnings",
                "AI product expansion"
            ],

            "risks": [
                "Regulatory pressure",
                "Market volatility"
            ]
        }
    }


# ---------- RUN ----------
if __name__ == "__main__":
    import uvicorn
    import os

    port = int(os.environ.get("PORT", 8000))

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port
    )
