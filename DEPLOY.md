# ORION deploy checklist

## GitHub Pages (`https://samvruthc.github.io/1orionmax/`)

Upload these files to the **1orionmax** repo (same paths):

- `index.html` (root)
- `data/top100.json` (required for discover paths)

Push to `main`; Pages must be enabled for that repo.

## Railway backend (`https://1orionmax-production.up.railway.app`)

**Required files in the deploy root:**

- `main.py`
- `top100_data.py` ← if missing, the app crashes on import → **502**
- `data/top100.json`, `data/custom_tickers.json`
- `requirements.txt`, `Procfile`, `runtime.txt`, `nixpacks.toml`

**Start command:** `python main.py` (see `Procfile` / `nixpacks.toml`)

**Verify after deploy:**

```bash
curl -sS https://1orionmax-production.up.railway.app/api/orion/health
```

Expect: `{"status":"ok","universe":...}`

If you see **502** or timeout: open Railway → Deployments → **View logs** and fix the traceback (often `ModuleNotFoundError: top100_data` or missing dependencies).

## Frontend ↔ backend

`index.html` points at:

`PRODUCTION_API = 'https://1orionmax-production.up.railway.app/api'`

When the backend is down, the UI uses **limited mode** (Yahoo spark from the browser + embedded top-100 list). Full memos, news, and charts need the Railway API online.
