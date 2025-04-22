import logging
import json
import csv
import io
from datetime import datetime, timedelta
from collections import defaultdict
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import aiohttp
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GoogleRequest

# === Settings ===
API_VERSION     = "v17"
DEVELOPER_TOKEN = "D4yv61IQ8R0JaE5dxrd1Uw"
CLIENT_ID       = "167266694231-g7hvta57r99etbp3sos3jfi7q7h4ef44.apps.googleusercontent.com"
CLIENT_SECRET   = "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"

# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ───────── Utility: ASCII sparkline ─────────
SPARK_BARS = ["▁","▂","▃","▄","▅","▆","▇","█"]
def sparkline(data):
    if not data:
        return ""
    mn, mx = min(data), max(data)
    span = mx - mn or 1
    return "".join(SPARK_BARS[int((v - mn)/span*(len(SPARK_BARS)-1))] for v in data)

# ───────── Helpers for Google Ads ─────────
async def get_access_token(refresh_token: str) -> str:
    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
    )
    creds.refresh(GoogleRequest())
    return creds.token

async def discover_customer_id(access_token: str) -> str:
    url = f"https://googleads.googleapis.com/{API_VERSION}/customers:listAccessibleCustomers"
    headers = {
        "Authorization":   f"Bearer {access_token}",
        "developer-token": DEVELOPER_TOKEN,
        "Content-Type":    "application/json"
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, headers=headers) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(502, text)
            names = json.loads(text).get("resourceNames", [])
            if not names:
                raise HTTPException(502, "No accessible customers")
            return names[0].split("/")[-1]

async def google_ads_list_active(refresh_token: str):
    token = await get_access_token(refresh_token)
    cid   = await discover_customer_id(token)
    url   = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/googleAds:search"
    headers = {
        "Authorization":   f"Bearer {token}",
        "developer-token": DEVELOPER_TOKEN,
        "Content-Type":    "application/json"
    }
    query = """
        SELECT campaign.id, campaign.name, campaign.status,
               metrics.impressions, metrics.clicks
        FROM campaign
        WHERE campaign.status = 'ENABLED'
    """
    async with aiohttp.ClientSession() as sess:
        async with sess.post(url, headers=headers, json={"query": query}) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, text)
            results = json.loads(text).get("results", [])
    rows = []
    for r in results:
        imp = int(r["metrics"]["impressions"])
        clk = int(r["metrics"]["clicks"])
        rows.append({
            "id":           r["campaign"]["id"],
            "name":         r["campaign"]["name"],
            "status":       r["campaign"]["status"],
            "impressions":  imp,
            "clicks":       clk,
            "ctr (%)":      round(clk / max(imp,1) * 100, 2),
        })
    return rows

async def google_ads_list_trends(refresh_token: str, days: int = 7):
    token = await get_access_token(refresh_token)
    cid   = await discover_customer_id(token)
    url   = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/googleAds:search"
    headers = {
        "Authorization":   f"Bearer {token}",
        "developer-token": DEVELOPER_TOKEN,
        "Content-Type":    "application/json"
    }
    query = f"""
        SELECT segments.date, metrics.impressions, metrics.clicks
        FROM campaign
        WHERE campaign.status = 'ENABLED'
          AND segments.date DURING LAST_{days}_DAYS
    """
    async with aiohttp.ClientSession() as sess:
        async with sess.post(url, headers=headers, json={"query": query}) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, text)
            results = json.loads(text).get("results", [])
    by_date = defaultdict(lambda: {"impressions":0,"clicks":0})
    for r in results:
        d = r["segments"]["date"]
        by_date[d]["impressions"] += int(r["metrics"]["impressions"])
        by_date[d]["clicks"]     += int(r["metrics"]["clicks"])
    dates = sorted(by_date)
    return dates, [by_date[d]["impressions"] for d in dates], [by_date[d]["clicks"] for d in dates]

# ───────── Helpers for Meta Ads (clamp to last 37 months) ─────────
async def meta_ads_list_active(account_id: str, access_token: str):
    url = f"https://graph.facebook.com/v16.0/act_{account_id}/campaigns"
    params = {"fields":"id,name,status","access_token":access_token}
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, params=params) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, f"Meta campaigns error: {text}")
            data = json.loads(text).get("data", [])
    active = [c for c in data if c.get("status") == "ACTIVE"]

    ins_url = f"https://graph.facebook.com/v16.0/act_{account_id}/insights"
    # clamp since to 37 months ago (~1110 days)
    since = (datetime.now().date() - timedelta(days=1110)).isoformat()
    until = datetime.now().date().isoformat()
    ins_params = {
        "level":        "campaign",
        "fields":       "campaign_id,impressions,clicks,spend",
        "time_range":   json.dumps({"since": since, "until": until}),
        "access_token": access_token
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.get(ins_url, params=ins_params) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, f"Meta insights error: {text}")
            insights = json.loads(text).get("data", [])

    metrics_map = {i["campaign_id"]: i for i in insights}
    rows = []
    for c in active:
        m   = metrics_map.get(c["id"], {})
        imp = int(m.get("impressions",0))
        clk = int(m.get("clicks",0))
        spd = float(m.get("spend",0))
        rows.append({
            "id":           c["id"],
            "name":         c["name"],
            "status":       c["status"],
            "impressions":  imp,
            "clicks":       clk,
            "spend":        round(spd,2),
            "ctr (%)":      round(clk/max(imp,1)*100,2),
            "cpc":          round(spd/max(clk,1),2)
        })
    return rows

async def meta_ads_list_trends(account_id: str, access_token: str, days: int = 7):
    url = f"https://graph.facebook.com/v16.0/act_{account_id}/insights"
    since = (datetime.now().date() - timedelta(days=1110)).isoformat()
    until = datetime.now().date().isoformat()
    params = {
        "level":         "campaign",
        "fields":        "date_start,impressions,clicks",
        "time_increment":1,
        "time_range":    json.dumps({"since": since, "until": until}),
        "access_token":  access_token
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, params=params) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, text)
            data = json.loads(text).get("data", [])
    by_date = defaultdict(lambda: {"impressions":0,"clicks":0})
    for d in data:
        dt = d["date_start"]
        by_date[dt]["impressions"] += int(d["impressions"])
        by_date[dt]["clicks"]     += int(d["clicks"])
    dates = sorted(by_date)
    return dates, [by_date[d]["impressions"] for d in dates], [by_date[d]["clicks"] for d in dates]

# ───────── Endpoints ─────────

@app.get("/export_combined_active_campaigns_csv")
async def export_combined_active_campaigns_csv(
    google_refresh_token: str = Query(..., alias="google_refresh_token"),
    meta_account_id:     str = Query(..., alias="meta_account_id"),
    meta_access_token:   str = Query(..., alias="meta_access_token")
):
    g_rows, (g_dates, g_imps, g_clks) = (
        await google_ads_list_active(google_refresh_token),
        await google_ads_list_trends(google_refresh_token)
    )
    m_rows, (m_dates, m_imps, m_clks) = (
        await meta_ads_list_active(meta_account_id, meta_access_token),
        await meta_ads_list_trends(meta_account_id, meta_access_token)
    )

    rows = g_rows + m_rows
    dates = g_dates
    combined_imps = [gi + mi for gi, mi in zip(g_imps, m_imps)]
    combined_clks = [gc + mc for gc, mc in zip(g_clks, m_clks)]

    total_imp = sum(r["impressions"] for r in rows)
    total_clk = sum(r["clicks"] for r in rows)
    avg_ctr   = round(total_clk / max(total_imp,1) * 100, 2)
    review    = f"Combined: Google {len(g_rows)} + Meta {len(m_rows)} = {len(rows)} campanhas | Imps {total_imp}, Cliques {total_clk}, CTR {avg_ctr}%"

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Review", review]); w.writerow([])
    w.writerow(["Date"] + dates)
    w.writerow(["Impressions Trend", sparkline(combined_imps)])
    w.writerow(["Clicks Trend",      sparkline(combined_clks)]); w.writerow([])
    w.writerow(["Metric","Value"])
    w.writerow(["Google Active",   len(g_rows)])
    w.writerow(["Meta Active",     len(m_rows)])
    w.writerow(["Total Campaigns", len(rows)])
    w.writerow(["Total Impressions", total_imp])
    w.writerow(["Total Clicks",      total_clk])
    w.writerow(["Overall CTR (%)",   avg_ctr]); w.writerow([])
    w.writerow(["Campaign ID","Network","Name","Status","Impressions","Clicks","CTR (%)"])
    for r in rows:
        net = "Google" if "cpc" not in r else "Meta"
        w.writerow([
            r["id"], net, r.get("name",""), r.get("status",""),
            r["impressions"], r["clicks"], r["ctr (%)"]
        ])

    data = buf.getvalue().encode("utf-8")
    return JSONResponse({
        "fileName": "combined_active_campaigns.csv",
        "mimeType": "text/csv",
        "bytes": list(data)
    })


if __name__ == "__main__":
    logging.info("Starting export service on port 8080")
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
