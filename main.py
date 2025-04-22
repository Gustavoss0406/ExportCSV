import logging
import json
import csv
import io
from datetime import datetime, timedelta
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

# ───────── Helpers para Google Ads ─────────
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
            logging.debug(f"[discover_customer_id] {resp.status} {text}")
            if resp.status != 200:
                raise HTTPException(502, f"listAccessibleCustomers error: {text}")
            names = json.loads(text).get("resourceNames", [])
            if not names:
                raise HTTPException(502, "No accessible customers")
            return names[0].split("/")[-1]

async def google_ads_list_active(refresh_token: str):
    token       = await get_access_token(refresh_token)
    customer_id = await discover_customer_id(token)
    url         = f"https://googleads.googleapis.com/{API_VERSION}/customers/{customer_id}/googleAds:search"
    headers     = {
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
            logging.debug(f"[google_ads_search] {resp.status} {text}")
            if resp.status != 200:
                raise HTTPException(resp.status, f"Google Ads search error: {text}")
            results = json.loads(text).get("results", [])
    return [
        {
            "id":           r["campaign"]["id"],
            "name":         r["campaign"]["name"],
            "status":       r["campaign"]["status"],
            "impressions":  int(r["metrics"]["impressions"]),
            "clicks":       int(r["metrics"]["clicks"]),
            "ctr (%)":      round(int(r["metrics"]["clicks"]) / max(int(r["metrics"]["impressions"]),1) * 100, 2)
        }
        for r in results
    ]

async def google_ads_list_trends(refresh_token: str, days: int = 7):
    token       = await get_access_token(refresh_token)
    customer_id = await discover_customer_id(token)
    url         = f"https://googleads.googleapis.com/{API_VERSION}/customers/{customer_id}/googleAds:search"
    headers     = {
        "Authorization":   f"Bearer {token}",
        "developer-token": DEVELOPER_TOKEN,
        "Content-Type":    "application/json"
    }
    query = f"""
        SELECT campaign.id, segments.date, metrics.impressions, metrics.clicks
        FROM campaign
        WHERE campaign.status = 'ENABLED'
          AND segments.date DURING LAST_{days}_DAYS
    """
    async with aiohttp.ClientSession() as sess:
        async with sess.post(url, headers=headers, json={"query": query}) as resp:
            text = await resp.text()
            logging.debug(f"[google_ads_trends] {resp.status} {text}")
            if resp.status != 200:
                raise HTTPException(resp.status, f"Google Ads trends error: {text}")
            results = json.loads(text).get("results", [])
    trends = []
    for r in results:
        imp = int(r["metrics"]["impressions"])
        clk = int(r["metrics"]["clicks"])
        trends.append({
            "date":        r["segments"]["date"],
            "campaign_id": r["campaign"]["id"],
            "impressions": imp,
            "clicks":      clk,
            "ctr (%)":     round(clk / max(imp,1) * 100, 2)
        })
    return trends

# ───────── Helpers para Meta Ads ─────────
async def meta_ads_list_active(account_id: str, access_token: str):
    url = f"https://graph.facebook.com/v16.0/act_{account_id}/campaigns"
    params = {
        "fields":       "id,name,status,insights.date_preset(lifetime){impressions,clicks,spend}",
        "filtering":    json.dumps([{"field":"effective_status","operator":"IN","value":["ACTIVE"]}]),
        "access_token": access_token
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, params=params) as resp:
            text = await resp.text()
            logging.debug(f"[meta_ads_active] {resp.status} {text}")
            if resp.status != 200:
                raise HTTPException(resp.status, f"Meta Ads error: {text}")
            data = json.loads(text).get("data", [])
    rows = []
    for c in data:
        ins = c.get("insights", {}).get("data", [{}])[0]
        imp = int(ins.get("impressions", 0))
        clk = int(ins.get("clicks", 0))
        spd = float(ins.get("spend", 0.0))
        rows.append({
            "id":           c["id"],
            "name":         c["name"],
            "status":       c["status"],
            "impressions":  imp,
            "clicks":       clk,
            "spend":        round(spd, 2),
            "ctr (%)":      round(clk / max(imp,1) * 100, 2),
            "cpc":          round(spd / max(clk,1), 2)
        })
    return rows

async def meta_ads_list_trends(account_id: str, access_token: str, days: int = 7):
    url = f"https://graph.facebook.com/v16.0/act_{account_id}/insights"
    since = (datetime.now().date() - timedelta(days=days)).isoformat()
    until = datetime.now().date().isoformat()
    params = {
        "fields":       "date_start,date_stop,impressions,clicks,spend",
        "time_increment": 1,
        "time_range":   json.dumps({"since": since, "until": until}),
        "filtering":    json.dumps([{"field":"effective_status","operator":"IN","value":["ACTIVE"]}]),
        "access_token": access_token
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, params=params) as resp:
            text = await resp.text()
            logging.debug(f"[meta_ads_trends] {resp.status} {text}")
            if resp.status != 200:
                raise HTTPException(resp.status, f"Meta Ads trends error: {text}")
            data = json.loads(text).get("data", [])
    trends = []
    for d in data:
        imp = int(d["impressions"])
        clk = int(d["clicks"])
        spd = float(d["spend"])
        trends.append({
            "date":        f"{d['date_start']}",
            "impressions": imp,
            "clicks":      clk,
            "spend":       round(spd, 2),
            "ctr (%)":     round(clk / max(imp,1) * 100, 2),
            "cpc":         round(spd / max(clk,1), 2)
        })
    return trends

# ───────── Endpoints CSV “profissa” ─────────

@app.get("/export_google_active_campaigns_csv")
async def export_google_active_campaigns_csv(
    google_refresh_token: str = Query(..., alias="google_refresh_token")
):
    rows   = await google_ads_list_active(google_refresh_token)
    trends = await google_ads_list_trends(google_refresh_token, days=7)

    # 1) Header com timestamp
    generated_at = datetime.now().isoformat()
    summary = [
        ["Report generated at", generated_at],
        ["Metric",            "Value"],
        ["Active Campaigns",  len(rows)],
        ["Total Impressions", sum(r["impressions"] for r in rows)],
        ["Total Clicks",      sum(r["clicks"] for r in rows)],
        ["Average CTR (%)",   round(sum(r["clicks"] for r in rows)
                                / max(sum(r["impressions"] for r in rows),1) * 100, 2)]
    ]

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerows(summary)
    writer.writerow([])

    # 2) Detalhamento por campanha
    writer.writerow(["Campaign ID","Name","Status","Impressions","Clicks","CTR (%)"])
    for r in rows:
        writer.writerow([
            r["id"], r["name"], r["status"],
            r["impressions"], r["clicks"], r["ctr (%)"]
        ])
    writer.writerow([])

    # 3) Tendências últimos 7 dias
    writer.writerow(["Date","Campaign ID","Impressions","Clicks","CTR (%)"])
    for t in trends:
        writer.writerow([
            t["date"], t["campaign_id"],
            t["impressions"], t["clicks"], t["ctr (%)"]
        ])

    data = buf.getvalue().encode("utf-8")
    return JSONResponse({
        "fileName": "google_active_campaigns.csv",
        "mimeType": "text/csv",
        "bytes": list(data)
    })


@app.get("/export_meta_active_campaigns_csv")
async def export_meta_active_campaigns_csv(
    meta_account_id:   str = Query(..., alias="meta_account_id"),
    meta_access_token: str = Query(..., alias="meta_access_token")
):
    rows   = await meta_ads_list_active(meta_account_id, meta_access_token)
    trends = await meta_ads_list_trends(meta_account_id, meta_access_token, days=7)

    generated_at = datetime.now().isoformat()
    summary = [
        ["Report generated at", generated_at],
        ["Metric",            "Value"],
        ["Active Campaigns",  len(rows)],
        ["Total Impressions", sum(r["impressions"] for r in rows)],
        ["Total Clicks",      sum(r["clicks"] for r in rows)],
        ["Total Spend",       round(sum(r["spend"] for r in rows), 2)],
        ["Average CTR (%)",   round(sum(r["clicks"] for r in rows)
                                / max(sum(r["impressions"] for r in rows),1) * 100, 2)],
        ["Average CPC",       round(sum(r["spend"] for r in rows)
                                / max(sum(r["clicks"] for r in rows),1), 2)]
    ]

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerows(summary)
    writer.writerow([])

    writer.writerow(["Campaign ID","Name","Status","Impressions","Clicks","Spend","CTR (%)","CPC"])
    for r in rows:
        writer.writerow([
            r["id"], r["name"], r["status"],
            r["impressions"], r["clicks"], r["spend"],
            r["ctr (%)"], r["cpc"]
        ])
    writer.writerow([])

    writer.writerow(["Date","Impressions","Clicks","Spend","CTR (%)","CPC"])
    for t in trends:
        writer.writerow([
            t["date"], t["impressions"], t["clicks"],
            t["spend"], t["ctr (%)"], t["cpc"]
        ])

    data = buf.getvalue().encode("utf-8")
    return JSONResponse({
        "fileName": "meta_active_campaigns.csv",
        "mimeType": "text/csv",
        "bytes": list(data)
    })


@app.get("/export_combined_active_campaigns_csv")
async def export_combined_active_campaigns_csv(
    google_refresh_token: str = Query(..., alias="google_refresh_token"),
    meta_account_id:     str = Query(..., alias="meta_account_id"),
    meta_access_token:   str = Query(..., alias="meta_access_token")
):
    g_rows = await google_ads_list_active(google_refresh_token)
    m_rows = await meta_ads_list_active(meta_account_id, meta_access_token)
    rows   = g_rows + m_rows

    generated_at = datetime.now().isoformat()
    summary = [
        ["Report generated at", generated_at],
        ["Metric",               "Value"],
        ["Google Active",        len(g_rows)],
        ["Meta Active",          len(m_rows)],
        ["Total Campaigns",      len(rows)],
        ["Total Impressions",    sum(r["impressions"] for r in rows)],
        ["Total Clicks",         sum(r["clicks"] for r in rows)],
        ["Total Spend (Meta)",   round(sum(r.get("spend",0) for r in m_rows),2)],
        ["Overall CTR (%)",      round(sum(r["clicks"] for r in rows)
                                   / max(sum(r["impressions"] for r in rows),1) * 100, 2)]
    ]

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerows(summary)
    writer.writerow([])

    writer.writerow(["Campaign ID","Network","Name","Status","Impressions","Clicks","Spend","CTR (%)"])
    for r in rows:
        network = "Google" if "ctr (%)" in r and "spend" not in r else "Meta"
        writer.writerow([
            r["id"], network, r.get("name",""), r.get("status",""),
            r["impressions"], r["clicks"], r.get("spend","—"), r["ctr (%)"]
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
