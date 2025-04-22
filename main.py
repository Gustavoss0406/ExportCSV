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

# Configura logging
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
        client_secret=CLIENT_SECRET
    )
    creds.refresh(GoogleRequest())
    return creds.token

async def discover_customer_id(access_token: str) -> str:
    url = f"https://googleads.googleapis.com/{API_VERSION}/customers:listAccessibleCustomers"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "developer-token": DEVELOPER_TOKEN,
        "Content-Type": "application/json"
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, headers=headers) as resp:
            text = await resp.text()
            logging.debug(f"[discover_customer_id] {resp.status} {text}")
            if resp.status != 200:
                raise HTTPException(502, f"listAccessibleCustomers error: {text}")
            data = json.loads(text).get("resourceNames", [])
            if not data:
                raise HTTPException(502, "No accessible customers")
            return data[0].split("/")[-1]

async def google_ads_list_active(refresh_token: str):
    token    = await get_access_token(refresh_token)
    customer = await discover_customer_id(token)
    url      = f"https://googleads.googleapis.com/{API_VERSION}/customers/{customer}/googleAds:search"
    headers  = {
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
                raise HTTPException(resp.status, f"Google Ads error: {text}")
            results = json.loads(text).get("results", [])
    return [
        {
            "id":          r["campaign"]["id"],
            "name":        r["campaign"]["name"],
            "status":      r["campaign"]["status"],
            "impressions": int(r["metrics"]["impressions"]),
            "clicks":      int(r["metrics"]["clicks"])
        } for r in results
    ]

async def google_ads_list_trends(refresh_token: str, days: int = 7):
    token    = await get_access_token(refresh_token)
    customer = await discover_customer_id(token)
    url      = f"https://googleads.googleapis.com/{API_VERSION}/customers/{customer}/googleAds:search"
    headers  = {
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
        ctr = round(clk / imp * 100, 2) if imp > 0 else 0.0
        trends.append({
            "date":        r["segments"]["date"],
            "campaign_id": r["campaign"]["id"],
            "impressions": imp,
            "clicks":      clk,
            "ctr (%)":     ctr
        })
    return trends

# ───────── Helpers para Meta Ads ─────────
async def meta_ads_list_active(account_id: str, access_token: str):
    url = f"https://graph.facebook.com/v16.0/act_{account_id}/campaigns"
    params = {
        "fields":      "id,name,status,insights.date_preset(lifetime){impressions,clicks}",
        "filtering":   json.dumps([{"field":"effective_status","operator":"IN","value":["ACTIVE"]}]),
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
        ins = c.get("insights", {}).get("data", [])
        imp = int(ins[0]["impressions"]) if ins else 0
        clk = int(ins[0]["clicks"]) if ins else 0
        rows.append({
            "id":          c["id"],
            "name":        c["name"],
            "status":      c["status"],
            "impressions": imp,
            "clicks":      clk
        })
    return rows

async def meta_ads_list_trends(account_id: str, access_token: str, days: int = 7):
    url = f"https://graph.facebook.com/v16.0/act_{account_id}/insights"
    since = (datetime.now().date() - timedelta(days=days)).isoformat()
    until = datetime.now().date().isoformat()
    params = {
        "fields":       "impressions,clicks,campaign_id",
        "level":        "campaign",
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
        ctr = round(clk / imp * 100, 2) if imp > 0 else 0.0
        trends.append({
            "date_range":  f"{since} to {until}",
            "campaign_id": d["campaign_id"],
            "impressions": imp,
            "clicks":      clk,
            "ctr (%)":     ctr
        })
    return trends

# ───────── Endpoints ─────────

@app.get("/export_google_active_campaigns_csv")
async def export_google_active_campaigns_csv(
    google_refresh_token: str = Query(..., alias="google_refresh_token")
):
    rows   = await google_ads_list_active(google_refresh_token)
    trends = await google_ads_list_trends(google_refresh_token, days=7)

    # Sumário top‑level
    total_imp = sum(r["impressions"] for r in rows)
    total_clk = sum(r["clicks"] for r in rows)
    summary = [
        ["Metric","Value"],
        ["Active Campaigns",      len(rows)],
        ["Total Impressions",     total_imp],
        ["Total Clicks",          total_clk],
        ["Average CTR (%)",       round(total_clk/total_imp*100,2) if total_imp>0 else 0.0]
    ]

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerows(summary)
    writer.writerow([])
    # Detalhado por campanha
    writer.writerow(["Campaign ID","Name","Status","Impressions","Clicks","CTR (%)"])
    for r in rows:
        ctr = round(r["clicks"]/r["impressions"]*100,2) if r["impressions"]>0 else 0.0
        writer.writerow([r["id"], r["name"], r["status"], r["impressions"], r["clicks"], ctr])
    writer.writerow([])
    # Tendência últimos 7 dias
    writer.writerow(["Date","Campaign ID","Impressions","Clicks","CTR (%)"])
    for t in trends:
        writer.writerow([t["date"], t["campaign_id"], t["impressions"], t["clicks"], t["ctr (%)"]])

    data = buf.getvalue().encode("utf-8")
    resp = {
        "fileName": "google_active_campaigns.csv",
        "mimeType": "text/csv",
        "bytes": list(data)
    }
    logging.debug(f"[export_google] CSV size={len(data)} bytes")
    return JSONResponse(content=resp)

@app.get("/export_meta_active_campaigns_csv")
async def export_meta_active_campaigns_csv(
    meta_account_id:   str = Query(..., alias="meta_account_id"),
    meta_access_token: str = Query(..., alias="meta_access_token")
):
    rows   = await meta_ads_list_active(meta_account_id, meta_access_token)
    trends = await meta_ads_list_trends(meta_account_id, meta_access_token, days=7)

    total_imp = sum(r["impressions"] for r in rows)
    total_clk = sum(r["clicks"] for r in rows)
    summary = [
        ["Metric","Value"],
        ["Active Campaigns",      len(rows)],
        ["Total Impressions",     total_imp],
        ["Total Clicks",          total_clk],
        ["Average CTR (%)",       round(total_clk/total_imp*100,2) if total_imp>0 else 0.0]
    ]

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerows(summary)
    writer.writerow([])
    writer.writerow(["Campaign ID","Name","Status","Impressions","Clicks","CTR (%)"])
    for r in rows:
        ctr = round(r["clicks"]/r["impressions"]*100,2) if r["impressions"]>0 else 0.0
        writer.writerow([r["id"], r["name"], r["status"], r["impressions"], r["clicks"], ctr])
    writer.writerow([])
    writer.writerow(["Date Range","Campaign ID","Impressions","Clicks","CTR (%)"])
    for t in trends:
        writer.writerow([t["date_range"], t["campaign_id"], t["impressions"], t["clicks"], t["ctr (%)"]])

    data = buf.getvalue().encode("utf-8")
    resp = {
        "fileName": "meta_active_campaigns.csv",
        "mimeType": "text/csv",
        "bytes": list(data)
    }
    logging.debug(f"[export_meta] CSV size={len(data)} bytes")
    return JSONResponse(content=resp)

@app.get("/export_combined_active_campaigns_csv")
async def export_combined_active_campaigns_csv(
    google_refresh_token: str = Query(..., alias="google_refresh_token"),
    meta_account_id:     str = Query(..., alias="meta_account_id"),
    meta_access_token:   str = Query(..., alias="meta_access_token")
):
    g_rows = await google_ads_list_active(google_refresh_token)
    m_rows = await meta_ads_list_active(meta_account_id, meta_access_token)
    rows   = g_rows + m_rows

    total_imp = sum(r["impressions"] for r in rows)
    total_clk = sum(r["clicks"] for r in rows)
    summary = [
        ["Metric","Value"],
        ["Total Active Campaigns", len(rows)],
        ["Total Impressions",      total_imp],
        ["Total Clicks",           total_clk],
        ["Overall CTR (%)",        round(total_clk/total_imp*100,2) if total_imp>0 else 0.0]
    ]

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerows(summary)
    writer.writerow([])
    writer.writerow(["Campaign ID","Name","Status","Impressions","Clicks","CTR (%)"])
    for r in rows:
        ctr = round(r["clicks"]/r["impressions"]*100,2) if r["impressions"]>0 else 0.0
        writer.writerow([r["id"], r.get("name",""), r.get("status",""), r["impressions"], r["clicks"], ctr])

    data = buf.getvalue().encode("utf-8")
    resp = {
        "fileName": "combined_active_campaigns.csv",
        "mimeType": "text/csv",
        "bytes": list(data)
    }
    logging.debug(f"[export_combined] CSV size={len(data)} bytes")
    return JSONResponse(content=resp)

if __name__ == "__main__":
    logging.info("Starting export service on port 8080")
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
