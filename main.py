import logging
import json
import csv
import io
from fastapi import FastAPI, HTTPException, Body, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
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
            logging.debug(f"[discover_customer_id] {resp.status} {text}")
            if resp.status != 200:
                raise HTTPException(502, f"listAccessibleCustomers error: {text}")
            names = json.loads(text).get("resourceNames", [])
            if not names:
                raise HTTPException(502, "No accessible customers")
            return names[0].split("/")[-1]

async def google_ads_list_active(refresh_token: str):
    access_token = await get_access_token(refresh_token)
    customer_id  = await discover_customer_id(access_token)
    url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{customer_id}/googleAds:search"
    headers = {
        "Authorization":   f"Bearer {access_token}",
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
        }
        for r in results
    ]

# ───────── Helpers for Meta Ads ─────────
async def meta_ads_list_active(account_id: str, access_token: str):
    url = f"https://graph.facebook.com/v16.0/act_{account_id}/campaigns"
    filtering = json.dumps([{"field":"effective_status","operator":"IN","value":["ACTIVE"]}])
    params = {"fields":"id,name,status","filtering":filtering,"access_token":access_token}
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, params=params) as resp:
            text = await resp.text()
            logging.debug(f"[meta_ads_campaigns] {resp.status} {text}")
            if resp.status != 200:
                raise HTTPException(resp.status, f"Meta Ads error: {text}")
            data = json.loads(text).get("data", [])
    return [
        {"id": c["id"], "name": c["name"], "status": c["status"]}
        for c in data
    ]

# ───────── GET Endpoints for CSV download ─────────
@app.get("/export_google_active_campaigns_csv")
async def export_google_active_campaigns_csv(
    google_refresh_token: str = Query(..., alias="google_refresh_token")
):
    rows = await google_ads_list_active(google_refresh_token)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id","name","status","impressions","clicks"])
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition":"attachment; filename=google_active_campaigns.csv"}
    )

@app.get("/export_meta_active_campaigns_csv")
async def export_meta_active_campaigns_csv(
    meta_account_id:   str = Query(..., alias="meta_account_id"),
    meta_access_token: str = Query(..., alias="meta_access_token")
):
    rows = await meta_ads_list_active(meta_account_id, meta_access_token)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id","name","status"])
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition":"attachment; filename=meta_active_campaigns.csv"}
    )

@app.get("/export_combined_active_campaigns_csv")
async def export_combined_active_campaigns_csv(
    google_refresh_token: str = Query(...),
    meta_account_id:      str = Query(...),
    meta_access_token:    str = Query(...)
):
    google_rows = await google_ads_list_active(google_refresh_token)
    meta_rows   = await meta_ads_list_active(meta_account_id, meta_access_token)
    all_rows    = google_rows + meta_rows

    total_campaigns = len(all_rows)
    total_impr      = sum(r["impressions"] for r in all_rows)
    total_clicks    = sum(r["clicks"]      for r in all_rows)
    ctr             = (total_clicks / total_impr * 100) if total_impr > 0 else 0.0

    buf = io.StringIO()
    writer = csv.writer(buf)
    # Aggregated header
    writer.writerow(["Metric","Value"])
    writer.writerow(["Active Campaigns", total_campaigns])
    writer.writerow(["Total Impressions", total_impr])
    writer.writerow(["Total Clicks", total_clicks])
    writer.writerow(["CTR (%)", f"{ctr:.2f}"])
    writer.writerow([])
    # Detail rows
    header = ["id","name","status","impressions","clicks"]
    writer.writerow(header)
    for row in all_rows:
        writer.writerow([row[h] for h in header])
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition":"attachment; filename=combined_active_campaigns.csv"}
    )

# ───────── POST Endpoints to return JSON URL for download ─────────
@app.post("/get_google_active_campaigns_url")
async def get_google_active_campaigns_url(
    request: Request,
    payload: dict = Body(...)
):
    logging.debug(f"[get_google_url] Payload: {json.dumps(payload)}")
    token = payload.get("google_refresh_token")
    if not token:
        raise HTTPException(400, "google_refresh_token is required")
    endpoint = request.url_for("export_google_active_campaigns_csv")
    url = f"{endpoint}?google_refresh_token={token}"
    logging.info(f"[get_google_url] Returning URL: {url}")
    return {"url": url}

@app.post("/get_meta_active_campaigns_url")
async def get_meta_active_campaigns_url(
    request: Request,
    payload: dict = Body(...)
):
    logging.debug(f"[get_meta_url] Payload: {json.dumps(payload)}")
    aid = payload.get("meta_account_id"); tok = payload.get("meta_access_token")
    if not aid or not tok:
        raise HTTPException(400, "meta_account_id and meta_access_token are required")
    endpoint = request.url_for("export_meta_active_campaigns_csv")
    url = f"{endpoint}?meta_account_id={aid}&meta_access_token={tok}"
    logging.info(f"[get_meta_url] Returning URL: {url}")
    return {"url": url}

@app.post("/get_combined_active_campaigns_url")
async def get_combined_active_campaigns_url(
    request: Request,
    payload: dict = Body(...)
):
    logging.debug(f"[get_combined_url] Payload: {json.dumps(payload)}")
    grt  = payload.get("google_refresh_token")
    maid = payload.get("meta_account_id")
    mat  = payload.get("meta_access_token")
    if not grt or not maid or not mat:
        raise HTTPException(400, "google_refresh_token, meta_account_id and meta_access_token are required")
    endpoint = request.url_for("export_combined_active_campaigns_csv")
    qs = f"?google_refresh_token={grt}&meta_account_id={maid}&meta_access_token={mat}"
    url = f"{endpoint}{qs}"
    logging.info(f"[get_combined_url] Returning URL: {url}")
    return {"url": url}

if __name__ == "__main__":
    import uvicorn
    logging.info("Starting export service on port 8080")
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
