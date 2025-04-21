import logging
import json
import csv
import io
from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import StreamingResponse
import aiohttp
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GoogleRequest

# === Settings ===
API_VERSION     = "v17"
DEVELOPER_TOKEN = "D4yv61IQ8R0JaE5dxrd1Uw"
CLIENT_ID       = "167266694231-g7hvta57r99etbp3sos3jfi7q7h4ef44.apps.googleusercontent.com"
CLIENT_SECRET   = "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"

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers Google Ads
# ─────────────────────────────────────────────────────────────────────────────
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
        SELECT
          campaign.id,
          campaign.name,
          campaign.status,
          metrics.impressions,
          metrics.clicks,
          metrics.ctr,
          metrics.all_conversions,
          metrics.cost_micros,
          metrics.average_cpc
        FROM campaign
        WHERE campaign.status = 'ENABLED'
    """
    async with aiohttp.ClientSession() as sess:
        async with sess.post(url, headers=headers, json={"query": query}) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, f"Google Ads search error: {text}")
            data = json.loads(text).get("results", [])
    rows = []
    for r in data:
        c = r["campaign"]
        m = r["metrics"]
        impressions = int(m.get("impressions", 0))
        clicks      = int(m.get("clicks", 0))
        spend       = float(m.get("costMicros", 0)) / 1e6
        avg_cpc     = float(m.get("averageCpc", 0))
        conversions = float(m.get("allConversions", 0))
        ctr         = float(m.get("ctr", 0))
        rows.append({
            "source":      "google",
            "id":          c.get("id", ""),
            "name":        c.get("name", ""),
            "status":      c.get("status", ""),
            "impressions": impressions,
            "clicks":      clicks,
            "ctr":         ctr,
            "conversions": conversions,
            "avg_cpc":     avg_cpc,
            "spend":       spend,
            "engagement":  clicks + conversions
        })
    return rows

# ─────────────────────────────────────────────────────────────────────────────
# Helpers Meta Ads
# ─────────────────────────────────────────────────────────────────────────────
async def meta_ads_list_active(account_id: str, access_token: str):
    url = f"https://graph.facebook.com/v16.0/act_{account_id}/campaigns"
    filtering = json.dumps([{
        "field":"effective_status",
        "operator":"IN",
        "value":["ACTIVE"]
    }])
    params = {
        "fields":        "id,name,status",
        "filtering":     filtering,
        "access_token":  access_token
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, params=params) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, f"Meta Ads error: {text}")
            data = json.loads(text).get("data", [])
    rows = []
    for c in data:
        cid = c["id"]
        ins_url = f"https://graph.facebook.com/v16.0/{cid}/insights"
        ins_params = {
            "fields":       "impressions,clicks,ctr,cpc,spend,actions",
            "date_preset":  "maximum",
            "access_token": access_token
        }
        async with aiohttp.ClientSession() as sess:
            async with sess.get(ins_url, params=ins_params) as ir:
                metrics = (await ir.json()).get("data", [{}])[0]
        impressions = int(metrics.get("impressions", 0))
        clicks      = int(metrics.get("clicks", 0))
        spend       = float(metrics.get("spend", 0))
        cpc         = float(metrics.get("cpc", 0)) if metrics.get("cpc") else 0.0
        ctr         = float(metrics.get("ctr", 0)) if metrics.get("ctr") else (clicks/impressions*100 if impressions>0 else 0.0)
        conv = sum(
            float(a.get("value", 0))
            for a in metrics.get("actions", [])
            if a.get("action_type") == "offsite_conversion"
        )
        rows.append({
            "source":      "meta",
            "id":          cid,
            "name":        c.get("name", ""),
            "status":      c.get("status", ""),
            "impressions": impressions,
            "clicks":      clicks,
            "ctr":         ctr,
            "conversions": conv,
            "avg_cpc":     cpc,
            "spend":       spend,
            "engagement":  clicks + conv
        })
    return rows

# ─────────────────────────────────────────────────────────────────────────────
# Combined endpoint
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/export_combined_active_campaigns_csv")
async def export_combined_active_campaigns_csv(payload: dict = Body(...)):
    """
    Export combined Google Ads + Meta Ads active campaigns to CSV,
    with aggregated metrics header. Auto‑discovers Google customer_id.
    Payload must include:
      - google_refresh_token
      - meta_account_id
      - meta_access_token
    """
    grt  = payload.get("google_refresh_token")
    maid = payload.get("meta_account_id")
    mat  = payload.get("meta_access_token")
    if not grt or not maid or not mat:
        raise HTTPException(400, "Need google_refresh_token, meta_account_id and meta_access_token")

    google_rows = await google_ads_list_active(grt)
    meta_rows   = await meta_ads_list_active(maid, mat)
    all_rows    = google_rows + meta_rows

    # Aggregate totals
    total_campaigns = len(all_rows)
    total_impr      = sum(r["impressions"] for r in all_rows)
    total_clicks    = sum(r["clicks"] for r in all_rows)
    total_conv      = sum(r["conversions"] for r in all_rows)
    total_spend     = sum(r["spend"] for r in all_rows)
    ctr             = (total_clicks/total_impr*100) if total_impr>0 else 0.0
    avg_cpc         = (total_spend/total_clicks) if total_clicks>0 else 0.0
    total_eng       = sum(r["engagement"] for r in all_rows)

    buf = io.StringIO()
    writer = csv.writer(buf)

    # Header section
    writer.writerow(["Metric","Value"])
    writer.writerow(["Active Campaigns",    total_campaigns])
    writer.writerow(["Total Impressions",   total_impr])
    writer.writerow(["Total Clicks",        total_clicks])
    writer.writerow(["CTR (%)",             f"{ctr:.2f}"])
    writer.writerow(["Conversions",         total_conv])
    writer.writerow(["Avg. CPC",            f"{avg_cpc:.2f}"])
    writer.writerow(["Engagement",          total_eng])
    writer.writerow(["Total Budget Spent",  f"{total_spend:.2f}"])
    writer.writerow([])

    # Detail section
    header = ["source","id","name","status","impressions","clicks","ctr","conversions","avg_cpc","spend","engagement"]
    writer.writerow(header)
    for row in all_rows:
        writer.writerow([row[h] for h in header])

    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition":"attachment; filename=combined_active_campaigns.csv"}
    )

if __name__ == "__main__":
    import uvicorn
    logging.info("Starting export service on port 8080")
    uvicorn.run(app, host="0.0.0.0", port=8080, reload=True)
