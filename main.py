import logging
import json
import csv
import io
import base64
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
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

# ───────── GET Endpoints returning Base64 JSON ─────────

@app.get("/export_google_active_campaigns_csv")
async def export_google_active_campaigns_csv(
    google_refresh_token: str = Query(..., alias="google_refresh_token")
):
    # 1) Busca dados via helper
    rows = await google_ads_list_active(google_refresh_token)

    # 2) Gera o CSV em memória
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id","name","status","impressions","clicks"])
    writer.writeheader()
    writer.writerows(rows)

    # 3) Converte para bytes e codifica em Base64
    csv_bytes = buf.getvalue().encode("utf-8")
    b64 = base64.b64encode(csv_bytes).decode("utf-8")

    # 4) Log do fileBytes (ou apenas do tamanho se for muito grande)
    logging.debug(f"[export_google_active_campaigns_csv] fileBytes length: {len(b64)}")
    # Se quiser ver o conteúdo completo (cuidado, pode lotar o log):
    # logging.debug(f"[export_google_active_campaigns_csv] fileBytes: {b64}")

    # 5) Retorna JSON para o FlutterFlow
    return {
        "fileName": "google_active_campaigns.csv",
        "mimeType": "text/csv",
        "fileBytes": b64
    }

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
    csv_bytes = buf.getvalue().encode("utf-8")
    b64 = base64.b64encode(csv_bytes).decode("utf-8")
    return {
        "fileName": "meta_active_campaigns.csv",
        "mimeType": "text/csv",
        "fileBytes": b64
    }

@app.get("/export_combined_active_campaigns_csv")
async def export_combined_active_campaigns_csv(
    google_refresh_token: str = Query(..., alias="google_refresh_token"),
    meta_account_id:     str = Query(..., alias="meta_account_id"),
    meta_access_token:   str = Query(..., alias="meta_access_token")
):
    google_rows = await google_ads_list_active(google_refresh_token)
    meta_rows   = await meta_ads_list_active(meta_account_id, meta_access_token)
    all_rows    = google_rows + meta_rows

    buf = io.StringIO()
    writer = csv.writer(buf)
    # cabeçalho de métricas
    total_impr   = sum(r.get("impressions", 0) for r in all_rows)
    total_clicks = sum(r.get("clicks", 0) for r in all_rows)
    ctr = (total_clicks / total_impr * 100) if total_impr > 0 else 0.0

    writer.writerow(["Metric","Value"])
    writer.writerow(["Active Campaigns", len(all_rows)])
    writer.writerow(["Total Impressions", total_impr])
    writer.writerow(["Total Clicks", total_clicks])
    writer.writerow(["CTR (%)", f"{ctr:.2f}"])
    writer.writerow([])
    # linhas detalhadas
    header = ["id","name","status","impressions","clicks"]
    writer.writerow(header)
    for r in all_rows:
        writer.writerow([r.get(h, "") for h in header])

    csv_bytes = buf.getvalue().encode("utf-8")
    b64 = base64.b64encode(csv_bytes).decode("utf-8")
    return {
        "fileName": "combined_active_campaigns.csv",
        "mimeType": "text/csv",
        "fileBytes": b64
    }

if __name__ == "__main__":
    logging.info("Starting export service on port 8080")
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
