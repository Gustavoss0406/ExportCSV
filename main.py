import logging
import json
import io
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import aiohttp
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GoogleRequest
import pandas as pd

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
    # incluímos insights para pegar impressões e cliques
    filtering = json.dumps([{"field":"effective_status","operator":"IN","value":["ACTIVE"]}])
    params = {
        "fields":      "id,name,status,insights.date_preset(lifetime){impressions,clicks}",
        "filtering":   filtering,
        "access_token": access_token
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, params=params) as resp:
            text = await resp.text()
            logging.debug(f"[meta_ads_campaigns] {resp.status} {text}")
            if resp.status != 200:
                raise HTTPException(resp.status, f"Meta Ads error: {text}")
            data = json.loads(text).get("data", [])
    rows = []
    for c in data:
        insights = c.get("insights", {}).get("data", [])
        if insights:
            imp = int(insights[0].get("impressions", 0))
            clk = int(insights[0].get("clicks", 0))
        else:
            imp = clk = 0
        rows.append({
            "id": c["id"],
            "name": c["name"],
            "status": c["status"],
            "impressions": imp,
            "clicks": clk
        })
    return rows

# ───────── Função comum para criar planilha Excel estilizada ─────────
def create_excel_report(rows, metrics_summary=None, sheet_name='Active Campaigns'):
    df = pd.DataFrame(rows)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Dados
        df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=4)
        workbook  = writer.book
        worksheet = writer.sheets[sheet_name]

        # Formato do cabeçalho de colunas
        header_fmt = workbook.add_format({
            'bold': True,
            'font_color': '#FFFFFF',
            'bg_color': '#4CAF50'
        })
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(4, col_num, value, header_fmt)
            worksheet.set_column(col_num, col_num, max(len(value) + 2, 15))

        # Sumário de métricas no topo (opcional)
        if metrics_summary:
            worksheet.write(0, 0, 'Metric', header_fmt)
            worksheet.write(0, 1, 'Value', header_fmt)
            for idx, (k, v) in enumerate(metrics_summary.items(), start=1):
                worksheet.write(idx, 0, k)
                worksheet.write(idx, 1, v)

        # Gráfico Impressões vs. Cliques
        chart = workbook.add_chart({'type': 'column'})
        n = len(df)
        # Supondo: col B = name, col D = impressions, col E = clicks
        chart.add_series({
            'name':       'Impressions',
            'categories': [sheet_name, 5, 1, 5+n-1, 1],
            'values':     [sheet_name, 5, 3, 5+n-1, 3],
        })
        chart.add_series({
            'name':       'Clicks',
            'categories': [sheet_name, 5, 1, 5+n-1, 1],
            'values':     [sheet_name, 5, 4, 5+n-1, 4],
        })
        chart.set_title ({'name': f'{sheet_name} Performance'})
        chart.set_x_axis({'name': 'Campaign'})
        chart.set_y_axis({'name': 'Count'})
        worksheet.insert_chart('H5', chart, {'x_scale':1.5, 'y_scale':1.5})

        writer.save()

    return output.getvalue()

# ───────── Endpoints revisados ─────────

@app.get("/export_google_active_campaigns_csv")
async def export_google_active_campaigns_csv(
    google_refresh_token: str = Query(..., alias="google_refresh_token")
):
    rows = await google_ads_list_active(google_refresh_token)
    metrics = {
        'Active Campaigns': len(rows),
        'Total Impressions': sum(r['impressions'] for r in rows),
        'Total Clicks':      sum(r['clicks'] for r in rows),
        'CTR (%)':           f"{(sum(r['clicks'] for r in rows) / sum(r['impressions'] for r in rows) * 100):.2f}"
    }
    xlsx = create_excel_report(rows, metrics_summary=metrics, sheet_name='Google Active')
    resp = {
        "fileName": "google_active_campaigns.xlsx",
        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "bytes": list(xlsx)
    }
    logging.debug(f"[export_google] GENERATED XLSX {len(xlsx)} bytes")
    return JSONResponse(content=resp)

@app.get("/export_meta_active_campaigns_csv")
async def export_meta_active_campaigns_csv(
    meta_account_id:   str = Query(..., alias="meta_account_id"),
    meta_access_token: str = Query(..., alias="meta_access_token")
):
    rows = await meta_ads_list_active(meta_account_id, meta_access_token)
    metrics = {
        'Active Campaigns': len(rows),
        'Total Impressions': sum(r['impressions'] for r in rows),
        'Total Clicks':      sum(r['clicks'] for r in rows),
        'CTR (%)':           f"{(sum(r['clicks'] for r in rows) / sum(r['impressions'] for r in rows) * 100):.2f}"
    }
    xlsx = create_excel_report(rows, metrics_summary=metrics, sheet_name='Meta Active')
    resp = {
        "fileName": "meta_active_campaigns.xlsx",
        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "bytes": list(xlsx)
    }
    logging.debug(f"[export_meta] GENERATED XLSX {len(xlsx)} bytes")
    return JSONResponse(content=resp)

@app.get("/export_combined_active_campaigns_csv")
async def export_combined_active_campaigns_csv(
    google_refresh_token: str = Query(..., alias="google_refresh_token"),
    meta_account_id:     str = Query(..., alias="meta_account_id"),
    meta_access_token:   str = Query(..., alias="meta_access_token")
):
    google_rows = await google_ads_list_active(google_refresh_token)
    meta_rows   = await meta_ads_list_active(meta_account_id, meta_access_token)
    all_rows    = google_rows + meta_rows
    metrics = {
        'Active Campaigns': len(all_rows),
        'Total Impressions': sum(r.get('impressions',0) for r in all_rows),
        'Total Clicks':      sum(r.get('clicks',0) for r in all_rows),
        'CTR (%)':           f"{(sum(r.get('clicks',0) for r in all_rows) / sum(r.get('impressions',0) for r in all_rows) * 100):.2f}"
    }
    xlsx = create_excel_report(all_rows, metrics_summary=metrics, sheet_name='Combined Active')
    resp = {
        "fileName": "combined_active_campaigns.xlsx",
        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "bytes": list(xlsx)
    }
    logging.debug(f"[export_combined] GENERATED XLSX {len(xlsx)} bytes")
    return JSONResponse(content=resp)

if __name__ == "__main__":
    logging.info("Starting export service on port 8080")
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
