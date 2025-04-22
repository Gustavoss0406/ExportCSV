import logging
import json
import io
from datetime import datetime, timedelta
from collections import defaultdict

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
    creds = Credentials(token=None, refresh_token=refresh_token,
                        token_uri="https://oauth2.googleapis.com/token",
                        client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    creds.refresh(GoogleRequest())
    return creds.token

async def discover_customer_id(token: str) -> str:
    url = f"https://googleads.googleapis.com/{API_VERSION}/customers:listAccessibleCustomers"
    headers = {"Authorization": f"Bearer {token}", "developer-token": DEVELOPER_TOKEN}
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, headers=headers) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(502, text)
            names = json.loads(text).get("resourceNames", [])
            if not names:
                raise HTTPException(502, "No accessible customers")
            return names[0].split("/")[-1]

async def google_ads_list(refresh_token: str, with_trends: bool = False):
    token       = await get_access_token(refresh_token)
    cid         = await discover_customer_id(token)
    base_url    = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/googleAds:search"
    headers     = {"Authorization": f"Bearer {token}", "developer-token": DEVELOPER_TOKEN}
    # active campaigns
    q_active = """
        SELECT campaign.id, campaign.name, campaign.status,
               metrics.impressions, metrics.clicks
        FROM campaign
        WHERE campaign.status = 'ENABLED'
    """
    async with aiohttp.ClientSession() as sess:
        async with sess.post(base_url, headers=headers, json={"query": q_active}) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, text)
            results = json.loads(text).get("results", [])
    rows = []
    for r in results:
        imp = int(r["metrics"]["impressions"])
        clk = int(r["metrics"]["clicks"])
        rows.append({
            "Campaign ID":    r["campaign"]["id"],
            "Name":           r["campaign"]["name"],
            "Status":         r["campaign"]["status"],
            "Impressions":    imp,
            "Clicks":         clk,
            "CTR (%)":        round(clk / max(imp,1) * 100, 2)
        })

    trends = None
    if with_trends:
        q_trend = f"""
            SELECT segments.date, metrics.impressions, metrics.clicks
            FROM campaign
            WHERE campaign.status = 'ENABLED'
              AND segments.date DURING LAST_7_DAYS
        """
        async with aiohttp.ClientSession() as sess:
            async with sess.post(base_url, headers=headers, json={"query": q_trend}) as resp:
                text = await resp.text()
                if resp.status != 200:
                    raise HTTPException(resp.status, text)
                results = json.loads(text).get("results", [])
        by_date = defaultdict(lambda: {"Impressions":0, "Clicks":0})
        for r in results:
            d = r["segments"]["date"]
            by_date[d]["Impressions"] += int(r["metrics"]["impressions"])
            by_date[d]["Clicks"]      += int(r["metrics"]["clicks"])
        dates = sorted(by_date)
        trends = pd.DataFrame({
            "Date": dates,
            "Impressions": [by_date[d]["Impressions"] for d in dates],
            "Clicks":      [by_date[d]["Clicks"] for d in dates]
        })
    return pd.DataFrame(rows), trends

# ───────── Helpers for Meta Ads ─────────
async def meta_ads_list(refresh_token: str, account_id: str, with_trends: bool = False):
    # campaigns
    url_campaigns = f"https://graph.facebook.com/v16.0/act_{account_id}/campaigns"
    params = {"fields":"id,name,status", "access_token": refresh_token}
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url_campaigns, params=params) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, text)
            data = json.loads(text).get("data", [])
    ids = [c["id"] for c in data if c["status"]=="ACTIVE"]

    # insights
    ins_url = f"https://graph.facebook.com/v16.0/act_{account_id}/insights"
    since = (datetime.now().date() - timedelta(days=7)).isoformat()
    until = datetime.now().date().isoformat()
    ins_params = {
        "level": "campaign",
        "fields": "campaign_id,impressions,clicks,spend",
        "time_range": json.dumps({"since": since, "until": until}),
        "access_token": refresh_token
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.get(ins_url, params=ins_params) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, text)
            insights = json.loads(text).get("data", [])
    map_ins = {i["campaign_id"]: i for i in insights}

    rows = []
    for c in data:
        if c["id"] not in ids: continue
        m = map_ins.get(c["id"], {})
        imp = int(m.get("impressions",0))
        clk = int(m.get("clicks",0))
        spd = float(m.get("spend",0))
        rows.append({
            "Campaign ID": c["id"],
            "Name":        c["name"],
            "Status":      c["status"],
            "Impressions": imp,
            "Clicks":      clk,
            "Spend":       round(spd,2),
            "CTR (%)":     round(clk / max(imp,1) * 100, 2),
            "CPC":         round(spd / max(clk,1), 2)
        })

    trends = None
    if with_trends:
        ins_params["time_range"] = json.dumps({"since": since, "until": until})
        async with aiohttp.ClientSession() as sess:
            async with sess.get(ins_url, params=ins_params) as resp:
                text = await resp.text()
                if resp.status != 200:
                    raise HTTPException(resp.status, text)
                data = json.loads(text).get("data", [])
        by_date = defaultdict(lambda: {"Impressions":0,"Clicks":0})
        for d in data:
            dt = d["date_start"]
            by_date[dt]["Impressions"] += int(d["impressions"])
            by_date[dt]["Clicks"]      += int(d["clicks"])
        dates = sorted(by_date)
        trends = pd.DataFrame({
            "Date": dates,
            "Impressions": [by_date[d]["Impressions"] for d in dates],
            "Clicks":      [by_date[d]["Clicks"] for d in dates]
        })

    return pd.DataFrame(rows), trends

# ───────── Excel report generator ─────────
def make_xlsx(df, trends, sheet_name):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=4)
        wb = writer.book
        ws = writer.sheets[sheet_name]

        # header format
        fmt_hdr = wb.add_format({"bold":True, "bg_color":"#4CAF50", "font_color":"#FFFFFF"})
        for col_num, value in enumerate(df.columns):
            ws.write(4, col_num, value, fmt_hdr)
            ws.set_column(col_num, col_num, max(len(value)+2, 15))

        # summary at top
        total = {
            "Count": len(df),
            "Impressions": int(df["Impressions"].sum()),
            "Clicks": int(df["Clicks"].sum()),
            "CTR (%)": round(df["CTR (%)"].mean(),2)
        }
        ws.write(0, 0, "Metric", fmt_hdr)
        ws.write(0, 1, "Value", fmt_hdr)
        for i, (k,v) in enumerate(total.items(), start=1):
            ws.write(i, 0, k)
            ws.write(i, 1, v)

        # trend chart
        if trends is not None:
            trends.to_excel(writer, sheet_name="Trends", index=False)
            ws2 = writer.sheets["Trends"]
            chart = wb.add_chart({"type":"line"})
            max_row = len(trends)
            chart.add_series({
                "name":       "Impressions",
                "categories": ["Trends", 1, 0, max_row, 0],
                "values":     ["Trends", 1, 1, max_row, 1],
            })
            chart.add_series({
                "name":       "Clicks",
                "categories": ["Trends", 1, 0, max_row, 0],
                "values":     ["Trends", 1, 2, max_row, 2],
            })
            chart.set_title({"name": f"{sheet_name} Trends (last 7d)"})
            chart.set_x_axis({"name": "Date"})
            chart.set_y_axis({"name": "Count"})
            ws2.insert_chart("E2", chart, {"x_scale":1.5, "y_scale":1.2})

        writer.save()
    return output.getvalue()

# ───────── Endpoints exporting XLSX ─────────

@app.get("/export_google_active_campaigns_csv")
async def export_google_xlsx(google_refresh_token: str = Query(..., alias="google_refresh_token")):
    df, trends = await google_ads_list(google_refresh_token, with_trends=True)
    xlsx = make_xlsx(df, trends, "Google Active")
    return JSONResponse({
        "fileName": "google_active_campaigns.xlsx",
        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "bytes": list(xlsx)
    })

@app.get("/export_meta_active_campaigns_csv")
async def export_meta_xlsx(
    meta_account_id: str = Query(..., alias="meta_account_id"),
    meta_access_token: str = Query(..., alias="meta_access_token")
):
    df, trends = await meta_ads_list(meta_access_token, meta_account_id, with_trends=True)
    xlsx = make_xlsx(df, trends, "Meta Active")
    return JSONResponse({
        "fileName": "meta_active_campaigns.xlsx",
        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "bytes": list(xlsx)
    })

@app.get("/export_combined_active_campaigns_csv")
async def export_combined_xlsx(
    google_refresh_token: str = Query(..., alias="google_refresh_token"),
    meta_account_id: str = Query(..., alias="meta_account_id"),
    meta_access_token: str = Query(..., alias="meta_access_token")
):
    g_df, g_tr = await google_ads_list(google_refresh_token, with_trends=True)
    m_df, m_tr = await meta_ads_list(meta_access_token, meta_account_id, with_trends=True)
    df = pd.concat([g_df, m_df], ignore_index=True)
    # combine trends
    tr = pd.merge(g_tr, m_tr, on="Date", how="outer", suffixes=("_G","_M")).fillna(0)
    tr["Impressions"] = tr["Impressions_G"] + tr["Impressions_M"]
    tr["Clicks"]      = tr["Clicks_G"] + tr["Clicks_M"]
    tr = tr[["Date","Impressions","Clicks"]]

    xlsx = make_xlsx(df, tr, "Combined Active")
    return JSONResponse({
        "fileName": "combined_active_campaigns.xlsx",
        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "bytes": list(xlsx)
    })

if __name__ == "__main__":
    logging.info("Starting export service on port 8080")
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
