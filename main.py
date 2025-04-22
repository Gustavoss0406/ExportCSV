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
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.chart import LineChart, Reference
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

# === Settings ===
API_VERSION     = "v17"
DEVELOPER_TOKEN = "D4yv61IQ8R0JaE5dxrd1Uw"
CLIENT_ID       = "167266694231-g7hvta57r99etbp3sos3jfi7q7h4ef44.apps.googleusercontent.com"
CLIENT_SECRET   = "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"

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# ───────── Helpers for API calls ─────────

async def get_access_token(refresh_token: str) -> str:
    try:
        creds = Credentials(
            token=None, refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=CLIENT_ID, client_secret=CLIENT_SECRET
        )
        creds.refresh(GoogleRequest())
        return creds.token
    except Exception as e:
        logging.error("Google token refresh failed: %s", e)
        raise HTTPException(401, f"Falha ao renovar token Google: {e}")

async def discover_customer_id(token: str) -> str:
    url = f"https://googleads.googleapis.com/{API_VERSION}/customers:listAccessibleCustomers"
    headers = {"Authorization": f"Bearer {token}", "developer-token": DEVELOPER_TOKEN}
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, headers=headers) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(502, f"Google listAccessibleCustomers: {text}")
            names = json.loads(text).get("resourceNames", [])
            if not names:
                raise HTTPException(502, "Google: sem customers acessíveis")
            return names[0].split("/")[-1]

async def google_ads_list(refresh_token: str, with_trends: bool = False):
    token = await get_access_token(refresh_token)
    cid   = await discover_customer_id(token)
    url   = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/googleAds:search"
    headers = {"Authorization": f"Bearer {token}", "developer-token": DEVELOPER_TOKEN}

    q_active = """
        SELECT campaign.id, campaign.name, campaign.status,
               metrics.impressions, metrics.clicks
        FROM campaign
        WHERE campaign.status = 'ENABLED'
    """
    async with aiohttp.ClientSession() as sess:
        async with sess.post(url, headers=headers, json={"query": q_active}) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, f"Google search: {text}")
            results = json.loads(text).get("results", [])
    rows = [{
        "Campaign ID": r["campaign"]["id"],
        "Name":        r["campaign"]["name"],
        "Status":      r["campaign"]["status"],
        "Impressions": int(r["metrics"]["impressions"]),
        "Clicks":      int(r["metrics"]["clicks"]),
        "CTR (%)":     round(int(r["metrics"]["clicks"])/max(int(r["metrics"]["impressions"]),1)*100,2)
    } for r in results]
    df = pd.DataFrame(rows)

    trends = None
    if with_trends:
        q_trend = """
            SELECT segments.date, metrics.impressions, metrics.clicks
            FROM campaign
            WHERE campaign.status='ENABLED'
              AND segments.date DURING LAST_7_DAYS
        """
        async with aiohttp.ClientSession() as sess:
            async with sess.post(url, headers=headers, json={"query": q_trend}) as resp:
                text = await resp.text()
                if resp.status != 200:
                    raise HTTPException(resp.status, f"Google trend: {text}")
                results = json.loads(text).get("results", [])
        by_date = defaultdict(lambda: {"Impressions":0,"Clicks":0})
        for r in results:
            d = r["segments"]["date"]
            by_date[d]["Impressions"] += int(r["metrics"]["impressions"])
            by_date[d]["Clicks"]      += int(r["metrics"]["clicks"])
        dates = sorted(by_date)
        trends = pd.DataFrame({
            "Date":        dates,
            "Impressions": [by_date[d]["Impressions"] for d in dates],
            "Clicks":      [by_date[d]["Clicks"] for d in dates],
        })

    return df, trends

async def meta_ads_list(refresh_token: str, account_id: str, with_trends: bool = False):
    url_c = f"https://graph.facebook.com/v16.0/act_{account_id}/campaigns"
    params = {"fields":"id,name,status","access_token": refresh_token}
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url_c, params=params) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, f"Meta campaigns: {text}")
            data = json.loads(text).get("data", [])
    active = [c for c in data if c["status"]=="ACTIVE"]

    ins_url = f"https://graph.facebook.com/v16.0/act_{account_id}/insights"
    since = (datetime.now().date() - timedelta(days=7)).isoformat()
    ins_params = {
        "level":"campaign",
        "fields":"campaign_id,impressions,clicks,spend,date_start",
        "time_range": json.dumps({"since":since,"until":since}),
        "access_token": refresh_token
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.get(ins_url, params=ins_params) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, f"Meta insights: {text}")
            insights = json.loads(text).get("data", [])
    map_ins = {i["campaign_id"]: i for i in insights}

    rows = []
    for c in active:
        m   = map_ins.get(c["id"], {})
        rows.append({
            "Campaign ID": c["id"],
            "Name":        c["name"],
            "Status":      c["status"],
            "Impressions": int(m.get("impressions",0)),
            "Clicks":      int(m.get("clicks",0)),
            "Spend":       round(float(m.get("spend",0)),2),
            "CTR (%)":     round(int(m.get("clicks",0))/max(int(m.get("impressions",1)),1)*100,2),
            "CPC":         round(float(m.get("spend",0))/max(int(m.get("clicks",1)),1),2)
        })
    df = pd.DataFrame(rows)

    trends = None
    if with_trends:
        by_date = defaultdict(lambda: {"Impressions":0,"Clicks":0})
        for i in insights:
            dt = i["date_start"]
            by_date[dt]["Impressions"] += int(i.get("impressions",0))
            by_date[dt]["Clicks"]      += int(i.get("clicks",0))
        dates = sorted(by_date)
        trends = pd.DataFrame({
            "Date":        dates,
            "Impressions": [by_date[d]["Impressions"] for d in dates],
            "Clicks":      [by_date[d]["Clicks"] for d in dates],
        })

    return df, trends

def make_xlsx(df: pd.DataFrame, trends: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        sheet = "Report"
        df.to_excel(writer, sheet_name=sheet, index=False, startrow=6)
        wb = writer.book
        ws = writer.sheets[sheet]

        # Colors
        purple = "6A0DAD"
        pink   = "D81B60"
        header_font = Font(bold=True, color="FFFFFF")
        val_font    = Font(bold=True, size=14, color="FFFFFF")
        align = Alignment(horizontal="center", vertical="center")

        # 1) KPI section in same sheet
        kpis = {
            "Active Campaigns": len(df),
            "Impressions":      int(df["Impressions"].sum()),
            "Clicks":           int(df["Clicks"].sum()),
            "Avg CTR (%)":      round(df["CTR (%)"].mean(),2)
        }
        col = 1
        for title, value in kpis.items():
            # Title cell
            ws.merge_cells(start_row=1, start_column=col, end_row=1, end_column=col+1)
            c1 = ws.cell(row=1, column=col, value=title)
            c1.font  = header_font; c1.fill = PatternFill("solid", fgColor=purple); c1.alignment = align
            # Value cell
            ws.merge_cells(start_row=2, start_column=col, end_row=2, end_column=col+1)
            c2 = ws.cell(row=2, column=col, value=value)
            c2.font = val_font; c2.fill = PatternFill("solid", fgColor=pink); c2.alignment = align
            col += 2

        # 2) Data Table header style
        for idx, col_name in enumerate(df.columns, start=1):
            cell = ws.cell(row=6, column=idx, value=col_name)
            cell.font = header_font
            cell.fill = PatternFill("solid", fgColor=pink)
            cell.alignment = align
            ws.column_dimensions[get_column_letter(idx)].width = max(len(col_name)+2, 15)

        ws.freeze_panes = "A7"
        ws.sheet_view.showGridLines = False

        # Excel Table style
        max_row, max_col = df.shape
        tab_ref = f"A6:{get_column_letter(max_col)}{6+max_row}"
        table = Table(displayName="DataTable", ref=tab_ref)
        style = TableStyleInfo(name="TableStyleLight9", showRowStripes=True)
        table.tableStyleInfo = style
        ws.add_table(table)

        # 3) Trends chart below table
        if trends is not None:
            start_chart_row = 8 + max_row
            for r_idx, date in enumerate(trends["Date"], start=start_chart_row+1):
                ws.cell(row=r_idx, column=1, value=date)
                ws.cell(row=r_idx, column=2, value=trends.at[r_idx-start_chart_row-1, "Impressions"])
                ws.cell(row=r_idx, column=3, value=trends.at[r_idx-start_chart_row-1, "Clicks"])
            chart = LineChart()
            chart.title = "Trends (7d)"
            chart.x_axis.title = "Date"
            chart.y_axis.title = "Count"
            data_ref = Reference(ws, min_col=2, min_row=start_chart_row+1,
                                 max_col=3, max_row=start_chart_row+len(trends))
            cats_ref = Reference(ws, min_col=1, min_row=start_chart_row+1,
                                 max_row=start_chart_row+len(trends))
            chart.add_data(data_ref, titles_from_data=True)
            chart.set_categories(cats_ref)
            ws.add_chart(chart, f"E{start_chart_row}")

    return buf.getvalue()

# ───────── Endpoints XLSX ─────────

@app.get("/export_google_active_campaigns_csv")
async def export_google_xlsx(google_refresh_token: str = Query(..., alias="google_refresh_token")):
    df, trends = await google_ads_list(google_refresh_token, with_trends=True)
    xlsx = make_xlsx(df, trends)
    return JSONResponse({
        "fileName": "google_active_campaigns.xlsx",
        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "bytes": list(xlsx)
    })

@app.get("/export_meta_active_campaigns_csv")
async def export_meta_xlsx(
    meta_account_id:   str = Query(..., alias="meta_account_id"),
    meta_access_token: str = Query(..., alias="meta_access_token")
):
    df, trends = await meta_ads_list(meta_access_token, meta_account_id, with_trends=True)
    xlsx = make_xlsx(df, trends)
    return JSONResponse({
        "fileName": "meta_active_campaigns.xlsx",
        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "bytes": list(xlsx)
    })

@app.get("/export_combined_active_campaigns_csv")
async def export_combined_xlsx(
    google_refresh_token: str = Query(..., alias="google_refresh_token"),
    meta_account_id:     str = Query(..., alias="meta_account_id"),
    meta_access_token:   str = Query(..., alias="meta_access_token")
):
    g_df, g_tr = await google_ads_list(google_refresh_token, with_trends=True)
    m_df, m_tr = await meta_ads_list(meta_access_token, meta_account_id, with_trends=True)
    df = pd.concat([g_df, m_df], ignore_index=True)
    tr = pd.merge(g_tr, m_tr, on="Date", how="outer", suffixes=("_G","_M")).fillna(0)
    tr["Impressions"] = tr["Impressions_G"] + tr["Impressions_M"]
    tr["Clicks"]      = tr["Clicks_G"] + tr["Clicks_M"]
    trends = tr[["Date","Impressions","Clicks"]]
    xlsx = make_xlsx(df, trends)
    return JSONResponse({
        "fileName": "combined_active_campaigns.xlsx",
        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "bytes": list(xlsx)
    })

if __name__ == "__main__":
    logging.info("Starting export service on port 8080")
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
