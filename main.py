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

# ───────── API Helpers ─────────

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

# ───────── Data Fetchers ─────────

async def google_ads_list(refresh_token: str, with_trends: bool = False):
    token = await get_access_token(refresh_token)
    cid   = await discover_customer_id(token)
    url   = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/googleAds:search"
    headers = {"Authorization": f"Bearer {token}", "developer-token": DEVELOPER_TOKEN}

    q_active = """
        SELECT campaign.id, campaign.name, campaign.status,
               metrics.impressions, metrics.clicks, metrics.cost_micros
        FROM campaign
        WHERE campaign.status = 'ENABLED'
    """
    async with aiohttp.ClientSession() as sess:
        async with sess.post(url, headers=headers, json={"query": q_active}) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, f"Google search: {text}")
            results = json.loads(text).get("results", [])
    rows = []
    for r in results:
        imp = int(r["metrics"]["impressions"])
        clk = int(r["metrics"]["clicks"])
        micros = r["metrics"].get("costMicros", 0)
        spend = float(micros) / 1e6
        rows.append({
            "Campaign ID": r["campaign"]["id"],
            "Name":        r["campaign"]["name"],
            "Status":      r["campaign"]["status"],
            "Impressions": imp,
            "Clicks":      clk,
            "Spend":       round(spend, 2),
            "CTR (%)":     round(clk / max(imp,1) * 100, 2),
            "CPC":         round(spend / max(clk,1), 2)
        })
    df = pd.DataFrame(rows)

    trends = None
    if with_trends:
        q_trend = """
            SELECT segments.date, metrics.impressions, metrics.clicks, metrics.cost_micros
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
        by_date = defaultdict(lambda: {"Impressions":0, "Clicks":0, "Spend":0})
        for r in results:
            d = r["segments"]["date"]
            by_date[d]["Impressions"] += int(r["metrics"]["impressions"])
            by_date[d]["Clicks"]      += int(r["metrics"]["clicks"])
            micros = r["metrics"].get("costMicros", 0)
            by_date[d]["Spend"]       += float(micros) / 1e6
        dates = sorted(by_date)
        trends = pd.DataFrame({
            "Date":        dates,
            "Impressions": [by_date[d]["Impressions"] for d in dates],
            "Clicks":      [by_date[d]["Clicks"] for d in dates],
            "Spend":       [round(by_date[d]["Spend"], 2) for d in dates],
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
        m = map_ins.get(c["id"], {})
        imp = int(m.get("impressions", 0))
        clk = int(m.get("clicks", 0))
        spd = float(m.get("spend", 0))
        rows.append({
            "Campaign ID": c["id"],
            "Name":        c["name"],
            "Status":      c["status"],
            "Impressions": imp,
            "Clicks":      clk,
            "Spend":       round(spd, 2),
            "CTR (%)":     round(clk / max(imp,1) * 100, 2),
            "CPC":         round(spd / max(clk,1), 2)
        })
    df = pd.DataFrame(rows)

    trends = None
    if with_trends:
        by_date = defaultdict(lambda: {"Impressions":0, "Clicks":0, "Spend":0})
        for i in insights:
            dt = i.get("date_start")
            by_date[dt]["Impressions"] += int(i.get("impressions", 0))
            by_date[dt]["Clicks"]      += int(i.get("clicks", 0))
            by_date[dt]["Spend"]       += float(i.get("spend", 0))
        dates = sorted(by_date)
        trends = pd.DataFrame({
            "Date":        dates,
            "Impressions": [by_date[d]["Impressions"] for d in dates],
            "Clicks":      [by_date[d]["Clicks"] for d in dates],
            "Spend":       [round(by_date[d]["Spend"], 2) for d in dates],
        })

    return df, trends

# ───────── XLSX Formatter ─────────

def make_xlsx(df: pd.DataFrame, trends: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        sheet = "Report"
        df.to_excel(writer, sheet_name=sheet, index=False, startrow=6)
        wb = writer.book
        ws = writer.sheets[sheet]

        # app colors
        primary   = "7F56D9"
        secondary = "E839BC"
        header_font = Font(bold=True, color="FFFFFF")
        val_font    = Font(bold=True, size=14, color="FFFFFF")
        align       = Alignment(horizontal="center", vertical="center")

        # 1) KPI cards with clickable links
        kpis = {
            "Active Campaigns": len(df),
            "Impressions":      int(df["Impressions"].sum()),
            "Clicks":           int(df["Clicks"].sum()),
            "Spend":            round(df["Spend"].sum(),2)
        }
        # chart positions for each metric
        positions = {
            "Active Campaigns": ("F20", None),
            "Impressions":      ("F35", ("A", 8 + len(df))),
            "Clicks":           ("F50", ("A", 8 + len(df) + len(trends) + 2)),
            "Spend":            ("F65", ("A", 8 + len(df) + len(trends) * 2 + 4))
        }
        col = 1
        for title, value in kpis.items():
            # title
            ws.merge_cells(1, col, 1, col+1)
            c1 = ws.cell(1, col, title)
            c1.font  = header_font; c1.fill = PatternFill("solid", fgColor=primary); c1.alignment = align
            # value
            ws.merge_cells(2, col, 2, col+1)
            c2 = ws.cell(2, col)
            link_cell = positions[title][1]
            if link_cell:
                link = f"'{sheet}'!{link_cell[0]}{link_cell[1]}"
                c2.value = f'=HYPERLINK("{link}", "{value}")'
            else:
                c2.value = value
            c2.font  = val_font; c2.fill = PatternFill("solid", fgColor=secondary); c2.alignment = align
            col += 2

        # 2) Data table header
        for idx, col_name in enumerate(df.columns, start=1):
            cell = ws.cell(6, idx, col_name)
            cell.font = header_font
            cell.fill = PatternFill("solid", fgColor=secondary)
            cell.alignment = align
            ws.column_dimensions[get_column_letter(idx)].width = max(len(col_name)+2, 15)
        ws.freeze_panes = "A7"
        ws.sheet_view.showGridLines = False
        max_row, max_col = df.shape
        tab_ref = f"A6:{get_column_letter(max_col)}{6+max_row}"
        table = Table(displayName="DataTable", ref=tab_ref)
        table.tableStyleInfo = TableStyleInfo("TableStyleLight9", showRowStripes=True)
        ws.add_table(table)

        # 3) Multiple charts inline
        if trends is not None:
            start = 8 + max_row
            # write trends data
            for i, (_, row) in enumerate(trends.iterrows(), start=0):
                ws.cell(start+i, 1, row["Date"])
                ws.cell(start+i, 2, row["Impressions"])
                ws.cell(start+i, 3, row["Clicks"])
                ws.cell(start+i, 4, row["Spend"])
            # Impressions + Clicks chart
            cht1 = LineChart()
            cht1.title = "Impr. & Clicks (7d)"
            ref_data = Reference(ws, 2, start, 3, start+len(trends)-1)
            ref_cat  = Reference(ws, 1, start+1, start+len(trends)-1)
            cht1.add_data(ref_data, titles_from_data=True)
            cht1.set_categories(ref_cat)
            ws.add_chart(cht1, positions["Impressions"][0])
            # Clicks only chart
            cht2 = LineChart()
            cht2.title = "Clicks (7d)"
            ref_clicks = Reference(ws, 3, start, 3, start+len(trends)-1)
            cht2.add_data(ref_clicks, titles_from_data=False)
            cht2.set_categories(ref_cat)
            ws.add_chart(cht2, positions["Clicks"][0])
            # Spend chart
            cht3 = LineChart()
            cht3.title = "Spend (7d)"
            ref_spend = Reference(ws, 4, start, 4, start+len(trends)-1)
            cht3.add_data(ref_spend, titles_from_data=False)
            cht3.set_categories(ref_cat)
            ws.add_chart(cht3, positions["Spend"][0])

    return buf.getvalue()

# ───────── Endpoints ─────────

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
    tr["Spend"]       = tr["Spend_G"] + tr["Spend_M"]
    trends = tr[["Date","Impressions","Clicks","Spend"]]
    xlsx = make_xlsx(df, trends)
    return JSONResponse({
        "fileName": "combined_active_campaigns.xlsx",
        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "bytes": list(xlsx)
    })

if __name__ == "__main__":
    logging.info("Starting export on port 8080")
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
