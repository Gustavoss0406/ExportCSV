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
from openpyxl.chart import BarChart, PieChart, Reference
from openpyxl.utils import get_column_letter

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

    q = """
        SELECT campaign.id, campaign.name, campaign.status,
               campaign_budget.amount_micros,
               metrics.impressions, metrics.clicks, metrics.cost_micros,
               metrics.average_cpc, metrics.ctr, metrics.conversions
        FROM campaign
        WHERE campaign.status = 'ENABLED'
    """
    async with aiohttp.ClientSession() as sess:
        async with sess.post(url, headers=headers, json={"query": q}) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, f"Google search: {text}")
            results = json.loads(text).get("results", [])

    rows = []
    for r in results:
        budget = r.get("campaignBudget", {}).get("amountMicros", 0) / 1e6
        spend  = r["metrics"].get("costMicros", 0) / 1e6
        clicks = int(r["metrics"].get("clicks", 0))
        impr   = int(r["metrics"].get("impressions", 0))
        conv   = int(r["metrics"].get("conversions", 0))
        rows.append({
            "Campaign ID": r["campaign"]["id"],
            "Name":        r["campaign"]["name"],
            "Budget":      round(budget, 2),
            "Spend":       round(spend, 2),
            "Impressions": impr,
            "Clicks":      clicks,
            "CTR (%)":     round(r["metrics"].get("ctr", 0)*100, 2),
            "CPC":         round(r["metrics"].get("averageCpc", 0)/1e6, 2),
            "Conversions": conv,
            "CPA":         round(spend / max(conv,1), 2)
        })
    df = pd.DataFrame(rows)

    # Cálculo de variação semanal e tendências de 7 dias (opcional)
    trends, changes = None, {}
    if with_trends:
        q2 = """
            SELECT segments.date, metrics.impressions, metrics.clicks
            FROM campaign
            WHERE campaign.status = 'ENABLED'
              AND segments.date DURING LAST_14_DAYS
        """
        async with aiohttp.ClientSession() as sess:
            async with sess.post(url, headers=headers, json={"query": q2}) as resp:
                text = await resp.text()
                if resp.status != 200:
                    raise HTTPException(resp.status, f"Google trend: {text}")
                results2 = json.loads(text).get("results", [])

        by_date = defaultdict(lambda: {"Impressions":0,"Clicks":0})
        for r in results2:
            d = r["segments"]["date"]
            by_date[d]["Impressions"] += int(r["metrics"]["impressions"])
            by_date[d]["Clicks"]      += int(r["metrics"]["clicks"])
        dates = sorted(by_date)
        last7 = dates[-7:]
        prev7 = dates[-14:-7] if len(dates)>=14 else dates[:max(len(dates)-7,0)]
        ci = sum(by_date[d]["Impressions"] for d in last7)
        pi = sum(by_date[d]["Impressions"] for d in prev7) if prev7 else 0
        cc = sum(by_date[d]["Clicks"] for d in last7)
        pc = sum(by_date[d]["Clicks"] for d in prev7) if prev7 else 0
        changes = {
            "Impr Δ (%)": round((ci-pi)/max(pi,1)*100,2),
            "Clk Δ (%)":  round((cc-pc)/max(pc,1)*100,2)
        }
        trends = pd.DataFrame({
            "Date":        last7,
            "Impressions": [by_date[d]["Impressions"] for d in last7],
            "Clicks":      [by_date[d]["Clicks"] for d in last7]
        })

    return df, trends, changes

async def meta_ads_list(refresh_token: str, account_id: str, with_trends: bool = False):
    url_c = f"https://graph.facebook.com/v16.0/act_{account_id}/campaigns"
    params = {
        "fields":"id,name,status,amount_spent,daily_budget,lifetime_budget",
        "access_token": refresh_token
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url_c, params=params) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, f"Meta campaigns: {text}")
            data = json.loads(text).get("data", [])

    since_14 = (datetime.now().date() - timedelta(days=14)).isoformat()
    until    = datetime.now().date().isoformat()
    ins_url = f"https://graph.facebook.com/v16.0/act_{account_id}/insights"
    ins_params = {
        "level":"campaign",
        "fields":"campaign_id,impressions,clicks,spend,actions",
        "time_range": json.dumps({"since": since_14, "until": until}),
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
    for c in data:
        spent = float(c.get("amount_spent",0))
        raw_budget = c.get("lifetime_budget") or str(int(c.get("daily_budget",0))*30)
        budget = float(raw_budget)/100
        ins = map_ins.get(c["id"], {})
        clicks = int(ins.get("clicks",0))
        impr   = int(ins.get("impressions",0))
        conv = sum(int(a.get("value",0)) for a in ins.get("actions",[]))
        rows.append({
            "Campaign ID": c["id"],
            "Name":        c["name"],
            "Budget":      round(budget,2),
            "Spend":       round(spent,2),
            "Impressions": impr,
            "Clicks":      clicks,
            "CTR (%)":     round(clicks/max(impr,1)*100,2),
            "CPC":         round(spent/max(clicks,1),2),
            "Conversions": conv,
            "CPA":         round(spent/max(conv,1),2)
        })
    df = pd.DataFrame(rows)

    trends, changes = None, {}
    if with_trends:
        by_date = defaultdict(lambda: {"Impressions":0,"Clicks":0})
        for i in insights:
            d = i["date_start"]
            by_date[d]["Impressions"] += int(i.get("impressions",0))
            by_date[d]["Clicks"]      += int(i.get("clicks",0))
        dates = sorted(by_date)
        last7 = dates[-7:]
        prev7 = dates[-14:-7] if len(dates)>=14 else dates[:max(len(dates)-7,0)]
        ci = sum(by_date[d]["Impressions"] for d in last7)
        pi = sum(by_date[d]["Impressions"] for d in prev7) if prev7 else 0
        cc = sum(by_date[d]["Clicks"] for d in last7)
        pc = sum(by_date[d]["Clicks"] for d in prev7) if prev7 else 0
        changes = {
            "Impr Δ (%)": round((ci-pi)/max(pi,1)*100,2),
            "Clk Δ (%)":  round((cc-pc)/max(pc,1)*100,2)
        }
        trends = pd.DataFrame({
            "Date":        last7,
            "Impressions": [by_date[d]["Impressions"] for d in last7],
            "Clicks":      [by_date[d]["Clicks"] for d in last7]
        })

    return df, trends, changes

def make_xlsx(df: pd.DataFrame, trends: pd.DataFrame, changes: dict) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        # 1) Gravar dados na aba oculta
        df.to_excel(writer, sheet_name="Data", index=False)
        wb = writer.book
        data_ws = wb["Data"]
        data_ws.sheet_state = "hidden"

        # 2) Criar aba Dashboard
        dash = wb.create_sheet("Dashboard")
        primary, secondary = "7F56D9", "E839BC"
        header_font = Font(bold=True, color="FFFFFF")
        val_font    = Font(bold=True, size=14, color="FFFFFF")
        align = Alignment(horizontal="center", vertical="center")

        # ─── KPI boxes ───
        kpis = {
            "Total Budget":  round(df["Budget"].sum(),2),
            "Total Spend":   round(df["Spend"].sum(),2),
            "+ / -":         round(df["Budget"].sum() - df["Spend"].sum(),2),
            "Impressions":   int(df["Impressions"].sum()),
            "Clicks":        int(df["Clicks"].sum()),
            "Conversions":   int(df["Conversions"].sum()),
            "CPC Avg":       round(df["Spend"].sum()/max(df["Clicks"].sum(),1),2),
            "CPA Avg":       round(df["Spend"].sum()/max(df["Conversions"].sum(),1),2),
            "CTR (%)":       round(df["Clicks"].sum()/max(df["Impressions"].sum(),1)*100,2),
            **changes
        }
        col = 1
        for title, value in kpis.items():
            dash.merge_cells(start_row=1, start_column=col, end_row=1, end_column=col+2)
            c1 = dash.cell(row=1, column=col, value=title)
            c1.font, c1.fill, c1.alignment = header_font, PatternFill("solid", fgColor=primary), align
            dash.merge_cells(start_row=2, start_column=col, end_row=2, end_column=col+2)
            c2 = dash.cell(row=2, column=col, value=value)
            c2.font, c2.fill, c2.alignment = val_font, PatternFill("solid", fgColor=secondary), align
            col += 3

        # ─── Charts ───
        n = len(df)
        # referências no sheet "Data"
        # Name: coluna 2, Budget:3, Spend:4, CPA:10, CPC:8, Conversions:9, CTR:7
        ref_names = Reference(data_ws, min_col=2, min_row=2, max_row=n+1)

        # Budget vs Spend
        chart1 = BarChart()
        chart1.title = "Budget vs Spend per Campaign"
        chart1.add_data(Reference(data_ws, min_col=3, min_row=1, max_col=4, max_row=n+1), titles_from_data=True)
        chart1.set_categories(ref_names)
        chart1.series[0].graphicalProperties.solidFill = primary
        chart1.series[1].graphicalProperties.solidFill = secondary
        dash.add_chart(chart1, "A5")

        # CPA per Campaign
        chart2 = BarChart()
        chart2.title = "CPA per Campaign"
        chart2.add_data(Reference(data_ws, min_col=10, min_row=1, max_row=n+1), titles_from_data=True)
        chart2.set_categories(ref_names)
        chart2.series[0].graphicalProperties.solidFill = primary
        dash.add_chart(chart2, "K5")

        # CPC per Campaign
        chart3 = BarChart()
        chart3.title = "CPC per Campaign"
        chart3.add_data(Reference(data_ws, min_col=8, min_row=1, max_row=n+1), titles_from_data=True)
        chart3.set_categories(ref_names)
        chart3.series[0].graphicalProperties.solidFill = primary
        dash.add_chart(chart3, "A20")

        # Acquisitions (Pie)
        pie = PieChart()
        pie.title = "Acquisitions"
        pie.add_data(Reference(data_ws, min_col=9, min_row=1, max_row=n+1), titles_from_data=True)
        pie.set_categories(ref_names)
        dash.add_chart(pie, "K20")

        # CTR per Campaign (horizontal)
        chart4 = BarChart(orientation="bar")
        chart4.title = "CTR per Campaign"
        chart4.add_data(Reference(data_ws, min_col=7, min_row=1, max_row=n+1), titles_from_data=True)
        chart4.set_categories(ref_names)
        chart4.series[0].graphicalProperties.solidFill = primary
        dash.add_chart(chart4, "A35")

    return buf.getvalue()

# ───────── Endpoints XLSX ─────────

@app.get("/export_google_active_campaigns_csv")
async def export_google_xlsx(google_refresh_token: str = Query(..., alias="google_refresh_token")):
    df, trends, changes = await google_ads_list(google_refresh_token, with_trends=True)
    xlsx = make_xlsx(df, trends, changes)
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
    df, trends, changes = await meta_ads_list(meta_access_token, meta_account_id, with_trends=True)
    xlsx = make_xlsx(df, trends, changes)
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
    g_df, g_tr, g_ch = await google_ads_list(google_refresh_token, with_trends=True)
    m_df, m_tr, m_ch = await meta_ads_list(meta_access_token, meta_account_id, with_trends=True)
    df = pd.concat([g_df, m_df], ignore_index=True)
    combined_changes = {
        "Impr Δ (%)": round((g_ch.get("Impr Δ (%)",0)+m_ch.get("Impr Δ (%)",0))/2,2),
        "Clk Δ (%)":  round((g_ch.get("Clk Δ (%)",0)+m_ch.get("Clk Δ (%)",0))/2,2)
    }
    xlsx = make_xlsx(df, None, combined_changes)
    return JSONResponse({
        "fileName": "combined_active_campaigns.xlsx",
        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "bytes": list(xlsx)
    })

if __name__ == "__main__":
    logging.info("Starting export service on port 8080")
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
