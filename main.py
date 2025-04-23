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

    # Métricas adicionais
    q_active = """
        SELECT campaign.id, campaign.name, campaign.status,
               metrics.impressions, metrics.clicks,
               metrics.cost_micros, metrics.average_cpc, metrics.ctr, metrics.conversions
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
        cost = r["metrics"].get("costMicros", 0) / 1e6
        clicks = int(r["metrics"].get("clicks", 0))
        impressions = int(r["metrics"].get("impressions", 0))
        rows.append({
            "Campaign ID":      r["campaign"]["id"],
            "Name":             r["campaign"]["name"],
            "Status":           r["campaign"]["status"],
            "Impressions":      impressions,
            "Clicks":           clicks,
            "Cost":             round(cost, 2),
            "Avg CPC":          round(r["metrics"].get("averageCpc", 0)/1e6, 2),
            "CTR (%)":          round(r["metrics"].get("ctr", 0)*100, 2),
            "Conversions":      int(r["metrics"].get("conversions", 0)),
            "Cost/Conv":        round(cost / max(int(r["metrics"].get("conversions", 1)),1), 2)
        })
    df = pd.DataFrame(rows)

    trends = None
    changes = {"Impressions Change (%)": 0.0, "Clicks Change (%)": 0.0}
    if with_trends:
        # Puxar 14 dias para calcular semana anterior x atual
        q_trend = """
            SELECT segments.date, metrics.impressions, metrics.clicks
            FROM campaign
            WHERE campaign.status = 'ENABLED'
              AND segments.date DURING LAST_14_DAYS
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
        # Separar últimas 7 datas e as 7 anteriores
        last7 = dates[-7:]
        prev7 = dates[-14:-7] if len(dates) >= 14 else dates[:max(len(dates)-7,0)]
        curr_imp = sum(by_date[d]["Impressions"] for d in last7)
        prev_imp = sum(by_date[d]["Impressions"] for d in prev7) if prev7 else 0
        curr_clk = sum(by_date[d]["Clicks"] for d in last7)
        prev_clk = sum(by_date[d]["Clicks"] for d in prev7) if prev7 else 0
        changes["Impressions Change (%)"] = round((curr_imp - prev_imp)/max(prev_imp,1)*100,2)
        changes["Clicks Change (%)"]      = round((curr_clk - prev_clk)/max(prev_clk,1)*100,2)

        # DataFrame de tendências de 7 dias + CTR diário
        trends = pd.DataFrame({
            "Date":        last7,
            "Impressions": [by_date[d]["Impressions"] for d in last7],
            "Clicks":      [by_date[d]["Clicks"] for d in last7]
        })
        trends["CTR (%)"] = trends.apply(lambda row: round(row["Clicks"]/max(row["Impressions"],1)*100,2), axis=1)

    return df, trends, changes

async def meta_ads_list(refresh_token: str, account_id: str, with_trends: bool = False):
    # Campaigns
    url_c = f"https://graph.facebook.com/v16.0/act_{account_id}/campaigns"
    params = {"fields":"id,name,status","access_token": refresh_token}
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url_c, params=params) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, f"Meta campaigns: {text}")
            data = json.loads(text).get("data", [])
    active = [c for c in data if c["status"]=="ACTIVE"]

    # Insights para 14 dias
    since_14 = (datetime.now().date() - timedelta(days=14)).isoformat()
    until    = datetime.now().date().isoformat()
    ins_url = f"https://graph.facebook.com/v16.0/act_{account_id}/insights"
    ins_params = {
        "level":"campaign",
        "fields":"campaign_id,impressions,clicks,spend,reach,frequency,date_start",
        "time_range": json.dumps({"since": since_14, "until": until}),
        "access_token": refresh_token
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.get(ins_url, params=ins_params) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, f"Meta insights: {text}")
            insights = json.loads(text).get("data", [])
    # Agrupar por campanha para tabela
    map_ins = {i["campaign_id"]: i for i in insights}
    rows = []
    for c in active:
        m = map_ins.get(c["id"], {})
        spend = float(m.get("spend",0))
        clicks = int(m.get("clicks",0))
        impr = int(m.get("impressions",0))
        rows.append({
            "Campaign ID": c["id"],
            "Name":        c["name"],
            "Status":      c["status"],
            "Impressions": impr,
            "Clicks":      clicks,
            "Spend":       round(spend,2),
            "CTR (%)":     round(clicks/max(impr,1)*100,2),
            "CPC":         round(spend/max(clicks,1),2),
            "Reach":       int(m.get("reach",0)),
            "Frequency":   round(float(m.get("frequency",0)),2)
        })
    df = pd.DataFrame(rows)

    trends = None
    changes = {"Impressions Change (%)": 0.0, "Clicks Change (%)": 0.0}
    if with_trends:
        # Agrupar por dia
        by_date = defaultdict(lambda: {"Impressions":0,"Clicks":0})
        for i in insights:
            dt = i["date_start"]
            by_date[dt]["Impressions"] += int(i.get("impressions",0))
            by_date[dt]["Clicks"]      += int(i.get("clicks",0))
        dates = sorted(by_date)
        last7 = dates[-7:]
        prev7 = dates[-14:-7] if len(dates) >= 14 else dates[:max(len(dates)-7,0)]
        curr_imp = sum(by_date[d]["Impressions"] for d in last7)
        prev_imp = sum(by_date[d]["Impressions"] for d in prev7) if prev7 else 0
        curr_clk = sum(by_date[d]["Clicks"] for d in last7)
        prev_clk = sum(by_date[d]["Clicks"] for d in prev7) if prev7 else 0
        changes["Impressions Change (%)"] = round((curr_imp - prev_imp)/max(prev_imp,1)*100,2)
        changes["Clicks Change (%)"]      = round((curr_clk - prev_clk)/max(prev_clk,1)*100,2)

        trends = pd.DataFrame({
            "Date":        last7,
            "Impressions": [by_date[d]["Impressions"] for d in last7],
            "Clicks":      [by_date[d]["Clicks"] for d in last7]
        })
        trends["CTR (%)"] = trends.apply(lambda row: round(row["Clicks"]/max(row["Impressions"],1)*100,2), axis=1)

    return df, trends, changes

def make_xlsx(df: pd.DataFrame, trends: pd.DataFrame, changes: dict) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        sheet = "Dashboard"
        wb = writer.book
        # Criar sheet
        ws = wb.create_sheet(sheet)
        # Ajustar colunas
        for col, width in zip(range(1, 21), [12,12,12,12,12,12,4,4,4,4,4,4,4,4,4,4,4,4,4,4]):
            ws.column_dimensions[get_column_letter(col)].width = width
        # KPI boxes
        kpis = {
            "Active Campaigns": len(df),
            "Total Impressions": int(df["Impressions"].sum()),
            "Total Clicks":      int(df["Clicks"].sum()),
            "Avg CTR (%)":       round(df["CTR (%)"].mean(),2),
            **changes
        }
        primary, secondary = "7F56D9", "E839BC"
        header_font = Font(bold=True, color="FFFFFF")
        val_font    = Font(bold=True, size=14, color="FFFFFF")
        align = Alignment(horizontal="center", vertical="center")

        col = 1
        for title, value in kpis.items():
            # Box título
            ws.merge_cells(start_row=1, start_column=col, end_row=1, end_column=col+2)
            c1 = ws.cell(row=1, column=col, value=title)
            c1.font  = header_font; c1.fill = PatternFill("solid", fgColor=primary); c1.alignment = align
            # Box valor
            ws.merge_cells(start_row=2, start_column=col, end_row=2, end_column=col+2)
            c2 = ws.cell(row=2, column=col, value=value)
            c2.font = val_font; c2.fill = PatternFill("solid", fgColor=secondary); c2.alignment = align
            col += 3

        # Inserir tabela de dados (em A5:F15 por exemplo)
        df.to_excel(writer, sheet_name=sheet, index=False, startrow=4, startcol=0)
        ws.freeze_panes = "A6"
        # Estilizar header da tabela
        for idx, col_name in enumerate(df.columns, start=1):
            cell = ws.cell(row=5, column=idx, value=col_name)
            cell.font = header_font
            cell.fill = PatternFill("solid", fgColor=secondary)
            cell.alignment = align

        # Budget vs Spend chart (barra) à esquerda
        bar = LineChart()
        bar.title = "Budget vs Spend por Campanha"
        # criar dados hipotéticos Budget/Spend extraídos do df
        # assumindo df contém colunas 'Budget' e 'Spend' caso você já as tenha
        # ou gere um df auxiliar antes
        # Aqui ilustrativo: uso Impressões e Cliques para demo
        data_ref = Reference(ws, min_col=4, min_row=5, max_col=5, max_row=5+len(df))
        cats_ref = Reference(ws, min_col=2, min_row=6, max_row=5+len(df))
        bar.add_data(data_ref, titles_from_data=True)
        bar.set_categories(cats_ref)
        bar.y_axis.title = "Valor"
        bar.x_axis.title = "Campanha"
        bar.style = 2
        bar.width = 12
        bar.height = 8
        bar.anchor = "A20"
        ws.add_chart(bar)

        # Pequenos charts em grade à direita
        charts_info = [
            ("CPA por Campanha",   6, 20, Reference(ws, min_col=6, min_row=5, max_col=6, max_row=5+len(df))),
            ("CPC por Campanha",   10,20, Reference(ws, min_col=7, min_row=5, max_col=7, max_row=5+len(df))),
            ("CTR por Campanha",   6, 35, Reference(ws, min_col=8, min_row=5, max_col=8, max_row=5+len(df))),
            ("Aquisições (%)",     10,35, None)  # abaixo criamos pie chart
        ]
        for title, col_pos, row_pos, data in charts_info:
            if title != "Aquisições (%)":
                c = LineChart()
                c.title = title
                c.add_data(data, titles_from_data=True)
                c.set_categories(cats_ref)
                c.style = 12
                c.width = 6
                c.height = 6
                c.anchor = get_column_letter(col_pos) + str(row_pos)
                ws.add_chart(c)
            else:
                # Pie de Aquisições
                pie = wb.create_chart('pie', title=title)
                # simular dados
                labels = [f"Campanha {i+1}" for i in range(len(df))]
                vals   = list(df["Clicks"])
                pie.add_data(Reference(ws, min_col=5, min_row=6, max_row=5+len(df)), titles_from_data=True)
                pie.set_categories(Reference(ws, min_col=2, min_row=6, max_row=5+len(df)))
                pie.width = 6
                pie.height = 6
                pie.anchor = get_column_letter(col_pos) + str(row_pos)
                ws.add_chart(pie)

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
    # Combinar tabelas
    df = pd.concat([g_df, m_df], ignore_index=True)
    # Combinar tendências
    tr = pd.merge(g_tr, m_tr, on="Date", how="outer", suffixes=("_G","_M")).fillna(0)
    tr["Impressions"] = tr["Impressions_G"] + tr["Impressions_M"]
    tr["Clicks"]      = tr["Clicks_G"] + tr["Clicks_M"]
    tr["CTR (%)"]     = tr.apply(lambda row: round(row["Clicks"]/max(row["Impressions"],1)*100,2), axis=1)
    trends = tr[["Date","Impressions","Clicks","CTR (%)"]]
    # Calcular variação combinada
    combined_changes = {
        "Impressions Change (%)": round((g_ch["Impressions Change (%)"] + m_ch["Impressions Change (%)"])/2, 2),
        "Clicks Change (%)":      round((g_ch["Clicks Change (%)"]      + m_ch["Clicks Change (%)"])/2, 2)
    }
    xlsx = make_xlsx(df, trends, combined_changes)
    return JSONResponse({
        "fileName": "combined_active_campaigns.xlsx",
        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "bytes": list(xlsx)
    })

if __name__ == "__main__":
    logging.info("Starting export service on port 8080")
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
