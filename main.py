import logging, json, io
from datetime import datetime, timedelta
from collections import defaultdict

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import aiohttp
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GoogleRequest

import pandas as pd
from openpyxl.styles import Font, PatternFill, Alignment, NamedStyle, Border, Side
from openpyxl.chart import LineChart, Reference
from openpyxl.utils import get_column_letter
from openpyxl.formatting.rule import ColorScaleRule

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

# ───────── Helpers para Google e Meta (não alterados) ─────────
# ... (código google_ads_list, meta_ads_list, etc. conforme última versão) ...

# ───────── Excel report generator com styling profissional ─────────
def make_xlsx(df: pd.DataFrame, trends: pd.DataFrame, sheet_name: str) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        # Dados
        df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=6)
        wb = writer.book
        ws = writer.sheets[sheet_name]

        # 1) Título
        title = f"{sheet_name} Report — {datetime.now().date().isoformat()}"
        ws.merge_cells("A1:E1")
        cell = ws["A1"]
        cell.value = title
        cell.font = Font(size=16, bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor="0B5394")
        cell.alignment = Alignment(horizontal="center")

        # 2) KPI Summary (inspiração HubSpot: blocos claros de métricas) :contentReference[oaicite:0]{index=0}
        totals = {
            "Campanhas":     len(df),
            "Impressões":    int(df["Impressions"].sum()),
            "Cliques":       int(df["Clicks"].sum()),
            "CTR (%) Média": round(df["CTR (%)"].mean(), 2)
        }
        row = 3
        for i, (k, v) in enumerate(totals.items(), start=1):
            ws.cell(row=row, column= i*2-1, value=k).font  = Font(bold=True)
            ws.cell(row=row, column= i*2 , value=v).font  = Font(bold=True)
            ws.cell(row=row, column= i*2-1).fill = PatternFill("solid", fgColor="C9DAF8")
            ws.cell(row=row, column= i*2 ).fill = PatternFill("solid", fgColor="C9DAF8")

        # 3) Cabeçalho formatado e banded rows :contentReference[oaicite:1]{index=1}
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill("solid", fgColor="0B5394")
        thin_border = Border(left=Side(), right=Side(), top=Side(), bottom=Side())

        header_row = 6
        for idx, col in enumerate(df.columns, start=1):
            cell = ws.cell(row=header_row, column=idx, value=col)
            cell.font  = header_font
            cell.fill  = header_fill
            cell.border= thin_border
            ws.column_dimensions[get_column_letter(idx)].width = max(len(col)+2, 15)

        # banded rows
        for r in range(header_row+1, header_row+1+len(df)):
            fill = PatternFill("solid", fgColor="F2F2F2") if r%2==1 else None
            for c in range(1, len(df.columns)+1):
                if fill:
                    ws.cell(row=r, column=c).fill = fill

        # 4) Freeze e filtro
        ws.auto_filter.ref = f"A{header_row}: {get_column_letter(len(df.columns))}{header_row+len(df)}"
        ws.freeze_panes = ws[f"A{header_row+1}"]

        # 5) Formatação numérica
        for r in ws.iter_rows(min_row=header_row+1, min_col=1, max_col=len(df.columns), max_row=header_row+len(df)):
            for cell in r:
                if cell.column_letter in ['D','E']:  # Impressions, Clicks
                    cell.number_format = "#,##0"
                if cell.column_letter == 'F':         # CTR (%)
                    cell.number_format = "0.00"
                if cell.column_letter in ['G','H']:  # Spend, CPC
                    cell.number_format = "#,##0.00"

        # 6) Conditional formatting no CTR (%) (col F) :contentReference[oaicite:2]{index=2}
        cf_rule = ColorScaleRule(start_type="min", start_color="F8696B",
                                 mid_type="percentile", mid_value=50, mid_color="FFEB84",
                                 end_type="max", end_color="63BE7B")
        ctr_col = get_column_letter(df.columns.get_loc("CTR (%)")+1)
        ws.conditional_formatting.add(
            f"{ctr_col}{header_row+1}:{ctr_col}{header_row+len(df)}", cf_rule
        )

        # 7) Aba de Trends com gráfico
        if trends is not None:
            trends.to_excel(writer, sheet_name="Trends", index=False)
            ws2 = writer.sheets["Trends"]
            chart = LineChart()
            chart.title = f"{sheet_name} Trends (7d)"
            chart.x_axis.title = "Date"
            chart.y_axis.title = "Count"
            max_row = len(trends) + 1
            data = Reference(ws2, min_col=2, min_row=1, max_col=3, max_row=max_row)
            cats = Reference(ws2, min_col=1, min_row=2, max_row=max_row)
            chart.add_data(data, titles_from_data=True)
            chart.set_categories(cats)
            ws2.add_chart(chart, "E2")

    return buf.getvalue()

# ───────── Endpoints XLSX ─────────
@app.get("/export_combined_active_campaigns_csv")
async def export_combined_xlsx(
    google_refresh_token: str = Query(..., alias="google_refresh_token"),
    meta_account_id:     str = Query(..., alias="meta_account_id"),
    meta_access_token:   str = Query(..., alias="meta_access_token")
):
    # ... busca g_df, g_tr, m_df, m_tr ...
    df = pd.concat([g_df, m_df], ignore_index=True)
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

# repita export_google_xlsx e export_meta_xlsx de modo similar…

