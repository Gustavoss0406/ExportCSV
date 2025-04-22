import logging
import json
import csv
import io
from datetime import datetime, timedelta
from collections import defaultdict
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
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

# ───────── Utility: ASCII sparkline ─────────
SPARK_BARS = ["▁","▂","▃","▄","▅","▆","▇","█"]
def sparkline(data):
    if not data:
        return ""
    mn, mx = min(data), max(data)
    span = mx - mn or 1
    result = ""
    for v in data:
        idx = int((v - mn) / span * (len(SPARK_BARS)-1))
        result += SPARK_BARS[idx]
    return result

# ───────── Helpers Google Ads ─────────
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
                raise HTTPException(502, text)
            names = json.loads(text).get("resourceNames", [])
            if not names:
                raise HTTPException(502, "No accessible customers")
            return names[0].split("/")[-1]

async def google_ads_list_active(refresh_token: str):
    token       = await get_access_token(refresh_token)
    cid         = await discover_customer_id(token)
    url         = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/googleAds:search"
    headers     = {
        "Authorization":   f"Bearer {token}",
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
        async with sess.post(url, headers=headers, json={"query":query}) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, text)
            results = json.loads(text).get("results", [])
    rows = []
    for r in results:
        imp = int(r["metrics"]["impressions"])
        clk = int(r["metrics"]["clicks"])
        rows.append({
            "id":          r["campaign"]["id"],
            "name":        r["campaign"]["name"],
            "status":      r["campaign"]["status"],
            "impressions": imp,
            "clicks":      clk,
            "ctr (%)":     round(clk / max(imp,1) * 100, 2)
        })
    return rows

async def google_ads_list_trends(refresh_token: str, days: int = 7):
    token       = await get_access_token(refresh_token)
    cid         = await discover_customer_id(token)
    url         = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/googleAds:search"
    headers     = {
        "Authorization":   f"Bearer {token}",
        "developer-token": DEVELOPER_TOKEN,
        "Content-Type":    "application/json"
    }
    query = f"""
        SELECT segments.date, metrics.impressions, metrics.clicks
        FROM campaign
        WHERE campaign.status = 'ENABLED'
          AND segments.date DURING LAST_{days}_DAYS
    """
    async with aiohttp.ClientSession() as sess:
        async with sess.post(url, headers=headers, json={"query":query}) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, text)
            results = json.loads(text).get("results", [])
    by_date = defaultdict(lambda: {"impressions":0,"clicks":0})
    for r in results:
        date = r["segments"]["date"]
        by_date[date]["impressions"] += int(r["metrics"]["impressions"])
        by_date[date]["clicks"]     += int(r["metrics"]["clicks"])
    # sort by date
    dates = sorted(by_date.keys())
    imps  = [by_date[d]["impressions"] for d in dates]
    clks  = [by_date[d]["clicks"] for d in dates]
    return dates, imps, clks

# ───────── Helpers Meta Ads ─────────
async def meta_ads_list_active(account_id: str, access_token: str):
    url = f"https://graph.facebook.com/v16.0/act_{account_id}/campaigns"
    params = {
        "fields":       "id,name,status,insights.date_preset(lifetime){impressions,clicks,spend}",
        "filtering":    json.dumps([{"field":"effective_status","operator":"IN","value":["ACTIVE"]}]),
        "access_token": access_token
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, params=params) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, text)
            data = json.loads(text).get("data", [])
    rows=[]
    for c in data:
        ins = c.get("insights",{}).get("data",[{}])[0]
        imp = int(ins.get("impressions",0))
        clk = int(ins.get("clicks",0))
        spd = float(ins.get("spend",0.0))
        rows.append({
            "id":           c["id"],
            "name":         c["name"],
            "status":       c["status"],
            "impressions":  imp,
            "clicks":       clk,
            "spend":        round(spd,2),
            "ctr (%)":      round(clk / max(imp,1) * 100,2),
            "cpc":          round(spd / max(clk,1),2)
        })
    return rows

async def meta_ads_list_trends(account_id: str, access_token: str, days: int = 7):
    url = f"https://graph.facebook.com/v16.0/act_{account_id}/insights"
    since = (datetime.now().date() - timedelta(days=days)).isoformat()
    until = datetime.now().date().isoformat()
    params = {
        "fields":         "date_start,impressions,clicks,spend",
        "time_increment": 1,
        "time_range":     json.dumps({"since":since,"until":until}),
        "filtering":      json.dumps([{"field":"effective_status","operator":"IN","value":["ACTIVE"]}]),
        "access_token":   access_token
    }
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, params=params) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise HTTPException(resp.status, text)
            data = json.loads(text).get("data", [])
    by_date = defaultdict(lambda: {"impressions":0,"clicks":0})
    for d in data:
        date = d["date_start"]
        by_date[date]["impressions"] += int(d["impressions"])
        by_date[date]["clicks"]     += int(d["clicks"])
    dates = sorted(by_date.keys())
    imps  = [by_date[d]["impressions"] for d in dates]
    clks  = [by_date[d]["clicks"] for d in dates]
    return dates, imps, clks

# ───────── Endpoint Google CSV com review e gráficos ─────────
@app.get("/export_google_active_campaigns_csv")
async def export_google_active_campaigns_csv(
    google_refresh_token: str = Query(..., alias="google_refresh_token")
):
    rows   = await google_ads_list_active(google_refresh_token)
    dates, imps, clks = await google_ads_list_trends(google_refresh_token)

    # sumários
    total_imp = sum(r["impressions"] for r in rows)
    total_clk = sum(r["clicks"] for r in rows)
    avg_ctr   = round(total_clk / max(total_imp,1) * 100, 2)
    review = (
        f"Google Ads: {len(rows)} campanhas ativas | "
        f"Total {total_imp} impressões, {total_clk} cliques | CTR médio {avg_ctr}%"
    )
    spark_imp = sparkline(imps)
    spark_clk = sparkline(clks)

    buf = io.StringIO()
    writer = csv.writer(buf)

    # Review geral
    writer.writerow(["Review", review])
    writer.writerow([])

    # Tendências (sparklines)
    writer.writerow(["Date"] + dates)
    writer.writerow(["Impressions Trend"] + [spark_imp])
    writer.writerow(["Clicks Trend"]     + [spark_clk])
    writer.writerow([])

    # Métricas gerais
    writer.writerow(["Metric","Value"])
    writer.writerow(["Active Campaigns", len(rows)])
    writer.writerow(["Total Impressions", total_imp])
    writer.writerow(["Total Clicks", total_clk])
    writer.writerow(["Average CTR (%)", avg_ctr])
    writer.writerow([])

    # Métricas por campanha
    writer.writerow(["Campaign ID","Name","Status","Impressions","Clicks","CTR (%)"])
    for r in rows:
        writer.writerow([
            r["id"], r["name"], r["status"],
            r["impressions"], r["clicks"], r["ctr (%)"]
        ])

    data = buf.getvalue().encode("utf-8")
    return JSONResponse({
        "fileName": "google_active_campaigns.csv",
        "mimeType": "text/csv",
        "bytes": list(data)
    })

# ───────── Endpoint Meta CSV com review e gráficos ─────────
@app.get("/export_meta_active_campaigns_csv")
async def export_meta_active_campaigns_csv(
    meta_account_id:   str = Query(..., alias="meta_account_id"),
    meta_access_token: str = Query(..., alias="meta_access_token")
):
    rows   = await meta_ads_list_active(meta_account_id, meta_access_token)
    dates, imps, clks = await meta_ads_list_trends(meta_account_id, meta_access_token)

    total_imp = sum(r["impressions"] for r in rows)
    total_clk = sum(r["clicks"] for r in rows)
    total_spd = round(sum(r["spend"] for r in rows),2)
    avg_ctr   = round(total_clk / max(total_imp,1) * 100,2)
    avg_cpc   = round(total_spd / max(total_clk,1),2)
    review = (
        f"Meta Ads: {len(rows)} campanhas ativas | "
        f"Total {total_imp} impressões, {total_clk} cliques, spend {total_spd} | "
        f"CTR {avg_ctr}%, CPC {avg_cpc}"
    )
    spark_imp = sparkline(imps)
    spark_clk = sparkline(clks)

    buf = io.StringIO()
    writer = csv.writer(buf)

    writer.writerow(["Review", review])
    writer.writerow([])

    writer.writerow(["Date"] + dates)
    writer.writerow(["Impressions Trend"] + [spark_imp])
    writer.writerow(["Clicks Trend"]     + [spark_clk])
    writer.writerow([])

    writer.writerow(["Metric","Value"])
    writer.writerow(["Active Campaigns", len(rows)])
    writer.writerow(["Total Impressions", total_imp])
    writer.writerow(["Total Clicks", total_clk])
    writer.writerow(["Total Spend", total_spd])
    writer.writerow(["Average CTR (%)", avg_ctr])
    writer.writerow(["Average CPC", avg_cpc])
    writer.writerow([])

    writer.writerow(["Campaign ID","Name","Status","Impressions","Clicks","Spend","CTR (%)","CPC"])
    for r in rows:
        writer.writerow([
            r["id"], r["name"], r["status"],
            r["impressions"], r["clicks"], r["spend"],
            r["ctr (%)"], r["cpc"]
        ])

    data = buf.getvalue().encode("utf-8")
    return JSONResponse({
        "fileName": "meta_active_campaigns.csv",
        "mimeType": "text/csv",
        "bytes": list(data)
    })

# ───────── Endpoint Combined CSV com review e gráficos ─────────
@app.get("/export_combined_active_campaigns_csv")
async def export_combined_active_campaigns_csv(
    google_refresh_token: str = Query(..., alias="google_refresh_token"),
    meta_account_id:     str = Query(..., alias="meta_account_id"),
    meta_access_token:   str = Query(..., alias="meta_access_token")
):
    g_rows = await google_ads_list_active(google_refresh_token)
    m_rows = await meta_ads_list_active(meta_account_id, meta_access_token)
    rows   = g_rows + m_rows

    # trends combinadas (somente impressões e cliques de ambas)
    g_dates, g_imps, g_clks = await google_ads_list_trends(google_refresh_token)
    m_dates, m_imps, m_clks = await meta_ads_list_trends(meta_account_id, meta_access_token)
    # assume mesmas datas
    dates = g_dates if g_dates else m_dates
    imps  = [gi + mi for gi,mi in zip(g_imps, m_imps)]
    clks  = [gc + mc for gc,mc in zip(g_clks, m_clks)]
    
    total_imp = sum(r["impressions"] for r in rows)
    total_clk = sum(r["clicks"] for r in rows)
    avg_ctr   = round(total_clk / max(total_imp,1) * 100,2)
    spark_imp = sparkline(imps)
    spark_clk = sparkline(clks)
    review = (
        f"Combined Ads: Google {len(g_rows)} + Meta {len(m_rows)} = {len(rows)} campanhas | "
        f"Imps {total_imp}, Clicks {total_clk}, CTR {avg_ctr}%"
    )

    buf = io.StringIO()
    writer = csv.writer(buf)

    writer.writerow(["Review", review])
    writer.writerow([])

    writer.writerow(["Date"] + dates)
    writer.writerow(["Impressions Trend"] + [spark_imp])
    writer.writerow(["Clicks Trend"]     + [spark_clk])
    writer.writerow([])

    writer.writerow(["Metric","Value"])
    writer.writerow(["Google Active",      len(g_rows)])
    writer.writerow(["Meta Active",        len(m_rows)])
    writer.writerow(["Total Campaigns",    len(rows)])
    writer.writerow(["Total Impressions",  total_imp])
    writer.writerow(["Total Clicks",       total_clk])
    writer.writerow(["Overall CTR (%)",    avg_ctr])
    writer.writerow([])

    writer.writerow(["Campaign ID","Network","Name","Status","Impressions","Clicks","CTR (%)"])
    for r in rows:
        network = "Google" if "spend" not in r else "Meta"
        writer.writerow([
            r["id"], network, r.get("name",""), r.get("status",""),
            r["impressions"], r["clicks"], r["ctr (%)"]
        ])

    data = buf.getvalue().encode("utf-8")
    return JSONResponse({
        "fileName": "combined_active_campaigns.csv",
        "mimeType": "text/csv",
        "bytes": list(data)
    })

if __name__ == "__main__":
    logging.info("Starting export service on port 8080")
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
