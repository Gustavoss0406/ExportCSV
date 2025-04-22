import logging
import json
import io
from datetime import datetime, timedelta
from collections import defaultdict

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
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
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ───────── Helpers para Google/Meta ─────────
async def get_access_token(rt: str) -> str:
    creds = Credentials(token=None, refresh_token=rt,
                        token_uri="https://oauth2.googleapis.com/token",
                        client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    try:
        creds.refresh(GoogleRequest())
    except Exception as e:
        raise HTTPException(401, f"Google OAuth falhou: {e}")
    return creds.token

async def discover_customer_id(tok: str) -> str:
    url = f"https://googleads.googleapis.com/{API_VERSION}/customers:listAccessibleCustomers"
    h = {"Authorization":f"Bearer {tok}", "developer-token":DEVELOPER_TOKEN}
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, headers=h) as r:
            t = await r.text()
            if r.status!=200: raise HTTPException(502, t)
            names = json.loads(t).get("resourceNames",[])
            if not names: raise HTTPException(502, "Sem customers")
            return names[0].split("/")[-1]

async def google_ads_data(rt: str, days:int=7):
    token = await get_access_token(rt)
    cid   = await discover_customer_id(token)
    base  = f"https://googleads.googleapis.com/{API_VERSION}/customers/{cid}/googleAds:search"
    h     = {"Authorization":f"Bearer {token}", "developer-token":DEVELOPER_TOKEN}
    # campanhas
    q1 = """
      SELECT campaign.id,campaign.name,metrics.impressions,metrics.clicks
      FROM campaign WHERE campaign.status='ENABLED'
    """
    async with aiohttp.ClientSession() as sess:
        async with sess.post(base, headers=h, json={"query":q1}) as r:
            t = await r.text()
            if r.status!=200: raise HTTPException(r.status,t)
            res = json.loads(t).get("results",[])
    rows=[]
    for r in res:
        imp=int(r["metrics"]["impressions"]); clk=int(r["metrics"]["clicks"])
        rows.append({"campaign":r["campaign"]["id"],"impressions":imp,"clicks":clk})
    # trends
    q2 = f"""
      SELECT segments.date,metrics.impressions,metrics.clicks
      FROM campaign WHERE campaign.status='ENABLED'
        AND segments.date DURING LAST_{days}_DAYS
    """
    async with aiohttp.ClientSession() as sess:
        async with sess.post(base, headers=h, json={"query":q2}) as r:
            t = await r.text()
            if r.status!=200: raise HTTPException(r.status,t)
            res2=json.loads(t).get("results",[])
    by_date=defaultdict(lambda:{"impressions":0,"clicks":0})
    for r in res2:
        d=r["segments"]["date"]
        by_date[d]["impressions"]+=int(r["metrics"]["impressions"])
        by_date[d]["clicks"]+=int(r["metrics"]["clicks"])
    dates=sorted(by_date)
    return rows, dates, [by_date[d]["impressions"] for d in dates], [by_date[d]["clicks"] for d in dates]

async def meta_ads_data(rt:str, acct:str, days:int=7):
    # campanhas
    url_c=f"https://graph.facebook.com/v16.0/act_{acct}/campaigns"
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url_c, params={"access_token":rt,"fields":"id,name,status"}) as r:
            t=await r.text()
            if r.status!=200: raise HTTPException(r.status,t)
            data=json.loads(t)["data"]
    actives=[c["id"] for c in data if c["status"]=="ACTIVE"]
    # insights trends
    url_i=f"https://graph.facebook.com/v16.0/act_{acct}/insights"
    since=(datetime.now().date()-timedelta(days=days)).isoformat()
    p={"access_token":rt,
       "level":"campaign","fields":"campaign_id,impressions,clicks,spend,date_start",
       "time_range":json.dumps({"since":since,"until":since})}
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url_i, params=p) as r:
            t=await r.text()
            if r.status!=200: raise HTTPException(r.status,t)
            ins=json.loads(t)["data"]
    # carregar rows
    rows=[];mapi={i["campaign_id"]:i for i in ins}
    for c in data:
        if c["id"] in actives:
            m=mapi.get(c["id"],{})
            imp=int(m.get("impressions",0)); clk=int(m.get("clicks",0)); spd=float(m.get("spend",0))
            rows.append({"campaign":c["id"],"impressions":imp,"clicks":clk,"spend":spd})
    # trends
    by_date=defaultdict(lambda:{"impressions":0,"clicks":0})
    for i in ins:
        d=i["date_start"]
        by_date[d]["impressions"]+=int(i.get("impressions",0))
        by_date[d]["clicks"]+=int(i.get("clicks",0))
    dates=sorted(by_date)
    return rows, dates, [by_date[d]["impressions"] for d in dates], [by_date[d]["clicks"] for d in dates]

# ───────── API de dados combinados ─────────
@app.get("/api/combined_data")
async def combined_data(
    google_refresh_token: str = Query(...), 
    meta_account_id: str = Query(...),
    meta_access_token: str = Query(...),
):
    g_rows, g_dates, g_imps, g_clks = await google_ads_data(google_refresh_token)
    m_rows, m_dates, m_imps, m_clks = await meta_ads_data(meta_access_token, meta_account_id)
    rows = g_rows + m_rows
    total_imp = sum(r["impressions"] for r in rows)
    total_clk = sum(r["clicks"] for r in rows)
    total_spd = sum(r.get("spend",0) for r in rows)
    return {
        "metrics": {
            "active_campaigns": len(rows),
            "impressions": total_imp,
            "clicks": total_clk,
            "spend": total_spd,
            "ctr": round(total_clk/max(total_imp,1)*100,2),
            "cpc": round(total_spd/max(total_clk,1),2)
        },
        "trends": {
            "dates": g_dates, 
            "impressions": g_imps, 
            "clicks": g_clks
        }
    }

# ───────── Dashboard interativo ─────────
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request,
    google_refresh_token: str = Query(...), 
    meta_account_id: str = Query(...),
    meta_access_token: str = Query(...)
):
    # html inline para não criar arquivo externo
    return HTMLResponse(f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Ads Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
  <style>
    body {{ font-family: sans-serif; margin:20px; }}
    .card-container {{ display:flex; gap:10px; flex-wrap:wrap; }}
    .card {{
      flex:1 1 200px;
      padding:20px;
      border-radius:8px;
      background:#7F56D9;
      color:white;
      cursor:pointer;
      text-align:center;
      transition:transform .2s;
    }}
    .card:hover {{ transform:scale(1.05); }}
    .card h3 {{ margin:0 0 10px; font-size:1.1em; }}
    .card p {{ font-size:1.5em; margin:0; }}
    #chart {{ margin-top:30px; }}
  </style>
</head>
<body>
  <h1>Ads Dashboard</h1>
  <div class="card-container" id="cards">
    <!-- cards gerados por JS -->
  </div>
  <div id="chart"></div>
<script>
const params = new URLSearchParams(window.location.search);
const url = `/api/combined_data?${params.toString()}`;
fetch(url)
.then(r=>r.json())
.then(data=>{
  const m = data.metrics;
  const cards = document.getElementById('cards');
  // mapa de métricas para exibir
  const map = {{
    "active_campaigns":"Campanhas", "impressions":"Impressões",
    "clicks":"Cliques", "spend":"Gasto", "ctr":"CTR (%)","cpc":"CPC"
  }};
  Object.keys(map).forEach(key=>{{
    const card = document.createElement('div');
    card.className='card';
    card.dataset.metric=key;
    card.innerHTML=`<h3>${{map[key]}}</h3><p>${{m[key]}}</p>`;
    cards.appendChild(card);
  }});
  // evento click
  document.querySelectorAll('.card').forEach(c=>c.onclick=()=>plot(c.dataset.metric,data));
  // plota por default impressions
  plot('impressions',data);
}});
function plot(metric,data){{
  let y,x;
  if(metric==='impressions'||metric==='clicks'){{
    x=data.trends.dates; y=data.trends[metric];
  }} else {{
    // valores fixos em um ponto
    x=['Hoje']; y=[data.metrics[metric]];
  }}
  Plotly.newPlot('chart',[{{
    x:x, y:y,
    type:'scatter', mode:'lines+markers',
    marker:{{color:'#E839BC'}},
    line:{{color:'#7F56D9'}}
  }}],{{
    title:metric.toUpperCase(),
    plot_bgcolor:'#f9f9f9',
    paper_bgcolor:'white'
  }},{{
    responsive:true,
    transition:{{duration:500, easing:'cubic-in-out'}},
    frame:{{duration:500}}
  }});
}}
</script>
</body>
</html>
    """)

# seus endpoints XLSX continuam aqui...

if __name__ == "__main__":
    logging.info("Iniciando em 8080")
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
