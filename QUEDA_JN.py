# -*- coding: utf-8 -*-
import os
import json
import time
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import pytz

start_time = time.time()

# ============ CONFIGURAÇÕES ============
SPREADSHEET_ID = "1jCOT6cxUjMbXsbwt9Ak9Q12RILMw_t5qxwZkHosrgC8"  # troque pelo ID correto
SHEET_NAME = "BD - GAM"
API_URL = "https://my.spun.com.br/api/admanager/data"
API_TOKEN = "8jwl4v1ZmBYQlwFzPPEHNkYC8IOvRxB3ino1665b93f36cd228"

# ============ FUNÇÕES AUXILIARES ============
def safe_float(v, default=0.0):
    try:
        return float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return default

def safe_int(v, default=0):
    try:
        return int(float(str(v).replace(",", ".")))
    except (TypeError, ValueError):
        return default

# ============ DATA (últimos 7 dias incluindo hoje) ============
fuso_br = pytz.timezone("America/Sao_Paulo")
today = datetime.now(fuso_br)
inicio = today - timedelta(days=6)
DATE_START = inicio.strftime("%Y-%m-%d")
DATE_END = today.strftime("%Y-%m-%d")

# ============ DOMÍNIOS ============
DOMAINS = [
    {"domain": "de8.com.br", "networkCode": "22705810042", "currency": "USD"},
    {"domain": "creativepulse23.com", "networkCode": "23144189085", "currency": "USD"},
    {"domain": "caxiason.com.br", "networkCode": "21655197668", "currency": "BRL"},
    {"domain": "thecredito.com.br", "networkCode": "21655197668", "currency": "BRL"},
    {"domain": "genialcredito.com", "networkCode": "21938760094", "currency": "BRL"},
    {"domain": "netdinheiro.com.br", "networkCode": "21629126805", "currency": "BRL"},
]

# ============ CONEXÃO GOOGLE SHEETS ============
creds_json = os.environ["GCP_CREDENTIALS"]
google_creds = json.loads(creds_json)
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
credentials = Credentials.from_service_account_info(google_creds, scopes=scopes)
gc = gspread.authorize(credentials)
sheet = gc.open_by_key(SPREADSHEET_ID)

# Atualiza a data/hora de execução em I3 da aba "A - JN US CC - Total (G)"
try:
    dashboard_ws = sheet.worksheet("A - JN US CC - Total (G)")
    dashboard_ws.update_acell("I3", today.strftime("%Y-%m-%d %H:%M:%S"))
except Exception as e:
    print(f"⚠️ Erro ao atualizar célula I3: {e}")

# Taxa de câmbio (D6:E6)
try:
    values = dashboard_ws.get("D6:E6")  # retorna lista de listas
    if not values or not values[0]:
        raise ValueError("Células D6:E6 estão vazias")
    raw_val = values[0][-1]  # pega último valor da faixa (E6 se existir, senão D6)
    # Remove R$, espaços e troca vírgula por ponto
    cleaned_val = str(raw_val).replace("R$", "").replace(" ", "").replace(",", ".")
    EXCHANGE_RATE = float(cleaned_val)
except Exception as e:
    print(f"⚠️ Erro ao pegar câmbio ({e}), fallback = 5.35")
    EXCHANGE_RATE = 5.35

# ============ PREPARAR ABA PRINCIPAL ============
try:
    worksheet = sheet.worksheet(SHEET_NAME)
    worksheet.clear()
except gspread.WorksheetNotFound:
    worksheet = sheet.add_worksheet(title=SHEET_NAME, rows="1000", cols="20")

# Cabeçalho
headers = [
    "Site",
    "Data",
    "Hora",
    "Canal",
    "Receita (USD)",
    "País",
    "URL",
    "Bloco",
    "Solicitações",
    "Cliques",
]
all_rows = [headers]

# ============ BUSCAR DADOS ============
for d in DOMAINS:
    payload = {
        "dimensions": [
            "SITE_NAME",
            "DATE",
            "HOUR",
            "CHANNEL_NAME",
            "COUNTRY_NAME",
            "URL_NAME",
            "AD_UNIT_NAME",
        ],
        "columns": [
            "AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE",
            "AD_EXCHANGE_TOTAL_REQUESTS",
            "AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS",
        ],
        "start_date": DATE_START,
        "end_date": DATE_END,
        "domain": d["domain"],
        "networkCode": d["networkCode"],
        "site_name": "en.de8.com.br,us.creativepulse23.com,card.caxiason.com.br,us.netdinheiro.com.br,en.genialcredito.com,finance.genialcredito.com,finance.creativepulse23.com,emp.thecredito.com.br",
    }
    headers_req = {"Authorization": API_TOKEN, "Content-Type": "application/json"}

    try:
        resp = requests.post(API_URL, json=payload, headers=headers_req)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"❌ Erro no domínio {d['domain']}: {e}")
        continue

    if not isinstance(data, list):
        continue

    for row in data:
        site_name = row.get("Dimension.SITE_NAME", "")
        if site_name not in ["en.de8.com.br","us.creativepulse23.com","card.caxiason.com.br","us.netdinheiro.com.br","en.genialcredito.com","finance.genialcredito.com","finance.creativepulse23.com","emp.thecredito.com.br"]:
            continue  # filtra apenas os sites desejados

        channel = row.get("Dimension.CHANNEL_NAME", "")
        if not channel:
            continue

        if any(kw in channel for kw in ["utm_source=google", "utm_source=queda", "utm_medium="]):
            try:
                revenue = safe_float(row.get("Column.AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE", 0)) / 1_000_000
                if d["currency"] == "BRL":
                    revenue /= EXCHANGE_RATE

                # Converter datas e horas
                date_fmt = row.get("Dimension.DATE", "")
                hour_raw = row.get("Dimension.HOUR", 0)
                hour_fmt = str(int(hour_raw)) if hour_raw not in [None, ""] else "0"

                all_rows.append([
                    site_name,
                    date_fmt,
                    hour_fmt,
                    channel,
                    round(revenue, 2),
                    row.get("Dimension.COUNTRY_NAME", ""),
                    row.get("Dimension.URL_NAME", ""),
                    row.get("Dimension.AD_UNIT_NAME", ""),
                    safe_int(row.get("Column.AD_EXCHANGE_TOTAL_REQUESTS", 0)),
                    safe_int(row.get("Column.AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS", 0)),
                ])
            except Exception as e:
                print(f"⚠️ Erro processando linha do domínio {d['domain']}: {e}")

# ============ ATUALIZAR PLANILHA ============
if len(all_rows) > 1:
    worksheet.update(values=all_rows, range_name="A1", value_input_option="USER_ENTERED")
    print(f"✅ Aba '{SHEET_NAME}' atualizada com {len(all_rows)-1} linhas.")
else:
    print("⚠️ Nenhum dado retornado.")

# ============ FIM ============
elapsed_time = time.time() - start_time
minutes, seconds = divmod(elapsed_time, 60)
print(f"⏱ Tempo total: {int(minutes)}m {seconds:.2f}s")
