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
SPREADSHEET_ID = "1DqC7bNOxljeZ5xze35p-PnsfmQSdZecZOtZp1FP-yh0"
SHEET_NAME = "BD - GAM"
API_URL = "https://my.spun.com.br/api/admanager/data"
API_TOKEN = "8jwl4v1ZmBYQlwFzPPEHNkYC8IOvRxB3ino1665b93f36cd228"

# Data (últimos 7 dias incluindo hoje)
fuso_br = pytz.timezone("America/Sao_Paulo")
today = datetime.now(fuso_br)
inicio = today - timedelta(days=6)

DATE_START = inicio.strftime("%Y-%m-%d")
DATE_END = today.strftime("%Y-%m-%d")

# Domínios
DOMAINS = [
    {"domain": "dissemedisse.com", "networkCode": "21962277692", "currency": "USD"},
    {"domain": "finantict.com", "networkCode": "12219877", "currency": "BRL"},
    {"domain": "oportalideal.com", "networkCode": "23312431047", "currency": "BRL"},
]

# ============ CONEXÃO GOOGLE SHEETS ============
creds_json = os.environ["GCP_CREDENTIALS"]
google_creds = json.loads(creds_json)
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
credentials = Credentials.from_service_account_info(google_creds, scopes=scopes)
gc = gspread.authorize(credentials)
sheet = gc.open_by_key(SPREADSHEET_ID)

# Atualiza a data/hora de execução em I3 da aba "A - GRID US CC - Total (G)"
try:
    dashboard_ws = sheet.worksheet("A - GRID US CC - Total (G)")
    dashboard_ws.update_acell("I3", today.strftime("%Y-%m-%d %H:%M:%S"))
except Exception as e:
    print(f"⚠️ Erro ao atualizar célula I3: {e}")

# Taxa de câmbio
try:
    dashboard_ws = sheet.worksheet("A - GRID US CC - Total (G)")
    EXCHANGE_RATE = float(str(dashboard_ws.acell("D6").value).replace(",", "."))
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
        "site_name": d["domain"],  # 👈 agora usa o domínio atual, não fixo
    }
    headers_req = {"Authorization": API_TOKEN, "Content-Type": "application/json"}

    try:
        resp = requests.post(API_URL, json=payload, headers=headers_req)
        resp.raise_for_status()
        data = resp.json()

        # Debug: mostrar como a API respondeu
        print(f"\n📡 Resposta do domínio {d['domain']} (primeiros 500 chars):")
        print(json.dumps(data, indent=2)[:500])

    except Exception as e:
        print(f"❌ Erro no domínio {d['domain']}: {e}")
        continue

    # Conferir se realmente veio lista
    if not isinstance(data, list):
        print(f"⚠️ Resposta não é lista para {d['domain']}: {type(data)}")
        continue

    for row in data:
        try:
            channel = row.get("Dimension.CHANNEL_NAME", "")

            revenue = float(row.get("Column.AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE", 0))
            # só dividir se o valor vier em micros
            if revenue > 1000:
                revenue /= 1_000_000

            if d["currency"] == "BRL":
                revenue /= EXCHANGE_RATE

            # datas e horas
            data_raw = row.get("Dimension.DATE", "")
            hora_raw = row.get("Dimension.HOUR", "0")

            data_fmt = (
                datetime.strptime(data_raw, "%Y-%m-%d").date() if data_raw else ""
            )
            hora_fmt = (
                datetime.strptime(hora_raw, "%H").time()
                if hora_raw.isdigit()
                else ""
            )

            all_rows.append(
                [
                    row.get("Dimension.SITE_NAME", ""),
                    data_fmt,
                    hora_fmt,
                    channel,
                    revenue,
                    row.get("Dimension.COUNTRY_NAME", ""),
                    row.get("Dimension.URL_NAME", ""),
                    row.get("Dimension.AD_UNIT_NAME", ""),
                    int(row.get("Column.AD_EXCHANGE_TOTAL_REQUESTS", 0)),
                    int(row.get("Column.AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS", 0)),
                ]
            )
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
