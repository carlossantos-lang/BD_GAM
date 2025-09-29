# -*- coding: utf-8 -*-
import os
import json
import time
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import pytz

# ===== Função para número serial da data =====
def date_to_gsheet_serial(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    base = datetime(1899, 12, 30)
    delta = dt - base
    return float(delta.days)

# ===== Função para forçar formato da coluna =====
def set_gsheet_column_as_date(spreadsheet_id, sheet_name, col_index=0, credentials_json=None):
    from googleapiclient.discovery import build
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = Credentials.from_service_account_info(credentials_json, scopes=scopes)
    service = build('sheets', 'v4', credentials=credentials)
    # Descobre o ID da aba
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = None
    for s in metadata['sheets']:
        if s['properties']['title'] == sheet_name:
            sheet_id = s['properties']['sheetId']
            break
    if sheet_id is None:
        print(f"Erro ao localizar aba '{sheet_name}' para formatar coluna como data.")
        return
    # Requisição para formatar a coluna A como data yyyy-MM-dd
    body = {
        "requests": [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,  # pula o cabeçalho
                        "startColumnIndex": col_index,
                        "endColumnIndex": col_index + 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "DATE",
                                "pattern": "yyyy-MM-dd"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    print("✅ Coluna de data formatada para yyyy-MM-dd.")

# ===== Outros helpers =====
def safe_float(v, default=0.0):
    try:
        return float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return default

def safe_int(v, default=0):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default

# ========== INÍCIO DA EXECUÇÃO ==========
start_time = time.time()

# Configurações
SPREADSHEET_ID = "1Lh9snLOrHPFs6AynP5pfSmh3uos7ulEOiRNJKKqPs7s"
SHEET_NAME = "BD - GAM"
API_URL = "https://my.spun.com.br/api/admanager/data"
API_TOKEN = "8jwl4v1ZmBYQlwFzPPEHNkYC8IOvRxB3ino1665b93f36cd228"

# Data de hoje em GMT-3 (Brasília)
fuso_br = pytz.timezone('America/Sao_Paulo')
today = datetime.now(fuso_br)
DATE_STRING = today.strftime('%Y-%m-%d')
# Para ontem, descomente:
# today = today - timedelta(days=1)
# DATE_STRING = today.strftime('%Y-%m-%d')

DOMAINS = [
    {"domain": "financecaxias.com", "networkCode": "23148707119", "currency": "USD"},
    {"domain": "zienic.com", "networkCode": "22407091784", "currency": "USD"},
    {"domain": "de8.com.br", "networkCode": "22705810042", "currency": "USD"},
    {"domain": "rendademae.com", "networkCode": "22883124850", "currency": "USD"},
    {"domain": "creativepulse23.com", "networkCode": "23144189085", "currency": "USD"},
    {"domain": "agoranamidia.com", "networkCode": "21655197668", "currency": "BRL"},
    {"domain": "guiabancario.com.br", "networkCode": "21655197668", "currency": "BRL"},
    {"domain": "caxiason.com.br", "networkCode": "21655197668", "currency": "BRL"},
    {"domain": "meucartaoideal.com", "networkCode": "21655197668", "currency": "BRL"},
    {"domain": "thecredito.com.br", "networkCode": "21655197668", "currency": "BRL"},
    {"domain": "meucreditoagora.com", "networkCode": "21761578357", "currency": "BRL"},
    {"domain": "genialcredito.com", "networkCode": "21938760094", "currency": "BRL"},
    {"domain": "netdinheiro.com.br", "networkCode": "21629126805", "currency": "BRL"},
    {"domain": "usfinancemore.com", "networkCode": "23158280633", "currency": "BRL"},
    {"domain": "jobscaxias.com", "networkCode": "23158280633", "currency": "BRL"},
]

# ===== CREDENCIAIS E CONEXÃO SHEETS =====
creds_json = os.environ['GCP_CREDENTIALS']
google_creds = json.loads(creds_json)
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
credentials = Credentials.from_service_account_info(google_creds, scopes=scopes)
gc = gspread.authorize(credentials)
sheet = gc.open_by_key(SPREADSHEET_ID)

# ===== COTAÇÃO DO DÓLAR =====
dollar_sheet_name = "JN_US_CC"
dollar_cell = "O2"
try:
    dollar_ws = sheet.worksheet(dollar_sheet_name)
    EXCHANGE_RATE = safe_float(dollar_ws.acell(dollar_cell).value, default=5.35)
    print(f"💵 Taxa de câmbio obtida: 1 USD = {EXCHANGE_RATE} BRL")
except Exception as e:
    EXCHANGE_RATE = 5.35
    print(f"⚠️ Erro ao pegar câmbio ({e}). Usando fallback: {EXCHANGE_RATE} BRL")

# ===== PREPARA ABA PRINCIPAL =====
try:
    worksheet = sheet.worksheet(SHEET_NAME)
    worksheet.clear()
except gspread.WorksheetNotFound:
    worksheet = sheet.add_worksheet(title=SHEET_NAME, rows="1000", cols="20")

# ===== CABEÇALHO =====
headers = [
    "Date", "Hora", "Site", "Channel Name", "URL", "Ad Unit", "Requests",
    "Revenue (USD)", "Cobertura", "eCPM"
]
all_rows = []

# ===== BUSCA DADOS DA API =====
for d in DOMAINS:
    payload = {
        "dimensions": [
            "DATE", "HOUR", "SITE_NAME", "CHANNEL_NAME", "URL_NAME", "AD_UNIT_NAME"
        ],
        "columns": [
            "AD_EXCHANGE_TOTAL_REQUESTS",
            "AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE",
            "AD_EXCHANGE_MATCH_RATE",
            "AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM"
        ],
        "start_date": DATE_STRING,
        "end_date": DATE_STRING,
        "domain": d["domain"],
        "networkCode": d["networkCode"],
        "site_name": "",
        "channel_name": "utm_source=email,utm_source=activecampaign,utm_source=broadcast,utm_source=newsletter"
    }
    headers_req = {
        "Authorization": API_TOKEN,
        "Content-Type": "application/json"
    }

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
        try:
            data_valor = str(row.get("Dimension.DATE", "")).strip().strip("'").strip('"')
            data_convertida = date_to_gsheet_serial(data_valor)
            revenue = safe_int(row.get("Column.AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE", 0)) / 1_000_000
            ecpm = safe_int(row.get("Column.AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM", 0)) / 1_000_000
            match_rate = safe_float(row.get("Column.AD_EXCHANGE_MATCH_RATE", 0))
            requests_val = safe_int(row.get("Column.AD_EXCHANGE_TOTAL_REQUESTS", 0))
            if d["currency"] == "BRL" and EXCHANGE_RATE:
                revenue /= EXCHANGE_RATE
                ecpm /= EXCHANGE_RATE

            all_rows.append([
                data_convertida,
                safe_int(row.get("Dimension.HOUR", 0)),
                row.get("Dimension.SITE_NAME", ""),
                row.get("Dimension.CHANNEL_NAME", ""),
                row.get("Dimension.URL_NAME", ""),
                row.get("Dimension.AD_UNIT_NAME", ""),
                requests_val,
                round(revenue, 2),
                0 if match_rate == 0 else round(match_rate, 4),
                round(ecpm, 2)
            ])
        except Exception as e:
            print(f"⚠️ Erro processando linha: {e}")

# ===== ATUALIZA PLANILHA =====
if all_rows:
    worksheet.update(values=[headers] + all_rows, range_name="A1")
    print(f"✅ Aba '{SHEET_NAME}' atualizada com {len(all_rows)} linhas.")
else:
    print("⚠️ Nenhuma linha retornada.")

# ===== FORMATA COLUNA DE DATA COMO "yyyy-MM-dd" =====
set_gsheet_column_as_date(
    SPREADSHEET_ID, SHEET_NAME, col_index=0, credentials_json=google_creds
)

# ===== FIM DO TIMER =====
elapsed_time = time.time() - start_time
minutes, seconds = divmod(elapsed_time, 60)
print(f"⏱ Tempo total: {int(minutes)}m {seconds:.2f}s")
