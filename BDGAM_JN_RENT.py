# -*- coding: utf-8 -*-
import os
import json
import time
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import pytz

start_time = time.time()

# ============ CONFIGURAÇÕES =============
SPREADSHEET_ID = "1n1WMWBkMtHA9SdQpveC8Ch7nUCCmBHnHySTqN-eY7PE"
SHEET_NAME = "BD - GAM"
API_URL = "https://my.spun.com.br/api/admanager/data"
API_TOKEN = "8jwl4v1ZmBYQlwFzPPEHNkYC8IOvRxB3ino1665b93f36cd228"

# Data de hoje em GMT-3 (Brasília)
fuso_br = pytz.timezone('America/Sao_Paulo')
today = datetime.now(fuso_br)
DATE_STRING = today.strftime('%Y-%m-%d')

DOMAINS = [
    {"domain": "thecredito.com.br", "networkCode": "21655197668", "currency": "BRL"},
    {"domain": "meucartaoideal.com", "networkCode": "21655197668", "currency": "BRL"},
    {"domain": "caxiason.com.br", "networkCode": "21655197668", "currency": "BRL"},
    {"domain": "guiabancario.com.br", "networkCode": "21655197668", "currency": "BRL"},
    {"domain": "agoranamidia.com", "networkCode": "21655197668", "currency": "BRL"},
    {"domain": "coinvistu.com", "networkCode": "23279186968", "currency": "USD"},
    {"domain": "creativepulse23.com", "networkCode": "23144189085", "currency": "USD"},
    {"domain": "genialcredito.com", "networkCode": "21938760094", "currency": "BRL"},
    {"domain": "usfinancemore.com", "networkCode": "23158280633", "currency": "BRL"},
    {"domain": "de8.com.br", "networkCode": "22705810042", "currency": "USD"},
    {"domain": "meucreditoagora.com", "networkCode": "21761578357", "currency": "BRL"},
    {"domain": "netdinheiro.com.br", "networkCode": "21629126805", "currency": "BRL"},
    {"domain": "rendademae.com", "networkCode": "22883124850", "currency": "USD"},
    {"domain": "zienic.com", "networkCode": "22407091784", "currency": "USD"}
]

# ============ FUNÇÕES AUXILIARES =============

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

def date_to_gsheet_serial(date_str):
    """Converte 'YYYY-MM-DD' para serial number do Google Sheets."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    base = datetime(1899, 12, 30)
    delta = dt - base
    return float(delta.days)

# ============ CONEXÃO GOOGLE SHEETS =============
creds_json = os.environ['GCP_CREDENTIALS']
google_creds = json.loads(creds_json)
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
credentials = Credentials.from_service_account_info(google_creds, scopes=scopes)
gc = gspread.authorize(credentials)
sheet = gc.open_by_key(SPREADSHEET_ID)

# ============ PEGAR COTAÇÃO DO DÓLAR =============
# ============ PEGAR COTAÇÃO DO DÓLAR =============
try:
    # Define a aba "info" onde o dólar está
    dashboard_ws = sheet.worksheet("info")

    # Pega o valor nas células B1:C1 (ajuste conforme a célula correta)
    values = dashboard_ws.get("B1:C1")  # retorna lista de listas

    if not values or not values[0]:
        raise ValueError("Células B1:C1 estão vazias")

    raw_val = values[0][-1]  # pega último valor da faixa
    # Remove R$, espaços e troca vírgula por ponto
    cleaned_val = str(raw_val).replace("R$", "").replace(" ", "").replace(",", ".")
    EXCHANGE_RATE = float(cleaned_val)
    print(f"💵 Câmbio obtido: {EXCHANGE_RATE}")

except Exception as e:
    print(f"⚠️ Erro ao pegar câmbio ({e}), fallback = 5.35")
    EXCHANGE_RATE = 5.35

# ============ PREPARAR ABA PRINCIPAL =============
try:
    worksheet = sheet.worksheet(SHEET_NAME)
    worksheet.clear()
except gspread.WorksheetNotFound:
    worksheet = sheet.add_worksheet(title=SHEET_NAME, rows="1000", cols="20")

# ============ CABEÇALHO =============
headers = ["Date", "Hora", "Site", "Channel Name", "URL", "Ad Unit", "Requests", "Revenue (USD)", "Cobertura", "eCPM"]
all_rows = []

# ============ BUSCAR DADOS DA API =============
for d in DOMAINS:
    payload = {
        "dimensions": ["DATE","HOUR","SITE_NAME","CHANNEL_NAME","URL_NAME","AD_UNIT_NAME"],
        "columns": ["AD_EXCHANGE_TOTAL_REQUESTS","AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE","AD_EXCHANGE_MATCH_RATE","AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM"],
        "start_date": DATE_STRING,
        "end_date": DATE_STRING,
        "domain": d["domain"],
        "networkCode": d["networkCode"],
        "site_name": "",
        "channel_name": "utm_source=email,utm_source=activecampaign,utm_source=spush,utm_source=pushalert"
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
        try:
            data_valor = row.get("Dimension.DATE", "")
            try:
                serial = date_to_gsheet_serial(data_valor)
            except Exception:
                serial = data_valor  # fallback se houver alguma data inválida

            revenue = safe_int(row.get("Column.AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE", 0)) / 1_000_000
            ecpm = safe_int(row.get("Column.AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM", 0)) / 1_000_000
            match_rate = safe_float(row.get("Column.AD_EXCHANGE_MATCH_RATE", 0))
            requests_val = safe_int(row.get("Column.AD_EXCHANGE_TOTAL_REQUESTS", 0))

            # Converter BRL → USD, se necessário
            if d["currency"] == "BRL" and EXCHANGE_RATE:
                revenue /= EXCHANGE_RATE
                ecpm /= EXCHANGE_RATE

            all_rows.append([
                serial,
                safe_int(row.get("Dimension.HOUR",0)),
                row.get("Dimension.SITE_NAME",""),
                row.get("Dimension.CHANNEL_NAME",""),
                row.get("Dimension.URL_NAME",""),
                row.get("Dimension.AD_UNIT_NAME",""),
                requests_val,
                round(revenue, 2),
                0 if match_rate == 0 else round(match_rate, 4),
                round(ecpm, 2)
            ])
        except Exception as e:
            print(f"⚠️ Erro processando linha: {e}")

# ============ ATUALIZAR PLANILHA =============
if all_rows:
    worksheet.update(values=[headers]+all_rows, range_name="A1")
    print(f"✅ Aba '{SHEET_NAME}' atualizada com {len(all_rows)} linhas.")
else:
    print("⚠️ Nenhuma linha retornada.")

# ============ FORMATAR COLUNA A COMO DATA yyyy-MM-dd =============
from googleapiclient.discovery import build

def format_col_A_as_date(spreadsheet_id, sheet_name, creds_json):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = Credentials.from_service_account_info(creds_json, scopes=scopes)
    service = build('sheets', 'v4', credentials=credentials)
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = None
    for s in metadata['sheets']:
        print(f"Sheet encontrado: '{s['properties']['title']}' id={s['properties']['sheetId']}")
        if s['properties']['title'] == sheet_name:
            sheet_id = s['properties']['sheetId']
    if sheet_id is None:
        print(f'❌ Não achou a aba "{sheet_name}"!')
        return
    body = {
        "requests": [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,  # pula cabeçalho; se quiser formatar o header também, use 0
                        "startColumnIndex": 0,
                        "endColumnIndex": 1
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
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()
    print("✅ Coluna A formatada como DATA yyyy-MM-dd.")

try:
    format_col_A_as_date(SPREADSHEET_ID, SHEET_NAME, google_creds)
except Exception as e:
    print(f"⚠️ Erro formatando coluna A como data: {e}")

# ============ FIM DO TIMER =============
elapsed_time = time.time() - start_time
minutes, seconds = divmod(elapsed_time, 60)
print(f"⏱ Tempo total: {int(minutes)}m {seconds:.2f}s")
