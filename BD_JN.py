# -*- coding: utf-8 -*-
import os
import json
import time
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import pytz
from googleapiclient.discovery import build

start_time = time.time()

# ============ CONFIGURAÇÕES ============
SPREADSHEET_IDS = [
    "1Lh9snLOrHPFs6AynP5pfSmh3uos7ulEOiRNJKKqPs7s",
    "1zPJAuoIp3hCEaRVubyiFrZq3KzRAgpfp06nRW2xCKrc",
    "1jjHJUu0im18BCxKUt6ZAS7FGFO3B7VQKq2S7q-01e-Q",
    "1XMVYlv1Iy5dDHiMMGRpcJ2neStF13rEeo0ou9rRw7aQ",
    "1lhDZGJJflyWCfYIEhNM1vho6QKPUynGSajjYxQzf8so",
    "1oSXRda1J_bOe06gcqf52frCX96xQ26fjwRZPmc50Eo8",
    "1AITsQmO0-Scs27hiXrSV1HFz8MtNYRZ89mBqHl58eio",
    "13sa5EwmMZa8wJKaCDf6APNYZOLGKGbhm9sgSUFSn25U",
    "1PBWDN0_zllMoaf0Mwg0BCDpKK27j374NX3Hqla8k1_E",
    "1Xs_6Sm8b6iAguZHJsMGiR5RmRlh0RoinWxy8h5-R9fE"
    
]
EXCHANGE_RATE_SHEET_ID = "1Lh9snLOrHPFs6AynP5pfSmh3uos7ulEOiRNJKKqPs7s"
SHEET_NAME = "BD - GAM"
API_URL = "https://my.spun.com.br/api/admanager/data"
API_TOKEN = "8jwl4v1ZmBYQlwFzPPEHNkYC8IOvRxB3ino1665b93f36cd228"

# Data atual em GMT-3 (Brasília)
fuso_br = pytz.timezone('America/Sao_Paulo')
today = datetime.now(fuso_br)
DATE_STRING = today.strftime('%Y-%m-%d')

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

# ============ FUNÇÕES AUXILIARES ============
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
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    base = datetime(1899, 12, 30)
    delta = dt - base
    return float(delta.days)

# ============ CONEXÃO GOOGLE SHEETS ============
creds_json = os.environ['GCP_CREDENTIALS']
google_creds = json.loads(creds_json)
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
credentials = Credentials.from_service_account_info(google_creds, scopes=scopes)
gc = gspread.authorize(credentials)

# ============ PEGAR COTAÇÃO DO DÓLAR ============
def get_exchange_rate():
    dollar_sheet_name = "JN_US_CC"
    dollar_cell = "O2"
    try:
        sheet = gc.open_by_key(EXCHANGE_RATE_SHEET_ID)
        dollar_ws = sheet.worksheet(dollar_sheet_name)
        rate = safe_float(dollar_ws.acell(dollar_cell).value, default=5.35)
        print(f"💵 Taxa de câmbio obtida: 1 USD = {rate} BRL")
        return rate
    except Exception as e:
        print(f"⚠️ Erro ao pegar câmbio ({e}). Usando fallback: 5.35 BRL")
        return 5.35

# ============ FORMATAÇÃO DE COLUNA ============
def format_col_A_as_date(spreadsheet_id, sheet_name, creds_json):
    credentials = Credentials.from_service_account_info(creds_json, scopes=scopes)
    service = build('sheets', 'v4', credentials=credentials)
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = None
    for s in metadata['sheets']:
        if s['properties']['title'] == sheet_name:
            sheet_id = s['properties']['sheetId']
            break
    if sheet_id is None:
        print(f'❌ Não achou a aba "{sheet_name}"!')
        return
    body = {
        "requests": [
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 1},
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "yyyy-MM-dd"}}},
                    "fields": "userEnteredFormat.numberFormat"
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    print("✅ Coluna A formatada como DATA yyyy-MM-dd.")

# ============ FUNÇÃO PARA ATUALIZAR PLANILHA EM CHUNKS ============
def update_sheet(spreadsheet_id, all_rows, chunk_size=10000):
    sheet = gc.open_by_key(spreadsheet_id)

    # Tenta abrir a worksheet, se não existir, cria já com muitas linhas.
    try:
        worksheet = sheet.worksheet(SHEET_NAME)
        worksheet.clear()
    except gspread.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=SHEET_NAME, rows="30000", cols="20")

    headers = ["Date", "Hora", "Site", "Channel Name", "URL", "Ad Unit", "Requests", "Revenue (USD)", "Cobertura", "eCPM"]
    worksheet.update(range_name="A1:J1", values=[headers])

    for i in range(0, len(all_rows), chunk_size):
        chunk = all_rows[i:i+chunk_size]
        start_row = i + 2
        end_row = start_row + len(chunk) - 1
        range_str = f"A{start_row}:J{end_row}"
        needed_rows = end_row

        # Adiciona linhas se necessário
        if worksheet.row_count < needed_rows:
            worksheet.add_rows(needed_rows - worksheet.row_count)

        worksheet.update(range_name=range_str, values=chunk)
        print(f"✅ Atualizadas linhas {start_row}-{end_row} na planilha {spreadsheet_id}")

    try:
        format_col_A_as_date(spreadsheet_id, SHEET_NAME, google_creds)
    except Exception as e:
        print(f"⚠️ Erro ao formatar a coluna A como data: {e}")

# ============ PEGAR COTAÇÃO SÓ UMA VEZ ============
EXCHANGE_RATE = get_exchange_rate()

# ============ BUSCAR DADOS DA API ============
all_rows = []
for d in DOMAINS:
    payload = {
        "dimensions": ["DATE","HOUR","SITE_NAME","CHANNEL_NAME","URL_NAME","AD_UNIT_NAME"],
        "columns": ["AD_EXCHANGE_TOTAL_REQUESTS","AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE","AD_EXCHANGE_MATCH_RATE","AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM"],
        "start_date": DATE_STRING,
        "end_date": DATE_STRING,
        "domain": d["domain"],
        "networkCode": d["networkCode"],
        "site_name": "",
        "channel_name": "utm_source=email,utm_source=activecampaign,utm_source=spush"
    }
    headers_req = {"Authorization": API_TOKEN, "Content-Type": "application/json"}

    try:
        resp = requests.post(API_URL, json=payload, headers=headers_req, timeout=120)
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
                serial = data_valor
            revenue = safe_int(row.get("Column.AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE", 0)) / 1_000_000
            ecpm = safe_int(row.get("Column.AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM", 0)) / 1_000_000
            match_rate = safe_float(row.get("Column.AD_EXCHANGE_MATCH_RATE", 0))
            requests_val = safe_int(row.get("Column.AD_EXCHANGE_TOTAL_REQUESTS", 0))

            # Converter BRL → USD, se necessário
            if d["currency"] == "BRL":
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

# ============ ATUALIZAR TODAS AS PLANILHAS ============
for sheet_id in SPREADSHEET_IDS:
    update_sheet(sheet_id, all_rows)

# ============ FIM DO TIMER ============
elapsed_time = time.time() - start_time
minutes, seconds = divmod(elapsed_time, 60)
print(f"⏱ Tempo total: {int(minutes)}m {seconds:.2f}s")
