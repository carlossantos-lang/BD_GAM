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
API_URL = "https://my.spun.com.br/api/admanager/data"
API_TOKEN = os.environ.get("SPUN_API_TOKEN")  # Token da API via variável de ambiente
GCP_CREDS_JSON = os.environ.get("GCP_CREDENTIALS")  # JSON de credenciais do GCP via variável de ambiente

SHEET_NAME = "BD - GAM"

# Domínios com moeda
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
]

# Planilhas com filtro de subdomínio (sem filtro opcional)
PLANILHAS_DOMINIOS = [
    {"spreadsheet_id": "1Lh9snLOrHPFs6AynP5pfSmh3uos7ulEOiRNJKKqPs7s"},  # sem filtro, pega tudo
    {"spreadsheet_id": "1zPJAuoIp3hCEaRVubyiFrZq3KzRAgpfp06nRW2xCKrc",
     "domain": "creativepulse23.com",
     "subdomain_filter": "www.caxiason.com.br","en.rendademae.com","zienic.com","us.creativepulse23.com"}
]

EXCHANGE_RATE_SHEET_ID = "1Lh9snLOrHPFs6AynP5pfSmh3uos7ulEOiRNJKKqPs7s"
EXCHANGE_RATE_SHEET_NAME = "JN_US_CC"
EXCHANGE_RATE_CELL = "O2"

# Data atual em GMT-3 (Brasília)
fuso_br = pytz.timezone('America/Sao_Paulo')
today = datetime.now(fuso_br)
DATE_STRING = today.strftime('%Y-%m-%d')

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
google_creds = json.loads(GCP_CREDS_JSON)
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
credentials = Credentials.from_service_account_info(google_creds, scopes=scopes)
gc = gspread.authorize(credentials)

# ============ PEGAR COTAÇÃO DO DÓLAR ============
def get_exchange_rate():
    try:
        sheet = gc.open_by_key(EXCHANGE_RATE_SHEET_ID)
        dollar_ws = sheet.worksheet(EXCHANGE_RATE_SHEET_NAME)
        rate = safe_float(dollar_ws.acell(EXCHANGE_RATE_CELL).value, default=5.35)
        print(f"💵 Taxa de câmbio obtida: 1 USD = {rate} BRL")
        return rate
    except Exception as e:
        print(f"⚠️ Erro ao pegar câmbio ({e}). Usando fallback: 5.35 BRL")
        return 5.35

EXCHANGE_RATE = get_exchange_rate()

# ============ FORMATAÇÃO DE COLUNA ============
def format_col_A_as_date(spreadsheet_id, sheet_name, creds_json):
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

# ============ FUNÇÃO PARA ATUALIZAR PLANILHA ============
def update_sheet(spreadsheet_id, all_rows, sheet_name=SHEET_NAME, chunk_size=10000):
    sheet = gc.open_by_key(spreadsheet_id)

    try:
        worksheet = sheet.worksheet(sheet_name)
        worksheet.clear()
    except gspread.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=sheet_name, rows="3000", cols="20")

    headers = ["Date", "Hora", "Site", "Channel Name", "URL", "Ad Unit", "Requests", "Revenue (USD)", "Cobertura", "eCPM"]
    worksheet.update(values=[headers], range_name="A1:J1")

    for i in range(0, len(all_rows), chunk_size):
        chunk = all_rows[i:i+chunk_size]
        start_row = i + 2
        end_row = start_row + len(chunk) - 1
        range_str = f"A{start_row}:J{end_row}"

        if worksheet.row_count < end_row:
            worksheet.add_rows(end_row - worksheet.row_count)

        worksheet.update(values=chunk, range_name=range_str)
        print(f"✅ Atualizadas linhas {start_row}-{end_row} na planilha {spreadsheet_id}")

    try:
        format_col_A_as_date(spreadsheet_id, sheet_name, google_creds)
    except Exception as e:
        print(f"⚠️ Erro ao formatar a coluna A como data: {e}")

# ============ BUSCAR DADOS DA API ============
all_rows = []
for d in DOMAINS:
    payload = {
        "dimensions": ["DATE","HOUR","SITE_NAME","CHANNEL_NAME","URL_NAME","AD_UNIT_NAME"],
        "columns": ["AD_EXCHANGE_TOTAL_REQUESTS","AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE",
                    "AD_EXCHANGE_MATCH_RATE","AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM"],
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
            serial = date_to_gsheet_serial(data_valor) if data_valor else data_valor

            revenue = safe_int(row.get("Column.AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE", 0)) / 1_000_000
            ecpm = safe_int(row.get("Column.AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM", 0)) / 1_000_000
            match_rate = safe_float(row.get("Column.AD_EXCHANGE_MATCH_RATE", 0))
            requests_val = safe_int(row.get("Column.AD_EXCHANGE_TOTAL_REQUESTS", 0))

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

print(f"📊 Total de linhas processadas da API: {len(all_rows)}")

# ============ FILTRAR POR SUBDOMÍNIO E ATUALIZAR PLANILHAS ============
for plan in PLANILHAS_DOMINIOS:
    if "domain" in plan and "subdomain_filter" in plan:
        subdomains = [s.strip().lower() for s in plan["subdomain_filter"].split(",")]
        filtered_rows = [
            row for row in all_rows
            if row[2].lower() == plan["domain"].lower() and any(sd in row[4].lower() for sd in subdomains)
        ]
    else:
        filtered_rows = all_rows  # sem filtro, pega tudo

    print(f"🔹 Planilha {plan['spreadsheet_id']} receberá {len(filtered_rows)} linhas")
    update_sheet(plan["spreadsheet_id"], filtered_rows)

# ============ FIM DO TIMER ============
elapsed_time = time.time() - start_time
minutes, seconds = divmod(elapsed_time, 60)
print(f"⏱ Tempo total: {int(minutes)}m {seconds:.2f}s")
