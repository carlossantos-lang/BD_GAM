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
SHEET_NAME = "BD - GAM"
API_URL = "https://my.spun.com.br/api/admanager/data"

# Secrets do GitHub Actions
API_TOKEN = os.environ.get("SPUN_API_TOKEN")
GCP_CREDENTIALS_JSON = os.environ.get("GCP_CREDENTIALS")

# Data atual em GMT-3 (Brasília)
fuso_br = pytz.timezone('America/Sao_Paulo')
today = datetime.now(fuso_br)
DATE_STRING = today.strftime('%Y-%m-%d')

# ============ DOMÍNIOS COM MOEDA ============
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
    {"domain": "mundodasfinancas.com.br", "networkCode": "22969181990", "currency": "USD"},
]

# ============ PLANILHAS COM FILTRO ============
PLANILHAS_DOMINIOS = [
    {"spreadsheet_id": "1Lh9snLOrHPFs6AynP5pfSmh3uos7ulEOiRNJKKqPs7s", "subdomain_filter": None},  
    {"spreadsheet_id": "1zPJAuoIp3hCEaRVubyiFrZq3KzRAgpfp06nRW2xCKrc",
     "subdomain_filter": ["www.caxiason.com.br", "en.rendademae.com", "zienic.com", "us.creativepulse23.com"]}  
]

# ============ FUNÇÕES AUXILIARES ============
def safe_float(v, default=0.0):
    try:
        return float(str(v).replace(",", "."))
    except:
        return default

def safe_int(v, default=0):
    try:
        return int(float(v))
    except:
        return default

def date_to_gsheet_serial(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    base = datetime(1899, 12, 30)
    delta = dt - base
    return float(delta.days)

# ============ CONEXÃO GOOGLE SHEETS ============
google_creds = json.loads(GCP_CREDENTIALS_JSON)
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
credentials = Credentials.from_service_account_info(google_creds, scopes=scopes)
gc = gspread.authorize(credentials)

# ============ PEGAR COTAÇÃO DO DÓLAR ============
def get_exchange_rate():
    try:
        sheet = gc.open_by_key(PLANILHAS_DOMINIOS[0]["spreadsheet_id"])
        ws = sheet.worksheet("JN_US_CC")
        rate = safe_float(ws.acell("O2").value, 5.35)
        print(f"💵 Taxa de câmbio obtida: 1 USD = {rate} BRL")
        return rate
    except:
        print("⚠️ Erro ao pegar câmbio. Usando fallback: 5.35 BRL")
        return 5.35

# ============ FORMATAÇÃO DE COLUNA ============
def format_col_A_as_date(spreadsheet_id):
    service = build('sheets', 'v4', credentials=credentials)
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = None
    for s in metadata['sheets']:
        if s['properties']['title'] == SHEET_NAME:
            sheet_id = s['properties']['sheetId']
            break
    if sheet_id is None:
        print(f'❌ Não achou a aba "{SHEET_NAME}"!')
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
def update_sheet(spreadsheet_id, all_rows):
    sheet = gc.open_by_key(spreadsheet_id)
    try:
        ws = sheet.worksheet(SHEET_NAME)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=SHEET_NAME, rows="30000", cols="20")

    headers = ["Date", "Hora", "Site", "Channel Name", "URL", "Ad Unit", "Requests", "Revenue (USD)", "Cobertura", "eCPM"]
    ws.update("A1:J1", [headers])

    if not all_rows:
        print(f"⚠️ Nenhuma linha para atualizar na planilha {spreadsheet_id}")
        return

    ws.update(f"A2:J{len(all_rows)+1}", all_rows)
    print(f"✅ Atualizadas {len(all_rows)} linhas na planilha {spreadsheet_id}")
    try:
        format_col_A_as_date(spreadsheet_id)
    except Exception as e:
        print(f"⚠️ Erro ao formatar a coluna A como data: {e}")

# ============ EXECUÇÃO ============
EXCHANGE_RATE = get_exchange_rate()
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
    headers_req = {"Authorization": API_TOKEN, "Content-Type": "application/json"}  # <--- sem Bearer

    try:
        resp = requests.post(API_URL, json=payload, headers=headers_req, timeout=120)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"❌ Erro no domínio {d['domain']}: {e}")
        continue

    for row in data:
        try:
            date_val = row.get("Dimension.DATE","")
            serial = date_to_gsheet_serial(date_val) if date_val else ""
            revenue = safe_int(row.get("Column.AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE",0)) / 1_000_000
            ecpm = safe_int(row.get("Column.AD_EXCHANGE_LINE_ITEM_LEVEL_AVERAGE_ECPM",0)) / 1_000_000
            match_rate = safe_float(row.get("Column.AD_EXCHANGE_MATCH_RATE",0))
            requests_val = safe_int(row.get("Column.AD_EXCHANGE_TOTAL_REQUESTS",0))

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
                round(revenue,2),
                0 if match_rate==0 else round(match_rate,4),
                round(ecpm,2)
            ])
        except Exception as e:
            print(f"⚠️ Erro processando linha: {e}")

# ============ ATUALIZAR PLANILHAS COM FILTRO DE SUBDOMÍNIO ============
for planilha in PLANILHAS_DOMINIOS:
    spreadsheet_id = planilha["spreadsheet_id"]
    subdomain_filter = planilha.get("subdomain_filter")

    if subdomain_filter:
        subdomain_filter_lower = [s.lower() for s in subdomain_filter]
        rows_to_write = [r for r in all_rows if r[2].lower() in subdomain_filter_lower]
    else:
        rows_to_write = all_rows

    print(f"🔹 Planilha {spreadsheet_id} receberá {len(rows_to_write)} linhas")
    update_sheet(spreadsheet_id, rows_to_write)

elapsed_time = time.time() - start_time
minutes, seconds = divmod(elapsed_time, 60)
print(f"⏱ Tempo total: {int(minutes)}m {seconds:.2f}s")
