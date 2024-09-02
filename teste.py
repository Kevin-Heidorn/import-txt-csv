import csv
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# Função para converter TXT para CSV
def txt_para_csv(arquivo_txt, arquivo_csv):
    with open(arquivo_txt, 'r') as file:
        linhas = file.readlines()

    with open(arquivo_csv, 'w', newline='') as file:
        escritor_csv = csv.writer(file)
        
        for linha in linhas:
            # Presumindo que os dados no arquivo txt são separados por espaços ou tabs
            escritor_csv.writerow(linha.strip().split())

# Função para enviar o CSV para o Google Sheets
def enviar_para_google_sheets(arquivo_csv, nome_da_aba):
    # Configura as credenciais e a conexão com o Google Sheets
    escopo = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credenciais = ServiceAccountCredentials.from_json_keyfile_name('credenciais.json', escopo)
    cliente = gspread.authorize(credenciais)

    try:
        planilha = cliente.create(nome_da_aba)
    except gspread.exceptions.APIError:
        planilha = cliente.open(nome_da_aba)

    aba = planilha.get_worksheet(0)

    # Lê o CSV e atualiza a planilha
    dados = pd.read_csv(arquivo_csv)
    aba.update([dados.columns.values.tolist()] + dados.values.tolist())

# Função principal para executar o processo periodicamente
def executar_periodicamente():
    arquivo_txt = 'dados.txt'
    arquivo_csv = 'dados.csv'
    nome_da_aba = 'PlanilhaAutomatizada'

    while True:
        # Etapa 1: Converter TXT para CSV
        txt_para_csv(arquivo_txt, arquivo_csv)

        # Etapa 2: Enviar CSV para Google Sheets
        enviar_para_google_sheets(arquivo_csv, nome_da_aba)

        # Aguarda 1 hora (3600 segundos)
        time.sleep(3600)

if __name__ == "__main__":
    executar_periodicamente()
