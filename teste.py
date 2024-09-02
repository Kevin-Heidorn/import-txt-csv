import csv
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import os

# Defina os caminhos absolutos
caminho = r'C:\Users\12723127838\Downloads\teste-texto'
arquivo_txt = os.path.join(caminho, 'dados.txt')

# Caminho para a pasta onde o CSV será salvo
pasta_csv = os.path.join(caminho, 'teste.csv')
# Nome do arquivo CSV dentro da pasta
arquivo_csv = os.path.join(pasta_csv, 'dados.csv')

nome_da_aba = 'PlanilhaAutomatizada'

# Função para converter TXT para CSV
def txt_para_csv(arquivo_txt, arquivo_csv):
    print(f"Iniciando a conversão de {arquivo_txt} para {arquivo_csv}...")
    try:
        # Verifica se a pasta destino existe, caso contrário, cria
        pasta_destino = os.path.dirname(arquivo_csv)
        if not os.path.exists(pasta_destino):
            os.makedirs(pasta_destino)
            print(f"Pasta criada: {pasta_destino}")
        
        with open(arquivo_txt, 'r') as file:
            linhas = file.readlines()
        print(f"{len(linhas)} linhas lidas do arquivo TXT.")

        with open(arquivo_csv, 'w', newline='') as file:
            escritor_csv = csv.writer(file)
            for linha in linhas:
                escritor_csv.writerow(linha.strip().split())
        print(f"Arquivo CSV salvo em: {arquivo_csv}")

    except FileNotFoundError:
        print(f"Arquivo não encontrado: {arquivo_txt}")
    except IOError as e:
        print(f"Erro ao ler/escrever o arquivo: {e}")

# Função para enviar o CSV para o Google Sheets
def enviar_para_google_sheets(arquivo_csv, nome_da_aba):
    print(f"Iniciando o envio do arquivo CSV para Google Sheets...")
    escopo = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    try:
        credenciais = ServiceAccountCredentials.from_json_keyfile_name('credenciais.json', escopo)
        cliente = gspread.authorize(credenciais)
        print("Autorização do cliente do Google Sheets bem-sucedida.")
    except Exception as e:
        print(f"Falha ao autorizar o cliente do Google Sheets: {e}")
        return

    try:
        try:
            planilha = cliente.create(nome_da_aba)
            print(f"Planilha criada: {nome_da_aba}")
        except gspread.exceptions.APIError:
            planilha = cliente.open(nome_da_aba)
            print(f"Planilha existente aberta: {nome_da_aba}")
    except gspread.exceptions.APIError as e:
        print(f"Falha ao acessar o Google Sheets: {e}")
        return

    aba = planilha.get_worksheet(0)

    try:
        dados = pd.read_csv(arquivo_csv)
        aba.update([dados.columns.values.tolist()] + dados.values.tolist())
        print("Dados atualizados no Google Sheets com sucesso.")
    except Exception as e:
        print(f"Falha ao atualizar o Google Sheets: {e}")

# Função principal para executar o processo periodicamente
def executar_periodicamente():
    print("Iniciando a execução periódica...")
    while True:
        # Etapa 1: Converter TXT para CSV
        txt_para_csv(arquivo_txt, arquivo_csv)

        # Etapa 2: Enviar CSV para Google Sheets
        enviar_para_google_sheets(arquivo_csv, nome_da_aba)

        # Aguarda 1 hora (3600 segundos)
        print("Aguardando 1 hora antes da próxima execução...")
        time.sleep(10)

if __name__ == "__main__":
    executar_periodicamente()
