import csv
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import os
import re

# Caminhos absolutos
caminho = r'C:\Users\kevin\Downloads\Teste'
arquivo_txt = os.path.join(caminho, 'txitens.txt')

# Caminho para o arquivo CSV
arquivo_csv = os.path.join(caminho, 'dados.csv')
novo_arquivo_csv = os.path.join(caminho, 'dados_processados.csv')

# Caminho completo para o arquivo de credenciais
caminho_credenciais = os.path.join(caminho, 'credenciais.json')

nome_da_aba = 'Planilha teste'

# Função para converter TXT para CSV sem separação ou modificação
def txt_para_csv(arquivo_txt, arquivo_csv):
    print(f"Iniciando a conversão de {arquivo_txt} para {arquivo_csv}...")
    try:
        pasta_destino = os.path.dirname(arquivo_csv)
        if not os.path.exists(pasta_destino):
            os.makedirs(pasta_destino)
            print(f"Pasta criada: {pasta_destino}")
        
        with open(arquivo_txt, 'r', encoding='ISO-8859-1') as file:  # Força a leitura com codificação ISO-8859-1
            linhas = file.readlines()
        print(f"{len(linhas)} linhas lidas do arquivo TXT.")
        
        if not linhas:
            raise ValueError("Nenhuma linha foi lida do arquivo TXT.")

        with open(arquivo_csv, 'w', newline='', encoding='ISO-8859-1') as file:  # Força a escrita com codificação ISO-8859-1
            escritor_csv = csv.writer(file)
            for linha in linhas:
                escritor_csv.writerow([linha.strip()])
        print(f"Arquivo CSV salvo em: {arquivo_csv}")

    except FileNotFoundError:
        print(f"Arquivo não encontrado: {arquivo_txt}")
    except IOError as e:
        print(f"Erro ao ler/escrever o arquivo: {e}")
    except Exception as e:
        print(f"Erro desconhecido: {e}")

# Função para processar o CSV e dividir em três colunas
def processar_csv(arquivo_csv, novo_arquivo_csv):
    print(f"Iniciando o processamento do arquivo CSV: {arquivo_csv}...")
    try:
        with open(arquivo_csv, 'r', encoding='ISO-8859-1') as file:  # Força a leitura com codificação ISO-8859-1
            leitor_csv = csv.reader(file)
            linhas = [linha[0] for linha in leitor_csv]  # Lê todas as linhas do CSV

        print(f"{len(linhas)} linhas lidas do arquivo CSV.")

        with open(novo_arquivo_csv, 'w', newline='', encoding='ISO-8859-1') as file:  # Força a escrita com codificação ISO-8859-1
            escritor_csv = csv.writer(file)
            escritor_csv.writerow(['Código', 'Preço', 'Nome'])  # Cabeçalhos das colunas

            for linha in linhas:
                if len(linha) > 7:
                    restante = linha[7:]  # Remove os primeiros 7 caracteres
                    codigo = restante[:4]  # Primeiro 4 caracteres como código
                    preco = restante[4:10]  # Próximos 6 caracteres como preço
                    
                    # Remove o texto "REVISADO" e a data no final da string
                    restante = re.sub(r'\s+REVISADO \d{2}/\d{2}/\d{4}$', '', restante).strip()

                    nome = restante[10:]  # O restante como nome
                    
                    # Remove os primeiros 3 caracteres do nome
                    nome = nome[3:].strip()

                    escritor_csv.writerow([codigo, preco, nome])

        print(f"Arquivo CSV processado e salvo em: {novo_arquivo_csv}")

    except FileNotFoundError:
        print(f"Arquivo não encontrado: {arquivo_csv}")
    except IOError as e:
        print(f"Erro ao ler/escrever o arquivo: {e}")
    except Exception as e:
        print(f"Erro desconhecido: {e}")

# Função para enviar o CSV para o Google Sheets
def enviar_para_google_sheets(arquivo_csv, nome_da_aba):
    print(f"Iniciando o envio do arquivo CSV para Google Sheets...")
    escopo = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    try:
        credenciais = ServiceAccountCredentials.from_json_keyfile_name(caminho_credenciais, escopo)
        cliente = gspread.authorize(credenciais)
        print("Autorização do cliente do Google Sheets bem-sucedida.")
    except Exception as e:
        print(f"Falha ao autorizar o cliente do Google Sheets: {e}")
        return

    try:
        planilha = cliente.open(nome_da_aba)
        print(f"Planilha existente aberta: {nome_da_aba}")
    except gspread.exceptions.APIError as e:
        print(f"Falha ao acessar o Google Sheets: {e}")
        return

    aba = planilha.get_worksheet(0)

    try:
        dados = pd.read_csv(arquivo_csv, encoding='ISO-8859-1')  # Força a leitura com codificação ISO-8859-1
        dados_list = [dados.columns.values.tolist()] + dados.values.tolist()
        aba.clear()  # Limpa a aba antes de atualizar
        aba.update('A1', dados_list)  # Atualiza os dados a partir da célula A1
        print("Dados atualizados no Google Sheets com sucesso.")
    except Exception as e:
        print(f"Falha ao atualizar o Google Sheets: {e}")

# Função principal para executar o processo periodicamente
def executar_periodicamente():
    print("Iniciando a execução periódica...")
    while True:
        # Etapa 1: Converter TXT para CSV
        txt_para_csv(arquivo_txt, arquivo_csv)

        # Etapa 2: Processar o CSV para dividir em 3 colunas
        processar_csv(arquivo_csv, novo_arquivo_csv)

        # Etapa 3: Enviar o CSV processado para Google Sheets
        enviar_para_google_sheets(novo_arquivo_csv, nome_da_aba)

        # Aguarda 1 hora (3600 segundos)
        print("Aguardando 1 hora antes da próxima execução...")
        time.sleep(3600)

if __name__ == "__main__":
    executar_periodicamente()
