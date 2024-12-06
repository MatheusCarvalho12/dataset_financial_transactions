import pandas as pd
import time
import psycopg2
from psycopg2.extras import execute_batch
from colorama import Fore, init
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Função para criar a conexão com o banco de dados PostgreSQL
def create_db_connection():
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    return conn

# Inicializando o colorama para garantir que funcione no Windows
init(autoreset=True)

# Função para inserir dados em massa na tabela merchants e transactions
def insert_data_in_bulk(conn, df, table_name):
    # Cria um cursor
    cursor = conn.cursor()

    if table_name == 'merchants':
        print(Fore.CYAN + "🚀 Iniciando inserção dos registros de merchants...")

        start_time = time.time()  # Início da medição de tempo
        # Preparando dados para inserção em massa na tabela de merchants
        merchants_data = []
        for _, row in (
            df[['merchant_id', 'merchant_city', 'merchant_state', 'zip', 'mcc']].drop_duplicates().iterrows()
        ):
            merchants_data.append((
                row['merchant_id'], 
                row['merchant_city'], 
                row['merchant_state'] if pd.notna(row['merchant_state']) and row['merchant_state'] != '' else None,  # Define NULL se vazio ou NaN
                row['zip'] if pd.notna(row['zip']) and row['zip'] != '' else None,  # Define NULL se vazio ou NaN
                row['mcc']
            ))

        if merchants_data:
            try:
                execute_batch(
                    cursor,
                    "INSERT INTO merchants (merchant_id, merchant_city, merchant_state, zip, mcc) "
                    "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (merchant_id) DO NOTHING", 
                    merchants_data,
                    page_size=10000  # Ajuste o tamanho do lote conforme necessário
                )
                print(Fore.GREEN + "✅ Todos os registros de merchants foram inseridos com sucesso.")
            except Exception as e:
                print(Fore.RED + f"❌ Erro ao inserir merchants: {e}")

        end_time = time.time()  # Fim da medição de tempo
        print(Fore.YELLOW + f"⏳ Tempo de inserção de merchants: {end_time - start_time:.2f} segundos.\n")

    elif table_name == 'transactions':
        print(Fore.CYAN + "🚀 Iniciando inserção dos registros de transactions...")

        start_time = time.time()  # Início da medição de tempo

        # Preparando dados para inserção em massa na tabela de transactions
        transactions_data = []

        # Obter os merchant_ids e seus ids correspondentes de uma vez
        cursor.execute("SELECT merchant_id, id FROM merchants")
        merchant_id_to_id = {row[0]: row[1] for row in cursor.fetchall()}

        for _, row in df.iterrows():  
            merchant_id = row['merchant_id']
            # Usar o id do merchant ao invés de uma subconsulta
            merchant_id_in_db = merchant_id_to_id.get(merchant_id, None)
            
            if merchant_id_in_db is None:
                print(Fore.RED + f"❌ Não foi encontrado um merchant_id válido para {merchant_id}")
                continue  # Pula para o próximo registro

            transactions_data.append((
                row['id'], 
                row['date'], 
                row['client_id'], 
                row['card_id'], 
                row['amount'],
                row['use_chip'], 
                merchant_id_in_db,
                row['errors'] if pd.notna(row['errors']) and row['errors'] != '' else None
            ))

        if transactions_data:
            try:
                execute_batch(
                    cursor,
                    "INSERT INTO transactions (id, date, client_id, card_id, amount, use_chip, merchant_id, errors) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", 
                    transactions_data,
                    page_size=10000  # Ajuste o tamanho do lote conforme necessário
                )
                print(Fore.GREEN + "✅ Todos os registros de transactions foram inseridos com sucesso.")
            except Exception as e:
                print(Fore.RED + f"❌ Erro ao inserir transactions: {e}")

        end_time = time.time()  # Fim da medição de tempo
        print(Fore.YELLOW + f"⏳ Tempo de inserção de transactions: {end_time - start_time:.2f} segundos.\n")
    
    # Commit das transações no banco
    conn.commit()

    # Fechar o cursor
    cursor.close()

# Função principal para carregar os dados e chamar as funções de inserção
def main():
    try:
        # Carregar o arquivo CSV de transações
        print(Fore.YELLOW + "📂 Carregando arquivo CSV de transactions...")
        df_transactions = pd.read_csv('transactions_data.csv')

        # Criar conexão com o banco de dados
        conn = create_db_connection()

        # Inserir dados na tabela merchants
        print(Fore.YELLOW + "🔄 Iniciando o processo de inserção de merchants...")
        insert_data_in_bulk(conn, df_transactions, 'merchants')

        # Inserir dados na tabela transactions
        print(Fore.YELLOW + "🔄 Iniciando o processo de inserção de transactions...")
        insert_data_in_bulk(conn, df_transactions, 'transactions')

        print(Fore.GREEN + "🎉 Processo de inserção concluído com sucesso!")

    except Exception as e:
        print(Fore.RED + f"❌ Erro no processo principal: {e}")

    finally:
        # Fechar a conexão com o banco
        if 'conn' in locals() and conn:
            conn.close()
            print(Fore.MAGENTA + "🔒 Conexão com o banco de dados fechada.")

# Chamar a função principal para rodar o script
if __name__ == '__main__':
    main()
