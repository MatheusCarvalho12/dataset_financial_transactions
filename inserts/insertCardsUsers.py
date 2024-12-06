import pandas as pd
import psycopg2
from datetime import datetime
import time
from colorama import init, Fore
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Inicializando o colorama
init(autoreset=True)

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

# Função para converter o ano e o mês em TIMESTAMP
def convert_to_timestamp(year, month):
    try:
        # Assumindo o primeiro dia do mês como o dia do nascimento
        return datetime(year, month, 1)
    except Exception as e:
        print(Fore.RED + f"Erro ao converter data: {e}")
        return None

# Função para inserir dados em massa na tabela users
def insert_users(conn, df):
    cursor = conn.cursor()
    users_data = []
    
    start_time = time.time()  # Marca o tempo de início da inserção dos usuários
    
    for _, row in df.iterrows():
        birth_date = convert_to_timestamp(row['birth_year'], row['birth_month'])
        if birth_date:
            users_data.append((
                row['id'], row['retirement_age'], birth_date, row['gender'], row['address'],
                row['per_capita_income'], row['yearly_income'], row['total_debt'], row['credit_score'],
                row['latitude'], row['longitude']
            ))

    if users_data:
        try:
            cursor.executemany(
                "INSERT INTO users (id, retirement_age, birth, gender, address, per_capita_income, "
                "yearly_income, total_debt, credit_score, latitude, longitude) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
                users_data
            )
            conn.commit()
            end_time = time.time()  # Marca o tempo de término
            print(Fore.GREEN + f"Todos os registros de usuários foram inseridos com sucesso em {end_time - start_time:.2f} segundos.")
        except Exception as e:
            print(Fore.RED + f"Erro ao inserir usuários: {e}")
        finally:
            cursor.close()

# Função para inserir dados em massa na tabela cards
def insert_cards(conn, df):
    cursor = conn.cursor()
    cards_data = []

    start_time = time.time()  # Marca o tempo de início da inserção dos cartões

    for _, row in df.iterrows():
        try:
            # Verificar se o card_number é válido (somente números)
            card_number = row['card_number']
            if not str(card_number).isdigit():  # Verifica se contém apenas dígitos
                card_number = None
                card_status = 'incorrect'
            else:
                card_status = row['card_status']

            # Processar as outras datas
            expires_date = datetime.strptime(row['expires'], '%Y-%m-%d').date()
            acct_open_date = datetime.strptime(row['acct_open_date'], '%Y-%m-%d').date()

            cards_data.append((
                row['id'], row['client_id'], row['card_brand'], row['card_type'], card_number,
                expires_date, row['cvv'], row['has_chip'], row['num_cards_issued'], row['credit_limit'],
                acct_open_date, row['year_pin_last_changed'], row['card_on_dark_web'], card_status
            ))
        except Exception as e:
            print(Fore.RED + f"Erro ao processar cartão {row['id']}: {e}")

    if cards_data:
        try:
            cursor.executemany(
                "INSERT INTO cards (id, client_id, card_brand, card_type, card_number, expires, cvv, has_chip, "
                "num_cards_issued, credit_limit, acct_open_date, year_pin_last_changed, card_on_dark_web, "
                "card_status) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (card_number) DO NOTHING", 
                cards_data
            )
            conn.commit()
            end_time = time.time()  # Marca o tempo de término
            print(Fore.GREEN + f"Todos os registros de cartões foram inseridos com sucesso em {end_time - start_time:.2f} segundos.")
        except Exception as e:
            print(Fore.RED + f"Erro ao inserir cartões: {e}")
        finally:
            cursor.close()

# Função principal para carregar os dados e chamar as funções de inserção
def main():
    # Carregar os arquivos CSV de users e cards
    print(Fore.YELLOW + "Carregando arquivos CSV...")
    df_users = pd.read_csv('users_data.csv')
    df_cards = pd.read_csv('cards_data.csv')

    # Criar conexão com o banco de dados
    conn = create_db_connection()

    # Inserir dados na tabela users
    print(Fore.CYAN + "Iniciando inserção de usuários...")
    insert_users(conn, df_users)

    # Inserir dados na tabela cards
    print(Fore.CYAN + "Iniciando inserção de cartões...")
    insert_cards(conn, df_cards)

    # Fechar a conexão com o banco
    conn.close()
    print(Fore.MAGENTA + "Conexão com o banco fechada.")

# Chamar a função principal para rodar o script
if __name__ == '__main__':
    main()
