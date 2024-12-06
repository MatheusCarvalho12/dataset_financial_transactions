import pandas as pd

# Carregar o CSV
df = pd.read_csv('cards_data.csv')

# Função para converter datas para timestamp do PostgreSQL
def convert_to_timestamp(date_str):
    try:
        # Se a data contém o ano (somente 'YYYY')
        if len(date_str) == 4:  # ano no formato 'YYYY'
            return pd.to_datetime(f"01/01/{date_str}", format='%d/%m/%Y')
        
        # Se a data está no formato 'MM/YYYY', converte para 'YYYY-MM-01'
        return pd.to_datetime(f"01/{date_str}", format='%d/%m/%Y')
    except Exception as e:
        print(f"Erro ao converter data: {date_str} - {e}")
        return pd.NaT  # Retorna valor nulo em caso de erro

# Função para verificar se o card_number é um número inteiro
def check_card_number(card_number):
    try:
        # Verifica se o card_number é um inteiro
        int(card_number)
        return 'correct'
    except ValueError:
        return 'incorrect'

# Função para converter 'YES'/'NO' para True/False
def convert_to_boolean(value):
    return value == 'YES'

# Aplicar a função de conversão nas colunas de data
df['expires'] = df['expires'].apply(convert_to_timestamp)
df['acct_open_date'] = df['acct_open_date'].apply(convert_to_timestamp)  # Aplicar a mesma lógica para 'acct_open_date'

# Aplicar a verificação do card_number para definir o status
df['status'] = df['card_number'].apply(check_card_number)

# Converter as colunas 'card_on_dark_web' e 'has_chip' para valores booleanos
df['card_on_dark_web'] = df['card_on_dark_web'].apply(convert_to_boolean)
df['has_chip'] = df['has_chip'].apply(convert_to_boolean)

# Salvar o CSV com as datas convertidas
df.to_csv('cards_data_filtered.csv', index=False)
