import pandas as pd

# Lê o CSV
df = pd.read_csv("data.csv")

# Remove os símbolos '$' e converte 'amount' para float
df['amount'] = df['amount'].replace('[\$,]', '', regex=True)

# Substitui valores vazios na coluna 'errors' por NULL
df['errors'] = df['errors'].replace('', None)

# Substitui valores vazios nas colunas 'merchant_city' e 'merchant_state' por NULL
df['merchant_city'] = df['merchant_city'].replace('', None)
df['merchant_state'] = df['merchant_state'].replace('', None)

# Remove o .0 dos códigos postais, se necessário
df['zip'] = df['zip'].apply(lambda x: str(x).replace('.0', '') if not pd.isna(x) else x)

# Salva o CSV ajustado
df.to_csv("transactions_data.csv", index=False)
