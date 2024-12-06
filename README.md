# Dataset de Transações Financeiras

### **Scripts de Processamento e Inserção de Dados em PostgreSQL**

Este repositório contém scripts em Python para processamento, limpeza e inserção de dados em um banco de dados PostgreSQL. As funcionalidades incluem tratamento de dados de cartões, transações e usuários, além de inserção otimizada em tabelas relacionadas.

---

## Estrutura do Projeto

### Diretórios e Arquivos

- **filters/**  
  Scripts para limpeza e processamento dos dados antes da inserção no banco.
  - `filterCards.py`: Processa dados de cartões (datas, números de cartão, conversão para booleanos e status).
  - `filterTransactions.py`: Processa dados de transações (remover símbolos, tratar campos vazios, e ajustar formatos).

- **inserts/**  
  Scripts para inserção de dados no PostgreSQL.
  - `insertCardsUsers.py`: Insere dados de usuários e cartões.
  - `insertTransactions.py`: Insere dados de transações e comerciantes.

---

## Tecnologias Utilizadas

- **Linguagem**: Python 3.x  
- **Bibliotecas**:  
  - `pandas`: Manipulação e limpeza de dados.  
  - `psycopg2`: Conexão e operações no PostgreSQL.  
  - `colorama`: Logs no terminal com cores.  
  - `dotenv`: Gerenciamento de variáveis de ambiente.  

- **Banco de Dados**: PostgreSQL  
- **Arquivos de Dados**: `.csv`  
- **Gerenciamento de Variáveis de Ambiente**: `.env`  

---

## Configuração e Uso

### Pré-requisitos
1. Python 3.x instalado.
2. Banco de dados PostgreSQL configurado.
3. Arquivos de dados (`.csv`) no diretório do projeto.
4. Configuração do arquivo `.env` com as credenciais do banco:
   ```env
   DB_NAME=nome_do_banco
   DB_USER=usuario
   DB_PASSWORD=senha
   DB_HOST=localhost
   DB_PORT=5432
   ```

5. Instale as dependências necessárias via `pip`:
   ```bash
   pip install pandas psycopg2 colorama python-dotenv
   ```

### Passos para Uso

1. **Filtragem de Dados**:
   - Execute os scripts em `filters/` para limpar e processar os arquivos CSV.
     ```bash
     python filters/filterCards.py
     python filters/filterTransactions.py
     ```

2. **Inserção de Dados**:
   - Use os scripts em `inserts/` para carregar os dados no banco.
     ```bash
     python inserts/insertCardsUsers.py
     python inserts/insertTransactions.py
     ```

---

## Funcionalidades

### Filters
- **`filterCards.py`**:  
  - Converte datas no formato padrão para timestamp PostgreSQL.  
  - Valida números de cartão e define status (`correct` ou `incorrect`).  
  - Transforma valores `YES`/`NO` em booleanos (`True`/`False`).  

- **`filterTransactions.py`**:  
  - Remove símbolos monetários (`$`).  
  - Preenche campos vazios com `NULL`.  
  - Ajusta formatos para colunas específicas, como códigos postais.  

### Inserts
- **`insertCardsUsers.py`**:  
  - Insere dados de usuários e cartões, com tratamento de conflitos.  
  - Conversão de datas e validação de números de cartão.  

- **`insertTransactions.py`**:  
  - Insere comerciantes (merchants) e transações, garantindo integridade referencial.  
  - Uso de `execute_batch` para otimizar inserções em massa.  
---
## Criação e Configuração do Banco de Dados PostgreSQL

### Estrutura das Tabelas e Gatilhos

A seguir estão os comandos SQL para criar as tabelas e configurar gatilhos e funções para auditoria de alterações no banco de dados.

---

### **Tabelas do Banco**

#### 1. Tabela de Usuários

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    retirement_age INT NOT NULL,
    birth TIMESTAMP NOT NULL,
    gender VARCHAR(6) NOT NULL,  
    address TEXT NOT NULL,
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6),
    per_capita_income DECIMAL(10, 2) NOT NULL,  
    yearly_income INT NOT NULL,
    total_debt INT NOT NULL,
    credit_score INT NOT NULL
);
```

---

#### 2. Tabela de Cartões

```sql
CREATE TABLE cards (
    id SERIAL PRIMARY KEY,
    client_id INT REFERENCES users(id) ON DELETE CASCADE,
    card_brand VARCHAR(20) NOT NULL,
    card_type VARCHAR(20) NOT NULL,
    card_number BIGINT UNIQUE,
    expires DATE NOT NULL,
    cvv VARCHAR(3) NOT NULL,
    has_chip BOOLEAN NOT NULL,
    num_cards_issued INT NOT NULL,
    credit_limit INT NOT NULL,
    acct_open_date DATE NOT NULL,
    year_pin_last_changed INT,  
    card_on_dark_web BOOLEAN NOT NULL,
    card_status VARCHAR(20)
);
```

---

#### 3. Tabela de Comerciantes

```sql
CREATE TABLE merchants (
    id SERIAL PRIMARY KEY,
    merchant_id INT NOT NULL UNIQUE,
    merchant_city VARCHAR(100) NOT NULL,
    merchant_state VARCHAR(50),
    zip VARCHAR(10),
    mcc INT NOT NULL
);
```

---

#### 4. Tabela de Transações

```sql
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP NOT NULL,
    client_id INT REFERENCES users(id) ON DELETE CASCADE,
    card_id INT REFERENCES cards(id) ON DELETE CASCADE,
    amount DECIMAL(10, 2) NOT NULL,
    use_chip VARCHAR(30) NOT NULL,
    merchant_id INT REFERENCES merchants(id),
    errors TEXT
);
```

---

#### 5. Tabela de Logs de Auditoria

```sql
CREATE TABLE data_logs (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    operation_type VARCHAR(10) NOT NULL, -- 'INSERT', 'UPDATE', 'DELETE'
    record_id INT NOT NULL, -- ID do registro afetado
    old_data JSONB, -- Dados antes da alteração (para UPDATE e DELETE)
    new_data JSONB, -- Dados após a alteração (para INSERT e UPDATE)
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Data e hora da operação
    performed_by VARCHAR(100) -- Usuário que realizou a operação
);
```

---

### **Funções e Gatilhos de Log**

#### Função Genérica de Log

```sql
CREATE OR REPLACE FUNCTION log_data_changes() 
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO data_logs(
        table_name,
        operation_type,
        record_id,
        old_data,
        new_data,
        changed_at,
        performed_by
    )
    VALUES (
        TG_TABLE_NAME,
        TG_OP,
        COALESCE(NEW.id, OLD.id),
        CASE WHEN TG_OP = 'UPDATE' OR TG_OP = 'DELETE' THEN to_jsonb(OLD) ELSE NULL END,
        CASE WHEN TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN to_jsonb(NEW) ELSE NULL END,
        CURRENT_TIMESTAMP,
        current_user
    );

    RETURN NEW;  
END;
$$ LANGUAGE plpgsql;
```

---

#### Gatilhos nas Tabelas

**Gatilho na tabela `users`**:

```sql
CREATE TRIGGER trg_log_changes_users
AFTER INSERT OR UPDATE OR DELETE ON users
FOR EACH ROW
EXECUTE FUNCTION log_data_changes();
```

**Gatilho na tabela `cards`**:

```sql
CREATE TRIGGER trg_log_changes_cards
AFTER INSERT OR UPDATE OR DELETE ON cards
FOR EACH ROW
EXECUTE FUNCTION log_data_changes();
```

**Gatilho na tabela `transactions`**:

```sql
CREATE TRIGGER trg_log_changes_transactions
AFTER INSERT OR UPDATE OR DELETE ON transactions
FOR EACH ROW
EXECUTE FUNCTION log_data_changes();
```

**Gatilho na tabela `merchants`**:

```sql
CREATE TRIGGER trg_log_changes_merchants
AFTER INSERT OR UPDATE OR DELETE ON merchants
FOR EACH ROW
EXECUTE FUNCTION log_data_changes();
```

---

### **Considerações**

- As tabelas possuem chaves primárias (`id`) e relações referenciadas com **`ON DELETE CASCADE`** para garantir integridade ao remover registros.
- A tabela `data_logs` armazena um histórico completo das alterações, incluindo os dados antigos (`old_data`) e novos (`new_data`), facilitando auditorias e depurações.
- O gatilho `log_data_changes` é genérico, podendo ser reutilizado em várias tabelas.  
---
