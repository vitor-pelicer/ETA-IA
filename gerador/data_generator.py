import psycopg2
from faker import Faker
import time
import random

# Configuração do banco de dados
DB_HOST = "localhost"
DB_NAME = "eta_fonte"
DB_USER = "postgres"
DB_PASSWORD = "postgres"

# Taxas de inserção (em segundos)
TAXA_INSERCAO_EMPRESA = 1
TAXA_INSERCAO_FICHA = 1

fator_empresa = 7
fator_ficha = 12

# Limite de inserções na tabela empresa
LIMITE_INSERCOES_EMPRESA = 200
LIMITE_INSERCOES_FICHA = 1000

# Faker para gerar dados falsos
fake = Faker('pt_BR')
fake.seed_instance(7)

def gerar_dados_empresa():
    return {
        'cnpj': fake.cnpj(),
        'nome': fake.company(),
        'tipo_empregador': random.choice(['Publica', 'Privada', 'Mista', 'ONG']),
        'verificado': random.choice([True, False]),
        'ativo': random.choice([True, False])
    }

def gerar_dados_ficha(cnpjs_validos):
    return {
        'rg': fake.rg(),
        'num_ordem': str(random.randint(1, 1000)),
        'num_sinan': str(random.randint(1, 1000)),
        'data_hora_atendimento': fake.date_time_between(start_date="-1y", end_date="now"),
        'unidade_notificante': random.randint(1, 10),
        'num_prontuario': str(random.randint(1, 1000)),
        'nome_acidentado': fake.name(),
        'data_nascimento': fake.date_of_birth(),
        'sexo': random.choice(['Masculino', 'Feminino', 'Outro']),
        'cor': random.randint(1, 5),
        'situacao_no_mercado': random.randint(1, 5),
        'escolaridade': random.randint(1, 8),
        'num_cartao_sus': fake.ssn(),
        'nome_mae': fake.name_female(),
        'endereco_residencial': random.randint(1, 100),
        'telefone': fake.phone_number(),
        'ocupacao': random.randint(1, 10),
        'empresa_empregadora': random.choice(cnpjs_validos) if cnpjs_validos else None,
        'data_hora_acidente': fake.date_time_between(start_date="-1y", end_date="now"),
        'local_acidente': random.randint(1, 5),
        'endereco_acidente': random.randint(1, 100),
        'empresa_acidente': random.randint(1, 10),
        'responsavel_preenchimento': random.randint(1, 5),
        'descricao_acidente': fake.text(),
        'maquina': random.randint(1, 5),
        'descricao_lesao': fake.text(),
        'diagnostico': fake.text(),
        'afastamento': random.choice(['Sim', 'Não']),
        'internacao': random.choice(['Sim', 'Não']),
        'duracao_tratamento': random.randint(1, 365),
        'obito': random.choice(['Sim', 'Não']),
        'tipo_acidente': random.randint(1, 10),
        'observacao': fake.text(),
        'medico': random.randint(1, 10),
        'sinan': random.choice(['Sim', 'Não']),
        'ativo': random.choice([True, False]),
        'possivel_duplicacao': random.choice([True, False]),
        'municipio_acidente': fake.city(),
        'data_obito': fake.date_of_birth() if random.choice([True, False]) else None,
        'dados_sinan': random.randint(1, 10)
    }

def inserir_dados(conn, data, tabela):
    """Insere dados em uma tabela."""
    colunas = ', '.join(data.keys())
    placeholders = ', '.join(['%s'] * len(data))
    sql = f"INSERT INTO {tabela} ({colunas}) VALUES ({placeholders})"
    with conn.cursor() as cur:
        cur.execute(sql, tuple(data.values()))
        conn.commit()

def atualizar_dados_empresa(conn, data):
    """Atualiza dados de uma empresa existente."""
    sql = """
        UPDATE empresa
        SET nome = %s,
            tipo_empregador = %s,
            verificado = %s,
            ativo = %s
        WHERE cnpj = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (data['nome'], data['tipo_empregador'], data['verificado'], data['ativo'], data['cnpj']))
        conn.commit()

def main():
    cnpjs_validos = []
    total_insercoes_empresa = 0
    total_insercoes_ficha = 0
    while True:
        try:
            conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)

            while True:
                # Inserir dados na tabela empresa até atingir o limite
                if total_insercoes_empresa < LIMITE_INSERCOES_EMPRESA:
                    if int(time.time()) % TAXA_INSERCAO_EMPRESA == 0:
                        for i in range(fator_empresa):
                            empresa_data = gerar_dados_empresa()
                            inserir_dados(conn, empresa_data, 'empresa')
                            cnpjs_validos.append(empresa_data['cnpj'])
                            total_insercoes_empresa += 1
                            print("Inserido em empresa:", empresa_data['cnpj'])
                else:
                    # Após o limite, apenas atualiza empresas existentes
                    if int(time.time()) % TAXA_INSERCAO_EMPRESA == 0:
                        for i in range(fator_empresa):
                            empresa_data = gerar_dados_empresa()
                            empresa_data['cnpj'] = random.choice(cnpjs_validos) # Seleciona um CNPJ existente
                            atualizar_dados_empresa(conn, empresa_data)
                            print("Atualizado em empresa:", empresa_data['cnpj'])

                # Inserir dados na tabela ficha
                if total_insercoes_ficha < LIMITE_INSERCOES_FICHA:
                    if int(time.time()) % TAXA_INSERCAO_FICHA == 0:
                        for i in range(fator_ficha):
                            ficha_data = gerar_dados_ficha(cnpjs_validos)
                            inserir_dados(conn, ficha_data, 'ficha')
                            total_insercoes_ficha += 1
                            print("Inserido em ficha")

                time.sleep(1)  # Aguarda 1 segundo
        except Exception as e:
            print(f"Erro: {e}")
        finally:
            if conn:
                conn.close()

if __name__ == "__main__":
    main()