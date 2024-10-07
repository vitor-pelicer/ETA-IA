

import psycopg2

# Informações de conexão com o banco de dados
DB_HOST = "localhost"
DB_NAME = "eta_fonte"
DB_USER = "postgres"
DB_PASS = "postgres"

try:
    # Conectar ao banco de dados
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    # Criar um cursor
    cur = conn.cursor()

    # Executar o comando DELETE com WHERE
    cur.execute("DELETE FROM cdc.cdc")
    conn.commit()

    cur.execute("DELETE FROM cdc.monitor")
    conn.commit()

    # Fechar o cursor e a conexão
    cur.close()
    conn.close()

    print("Registros deletados com sucesso.")

except (Exception, psycopg2.Error) as error:
    print("Erro ao deletar registros:", error)