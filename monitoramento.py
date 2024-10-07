import pandas as pd
import streamlit as st
import psycopg2
import threading
import time
import queue
from streamlit.runtime.scriptrunner import add_script_run_ctx
from datetime import datetime
import plotly.express as px


def consultar_relevancia(host, porta, usuario, senha, database, tabela):
  fk_query = f"""
    SELECT  
        tc.table_name, 
        kcu.column_name AS foreign_key_column, 
        ccu.table_name AS referenced_table_name,
        ccu.column_name AS referenced_column_name
    FROM 
        information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
    WHERE 
        constraint_type = 'FOREIGN KEY' 
        AND ccu.table_name = '{tabela}'
    ORDER BY 
        tc.table_name, 
        kcu.column_name;"""
  
  id_query = f"""
    SELECT DISTINCT id_tupla
    FROM cdc.cdc
    WHERE tabela='{tabela}'"""

  referencias_tabela = 0
  total_tabela = 0

  try:
    conn = psycopg2.connect(
            host=host,
            port=porta,
            database=database,
            user=usuario,
            password=senha
        )
    
    cursor = conn.cursor()
    cursor.execute(id_query)
    ids = cursor.fetchall()

    # Busca as tabelas com chaves estrageiras que referenciam a tabela buscada
    # nome da tabela que referencia | coluna FK | tabela buscada | coluna PK
    cursor.execute(fk_query)
    tables = cursor.fetchall()

    if len(ids) > 0:
      for table in tables:

        coluna_fk = table[1]
        tabela_ref = table[0]


        ref_query = f"""
        SELECT COUNT({coluna_fk})
        FROM {tabela_ref}
        WHERE {coluna_fk} IN ({", ".join(map(lambda x: f"'{str(x[0])}'", ids))})
        """

        total_query = f"""
        SELECT COUNT(*)
        FROM {tabela_ref}
        """

        # executa a query que conta a quantidade de referencias que o ID tem
        cursor.execute(ref_query, (ids,))
        referencias_tabela += cursor.fetchall()[0][0]

        cursor.execute(total_query)
        total_tabela += cursor.fetchall()[0][0]
    else:
       return 0

  except (Exception, psycopg2.Error) as error:
      print(f"Erro ao conectar-se com o banco de dados: {error}")
      conn.close()
      return 0
  finally:
    conn.close()
    if total_tabela != 0:
      return referencias_tabela/total_tabela
    else:
      return 0


def consultar_volume(host, porta, usuario, senha, database, tabela):
  query_total = f"""
  SELECT COUNT(*)
  FROM cdc.cdc
  WHERE tabela = '{tabela}'
  """
  
  try:
    conn = psycopg2.connect(
            host=host,
            port=porta,
            database=database,
            user=usuario,
            password=senha
        )
    cursor = conn.cursor()
    cursor.execute(query_total)
    total = cursor.fetchall()[0][0]
  except (Exception, psycopg2.Error) as error:
    print(f"Erro ao conectar-se com o banco de dados: {error}")
    conn.close()
    return 0
  finally:
    conn.close()
    return total
  

class MonitorBancoDeDados(threading.Thread):
  def __init__(self, fonte, dw, tabela, pk, tempo_espera, relevancia_minima, volume_minimo):
    super().__init__()
    self.fonte_host = fonte.get("ip")
    self.fonte_porta = fonte.get("porta")
    self.fonte_usuario = fonte.get("usuario")
    self.fonte_senha = fonte.get("senha")
    self.fonte_database = fonte.get("db")
    self.dw_host = dw.get("ip")
    self.dw_porta = dw.get("porta")
    self.dw_usuario = dw.get("usuario")
    self.dw_senha = dw.get("senha")
    self.dw_database = dw.get("db")
    self.tabela = tabela
    self.pk = pk
    self.tempo_espera = tempo_espera
    self.relevancia_minima = relevancia_minima
    self.volume_minimo = volume_minimo
    self._executando = True  # Sinaliza se o thread deve continuar executando
    add_script_run_ctx(self)

  def run(self):
    print("iniciando thread")


    
    count = 0
    ativou = False
    while self._executando:

      # Calcula a relevância e o volume usando as funções fornecidas
      relevancia_atual = consultar_relevancia(self.fonte_host, self.fonte_porta, self.fonte_usuario, self.fonte_senha, self.fonte_database, self.tabela)
      volume_atual = consultar_volume(self.fonte_host, self.fonte_porta, self.fonte_usuario, self.fonte_senha, self.fonte_database, self.tabela)

      print(f"Processando: R: {relevancia_atual} V:{volume_atual}")

      cdc_tabela = []
      try:
        conn = psycopg2.connect(
                host=self.fonte_host,
                port=self.fonte_porta,
                database=self.fonte_database,
                user=self.fonte_usuario,
                password=self.fonte_senha
            )
        cursor = conn.cursor()
        if relevancia_atual >= self.relevancia_minima or volume_atual >= self.volume_minimo:
          ativou = True
          # busca todos os registros no CDC
          query_cdc = f"""
          SELECT id, instancia, tabela, operacao, id_tupla, dados_novos, timestamp
          FROM cdc.cdc
          WHERE tabela = '{self.tabela}'
          ORDER BY timestamp ASC
          """
          cursor.execute(query_cdc)
          cdc_tabela = cursor.fetchall()

        else:
          ativou = False
        monitor_query = """
        INSERT INTO cdc.monitor(
          relevancia, volume, ativou, delta_r, delta_v, tabela, seq, tempo)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW());
        """
        cursor.execute(monitor_query, (relevancia_atual, volume_atual, ativou, 
                                       self.relevancia_minima, self.volume_minimo, 
                                       self.tabela, count))
        conn.commit()
      except (Exception, psycopg2.Error) as error:
        print(f"Erro ao conectar-se com o banco de dados: {error}")
        conn.close()
      finally:
        conn.close()

      ls_ids = []
      # para cada linha na tabela de CDC, faz a integração com o DW
      # ajusta a query para cada tipo de operação
      for row in cdc_tabela:
        valores = None
        if row[3] == 'DELETE':
            query = f"""DELETE FROM {self.tabela} WHERE {self.pk} = %s"""
            valores = (row[4],)  # Passar como tupla, mesmo com um único valor
        elif row[3] == 'INSERT':
            colunas = list(row[5].keys())
            valores = list(row[5].values())
            colunas_str = ', '.join(colunas)
            placeholders = ', '.join(['%s'] * len(valores))
            query = f"""INSERT INTO {self.tabela} ({colunas_str}) VALUES ({placeholders})"""
        elif row[3] == 'UPDATE':
            colunas_valores = ', '.join([f"{coluna} = %s" for coluna in row[5]])
            valores = list(row[5].values()) + [row[4]]
            query = f"""UPDATE {self.tabela} SET {colunas_valores} WHERE {self.pk} = %s"""

        try:
            conn = psycopg2.connect(
                host=self.dw_host,
                port=self.dw_porta,
                database=self.dw_database,
                user=self.dw_usuario,
                password=self.dw_senha
            )
            cursor = conn.cursor()

            if valores:
                cursor.execute(query, valores)
            else:
                cursor.execute(query)

            conn.commit()  # Faz commit após cada execução
            ls_ids.append(row[0])
        except Exception as e:
            print(f"Erro ao executar consulta: {e}")
        finally:
            if conn:
                cursor.close()
                conn.close()


      if ls_ids:
        try:
          # exclui os ids da tabela de cdc
          conn = psycopg2.connect(
                  host=self.fonte_host,
                  port=self.fonte_porta,
                  database=self.fonte_database,
                  user=self.fonte_usuario,
                  password=self.fonte_senha
              )
          cursor = conn.cursor()
          placeholders = ', '.join(['%s'] * len(ls_ids))
          query_delete = f"""DELETE FROM cdc.cdc WHERE id IN ({placeholders})"""
          cursor.execute(query_delete, ls_ids)
          conn.commit()
        except (Exception, psycopg2.Error) as error:
          print(f"Erro ao conectar-se com o banco de dados: {error}")
          conn.close()
        finally:
          conn.close()

      count+=1
      time.sleep(self.tempo_espera)

  def parar(self):
    """Define o sinal para interromper a execução do thread."""
    self._executando = False
    print("parando thread")

def monitoramento():
  st.title("Monitoramento da integração")

  monitor = []

  
  monitorando = False

  fonte = st.session_state.fonte
  dw = st.session_state.dw
  mapeamento = st.session_state.mapeamento

  if "ficha_thread" not in st.session_state:
    st.session_state.ficha_thread = {tabela: {"monitorando": False} for tabela in mapeamento.keys()}

  
  for tabela, value in mapeamento.items():
    map = st.session_state.mapeamento[tabela]
    relevancia = map.get("relevancia")
    volume = map.get("volume")
    tempo = map.get("tempo")
    pk = map.get('pk')

    if "monitorando" not in st.session_state:
      st.session_state.monitorando = False

    ficha_threads = st.session_state.ficha_thread
    ficha_tabela = ficha_threads.get(tabela)
    if ficha_tabela:
      monitorando = ficha_tabela.get("monitorando")


    if st.button("Iniciar Monitoramento", key=f"iniciar_monitoramento_{tabela}") and not monitorando:
      ficha_tabela["thread"] = MonitorBancoDeDados(
          fonte, dw, tabela, pk, tempo, relevancia, volume
      )
      ficha_tabela["thread"].start()
      ficha_tabela["monitorando"] = True
      ficha_threads[tabela] = ficha_tabela
      st.session_state.ficha_thread = ficha_threads
      st.rerun()

    if st.button("Parar Monitoramento", key=f"parar_monitoramento_{tabela}") and monitorando:
      ficha_tabela["thread"].parar()
      ficha_tabela["thread"].join()  # Aguarda a thread finalizar
      ficha_tabela["monitorando"] = False
      ficha_threads[tabela] = ficha_tabela
      st.session_state.ficha_thread = ficha_threads
      st.rerun()


    # código para mostrar monitoramento

    if st.button("Atualizar gráficos para {tabela}", key=f"att_{tabela}"):
      df = None
      conn = None
      try:
        conn = psycopg2.connect(
                host=fonte.get("ip"),
                port=fonte.get("porta"),
                database=fonte.get("db"),
                user=fonte.get("usuario"),
                password=fonte.get("senha")
            )
        df = pd.read_sql_query(f"SELECT * FROM cdc.monitor WHERE tabela='{tabela}'", conn)
        conn.close()
      except Exception as e:
        print(f"Erro ao executar consulta: {e}")
      finally:
        if conn:
          conn.close()
      # Cria os gráficos
      fig_volume = px.line(
          df, x="tempo", y="volume", title="Volume x Tempo", markers=True
      )
      fig_relevancia = px.line(
          df, x="tempo", y="relevancia", title="Relevância x Tempo", markers=True
      )

      # Destaca os pontos onde 'ativou' é True
      fig_volume.update_traces(
          marker=dict(color="red", size=10),
          selector=dict(arg="where", ativou=True),
      )
      fig_relevancia.update_traces(
          marker=dict(color="red", size=10),
          selector=dict(arg="where", ativou=True),
      )

      # Exibe os gráficos no Streamlit
      col1, col2 = st.columns(2)
      with col1:
          st.plotly_chart(fig_volume, use_container_width=True)
      with col2:
          st.plotly_chart(fig_relevancia, use_container_width=True)
  

  back = st.button("Anterior")
  if back:
    st.session_state.page = "configMapeamento"