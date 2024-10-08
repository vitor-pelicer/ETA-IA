import pandas as pd
import streamlit as st
import psycopg2
import threading
import time
import queue
from streamlit.runtime.scriptrunner import add_script_run_ctx
from datetime import datetime
import plotly.express as px
import numpy as np
import gymnasium as gym
from gymnasium import spaces
from stable_baselines3 import PPO
from stable_baselines3.common.env_checker import check_env
import gymnasium as gym
from gymnasium import spaces


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
  


class Ambiente(gym.Env):
    def __init__(self, volume_inicial=0, relevancia_inicial=0, 
                 delta_volume=400, delta_relevancia=0.4):
        super().__init__()

        self.volume_inicial = volume_inicial
        self.relevancia_inicial = relevancia_inicial

        self.delta_volume_inicial = delta_volume
        self.delta_relevancia_inicial = delta_relevancia

        self.ativacoes = []
        self.media_ativacoes = 0
        
        # Define o espaço de ações:
        self.action_space = spaces.Box(
            low=np.array([-50, -0.1]),  # Limites mínimos para delta_volume e delta_relevancia
            high=np.array([50, 0.1]),   # Limites máximos
            dtype=np.float32
        )

        self.observation_space = spaces.Dict({
            'volume': spaces.Discrete(1001),  # 0 a 10000
            'relevancia': spaces.Discrete(100),  # 0 a 99 -> 0.00 a 0.99
            'delta_volume': spaces.Discrete(1001),  # 0 a 10000
            'delta_relevancia': spaces.Discrete(100),  # 0 a 99 -> 0.00 a 0.99
            'media_ativacoes': spaces.Discrete(201),  # 0 a 200 -> -1.0 a 1.0
        })


        self.reset()

    def reset(self, seed=None, options=None):
        """Reinicia o ambiente para um novo episódio."""
        super().reset(seed=seed) # Para garantir compatibilidade com versões futuras do Gymnasium

        self.volume_atual = self.volume_inicial  
        self.relevancia_atual = self.relevancia_inicial 

        self.delta_volume = self.delta_volume_inicial
        self.delta_relevancia = self.delta_relevancia_inicial

        self.ativacoes = []
        self.media_ativacoes = 0

        # Retorna a observação e informações adicionais (vazio neste caso)
        return self._get_obs(), {}

    def _get_obs(self):
        """Retorna o estado atual do ambiente."""
        return {'volume': int(self.volume_atual), 
                'relevancia': int(self.relevancia_atual * 100),
                'delta_volume': int(self.delta_volume),
                'delta_relevancia': int(self.delta_relevancia * 100),
                'media_ativacoes': int(self.media_ativacoes * 100)+100
                }
    
    def set_volume(self, volume):
       self.volume_atual = np.clip(volume, 0.1, 0.99)
    
    def set_relevancia(self, relevancia):
       self.relevancia_atual = np.clip(relevancia, 0.1, 0.99)

    def step(self, action):
        """Aplica a ação e atualiza o ambiente."""
        delta_volume, delta_relevancia = action[0], action[1]  # Extrai os valores do array

        # Atualiza os deltas (sem necessidade de ajustes adicionais)
        self.delta_relevancia += delta_relevancia
        self.delta_volume += delta_volume

        self.delta_relevancia = np.clip(self.delta_relevancia, 0.1, 0.99)
        self.delta_volume = np.clip(self.delta_volume, 200, 1000)

        self.delta_relevancia = np.clip(self.delta_relevancia, 0.1, 0.99)
        self.delta_volume = np.clip(self.delta_volume, 200, 1000)

        # Impede que os valores ultrapassem os limites
        self.volume_atual = np.clip(self.volume_atual, 0, 1000)
        self.relevancia_atual = np.clip(self.relevancia_atual, 0, 1)

        # Determina se o processo foi ativado
        processo_ativado = self.volume_atual >= self.delta_volume or \
                           self.relevancia_atual >= self.delta_relevancia
        
        # Reset caso o processo seja ativado
        if processo_ativado:

            if self.volume_atual >= self.delta_volume and self.relevancia_atual >= self.delta_relevancia:
                self.ativacoes.append(0)
            elif self.volume_atual >= self.delta_volume:
                self.ativacoes.append(1)
            elif self.relevancia_atual >= self.delta_relevancia:
                self.ativacoes.append(-1)
            

            if len(self.ativacoes) > 10:
                self.ativacoes.pop(0)

            if len(self.ativacoes) > 0:
                self.media_ativacoes = sum(self.ativacoes) / (len(self.ativacoes))

            

            self.volume_atual = 0
            self.relevancia_atual = 0

        # Calcula a recompensa
        reward = self.calcular_recompensa(processo_ativado, 
                                          self.volume_atual, 
                                          self.relevancia_atual,
                                          self.delta_volume,
                                          self.delta_relevancia,
                                          self.media_ativacoes
                                          )

        done = False
        truncated = False  

        return self._get_obs(), reward, done, truncated, {}

    def calcular_recompensa(self, processo_ativado, volume, relevancia, delta_volume, delta_relevancia, media_ativacoes):
        recompensa = 0

        # Penalidades por deltas fora do intervalo
        if delta_volume < 200:
            recompensa -= (200 - delta_volume) / 100
        elif delta_volume > 8000:
            recompensa -= (delta_volume - 800) / 100

        if delta_relevancia < 0.2:
            recompensa -= (0.2 - delta_relevancia) * 10 
        elif delta_relevancia > 0.7:
            recompensa -= (delta_relevancia - 0.7) * 10

        if abs(media_ativacoes) > 0.5:
            recompensa -= 0.8
        else:
            recompensa += 1

        # Bônus por deltas no intervalo ideal
        if 2000 <= delta_volume <= 800 and 0.2 <= delta_relevancia <= 0.7:
            recompensa += 1 

        return recompensa
  

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


    env = Ambiente(volume_inicial=0, relevancia_inicial=0, 
                 delta_volume=self.relevancia_minima, delta_relevancia=self.relevancia_minima)
    
    check_env(env)

    model = PPO("MultiInputPolicy", env, gamma=0.4)

    model = model.load("modelo_ppo10k_windows.zip")

    count = 0
    ativou = False
    while self._executando:

      # Calcula a relevância e o volume usando as funções fornecidas
      relevancia_atual = consultar_relevancia(self.fonte_host, self.fonte_porta, self.fonte_usuario, self.fonte_senha, self.fonte_database, self.tabela)
      volume_atual = consultar_volume(self.fonte_host, self.fonte_porta, self.fonte_usuario, self.fonte_senha, self.fonte_database, self.tabela)

      env.set_relevancia(relevancia_atual)
      env.set_volume(volume_atual)

      print(f"Processando: R: {relevancia_atual} V:{volume_atual}")

      # Determina se vai realizar exploração (False) ou se irá buscar o melhor resultado (True)
      obs = env._get_obs()
      action, _ = model.predict(obs, deterministic=True)
      obs, reward, done, _, _ = env.step(action)

      self.relevancia_minima = obs.get("delta_relevancia") / 100
      self.volume_minimo = obs.get("delta_volume")

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

    if st.button(f"Atualizar gráficos para {tabela}", key=f"att_{tabela}"):
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
        df = pd.read_sql_query(f"SELECT * FROM cdc.monitor WHERE tabela='{tabela}' ORDER BY tempo ASC", conn)
        conn.close()
      except Exception as e:
        print(f"Erro ao executar consulta: {e}")
      finally:
        if conn:
          conn.close()
      # Cria os gráficos
      fig_volume = px.line(
          df, 
          x="tempo", 
          y="volume", 
          title="Volume x Tempo", 
          markers=True  # Mantemos os marcadores para todos os pontos
      )

      # Cria uma lista de cores para os marcadores
      cores_volume = ['red' if ativado else 'blue' for ativado in df["ativou"]] 
      fig_volume.update_traces(marker=dict(color=cores_volume, size=10))

      # Repetimos para o outro gráfico
      fig_relevancia = px.line(
          df, 
          x="tempo", 
          y="relevancia", 
          title="Relevância x Tempo", 
          markers=True
      )

      # Cria uma lista de cores para os marcadores
      cores_relevancia = ['red' if ativado else 'blue' for ativado in df["ativou"]]
      fig_relevancia.update_traces(marker=dict(color=cores_relevancia, size=10))

      # Adiciona linhas extras para delta_v e delta_r (mantemos essa parte)
      fig_volume.add_scatter(x=df["tempo"], y=df["delta_v"], mode='lines', name='Delta Volume')
      fig_relevancia.add_scatter(x=df["tempo"], y=df["delta_r"], mode='lines', name='Delta Relevância')

      # Exibe os gráficos no Streamlit
      col1, col2 = st.columns(2)
      with col1:
          st.plotly_chart(fig_volume, use_container_width=True)
      with col2:
          st.plotly_chart(fig_relevancia, use_container_width=True)
  

  back = st.button("Anterior")
  if back:
    st.session_state.page = "configMapeamento"