import streamlit as st
import psycopg2

def config_mapeamento():
  st.title("Configuração do Mapeamento")

  fonte = st.session_state.fonte
  
  try:
      # Conexão com o banco de dados
      conn = psycopg2.connect(
          host=fonte.get("ip"),
          port=fonte.get("porta"),
          database=fonte.get("db"),
          user=fonte.get("usuario"),
          password=fonte.get("senha")
      )

      # Cria um cursor para executar comandos SQL
      cursor = conn.cursor()

      # Executa a consulta para listar as tabelas
      cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")

      # Recupera os resultados da consulta
      tables = cursor.fetchall()

      # Extrai os nomes das tabelas da lista de tuplas
      table_names = [table[0] for table in tables]

      # Cria um multiselect para escolher as tabelas
      selected_tables = st.multiselect("Selecione as tabelas:", table_names)

      mapeamento = {}
      st.write("Aqui são definidos os valores ΔR, ΔV, e tempo de atualização do integrador")
      if selected_tables:
        for table in selected_tables:
            st.write(f"**Parâmetros para a tabela: {table}**")  # Título para cada tabela

            col1, col2, col3 = st.columns(3)

            with col1:
                relevancia = st.number_input("Relevância (0-1):", min_value=0.0, max_value=1.0, value=0.5, key=f"relevancia_{table}")
                pk = st.text_input("chave primária", "")
            with col2:
                volume = st.number_input("Volume:", min_value=1, value=100, key=f"volume_{table}")
            with col3:
                tempo = st.number_input("Tempo (minutos):", min_value=1, step=1, value=7, key=f"tempo_{table}")
            mapeamento[table] = {
                "relevancia": relevancia,
                "volume": volume,
                "tempo": tempo,
                "pk": pk, 
                }

  except (Exception, psycopg2.Error) as error:
      st.error(f"Erro ao conectar ao banco de dados: {error}")

  finally:
      conn.close()
      salvar = st.button("salvar")
      if salvar:
        st.session_state.mapeamento = mapeamento
      proximo = st.button("proximo")
      if proximo:
        st.session_state.mapeamento = mapeamento
        st.session_state.page = "monitoramento"