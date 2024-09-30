import streamlit as st

def conexao_dw():
  st.set_page_config(page_title = "conexão do Data Warehouse")
  st.title("Configuração do Data Warehouse")

  dw = st.session_state.dw

  ip_default = dw.get("ip") if dw.get("ip") else "localhost"
  porta_default = dw.get("porta") if dw.get("porta") else "5432"
  usuario_default = dw.get("usuario") if dw.get("usuario") else ""
  db_default = dw.get("db") if dw.get("db") else ""

  
  with st.form("form_conexao_dw"):
    ip = st.text_input("IP do servidor:", ip_default)
    porta = st.text_input("Porta:", porta_default)
    usuario = st.text_input("Usuário:", usuario_default)
    senha = st.text_input("Senha:", type="password")
    db = st.text_input("Database:", db_default)

    submitted = st.form_submit_button("Salvar")
    if submitted:
      dw["ip"] = ip
      dw["porta"] = porta
      dw["usuario"] = usuario
      dw["senha"] = senha
      dw["db"] = db
      st.session_state.dw = dw

  next = st.button("Próximo")
  if next:
    st.session_state.page = "configMapeamento"
  back = st.button("Anterior")
  if back:
    st.session_state.page = "ConexãoFonte"

