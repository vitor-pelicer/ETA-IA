import streamlit as st

def conexao_fonte():
  st.set_page_config(page_title = "conexão da fonte de dados")
  st.title("Configuração da fonte de dados externa")

  fonte = st.session_state.fonte

  ip_default = fonte.get("ip") if fonte.get("ip") else "localhost"
  porta_default = fonte.get("porta") if fonte.get("porta") else "5432"
  usuario_default = fonte.get("usuario") if fonte.get("usuario") else ""
  db_default = fonte.get("db") if fonte.get("db") else ""

  
  with st.form("form_conexao_fonte"):
    ip = st.text_input("IP do servidor:", ip_default)
    porta = st.text_input("Porta:", porta_default)
    usuario = st.text_input("Usuário:", usuario_default)
    senha = st.text_input("Senha:", type="password")
    db = st.text_input("Database:", db_default)

    submitted = st.form_submit_button("Salvar")
    if submitted:
      fonte["ip"] = ip
      fonte["porta"] = porta
      fonte["usuario"] = usuario
      fonte["senha"] = senha
      fonte["db"] = db
      st.session_state.fonte = fonte

  next = st.button("Próximo")
  if next:
    st.session_state.page = "ConexãoDW"

