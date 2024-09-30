import streamlit as st
from home import home
from conexao_fonte import conexao_fonte
from conexao_dw import conexao_dw
from config_mapeamento import config_mapeamento
from monitoramento import monitoramento 

if "fonte" not in st.session_state:
  st.session_state.fonte = {
    "ip": "localhost",
    "porta": "5432",
    "usuario": "postgres",
    "senha" : "postgres",
    "db" : "eta_fonte",
  }

if "dw" not in st.session_state:
  st.session_state.dw = {
    "ip": "localhost",
    "porta": "5432",
    "usuario": "postgres",
    "senha" : "postgres",
    "db" : "eta_dw",
  }



def main():

  if "page" not in st.session_state:
    st.session_state.page = "Home"

  if st.session_state.page == "Home":
    home()
  elif st.session_state.page == "ConexãoFonte":
    conexao_fonte()
  elif st.session_state.page == "ConexãoDW":
    conexao_dw()
  elif st.session_state.page == "configMapeamento":
    config_mapeamento()
  elif st.session_state.page == "monitoramento":
    monitoramento()
    

if __name__ == "__main__":
  main()
