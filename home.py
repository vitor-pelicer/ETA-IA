import streamlit as st

def home():
    st.title("Página Inicial")
    if st.button("Ir para configuração do projeto"):
        st.session_state.page = "ConexãoFonte"