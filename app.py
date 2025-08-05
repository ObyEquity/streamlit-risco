import streamlit as st
import pandas as pd
import requests

# --- Segurança por senha ---
senha = st.text_input("Digite a senha para acessar o app:", type="password")
if senha != st.secrets["auth"]["password"]:
    st.warning("Senha incorreta.")
    st.stop()

st.set_page_config(page_title="Dashboard Oby Equities", layout="wide")

st.title("Dashboard de Carteira e Risco Oby Equities")

st.write('Navegue nas páginas ao lado de acordo com o que deseja')