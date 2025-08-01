import streamlit as st
import pandas as pd
import requests

st.set_page_config(page_title="Dashboard de Fundos", layout="wide")

st.title("Dashboard de Carteira e Risco")

@st.cache_data
def carregar_dados():
    url = "https://cvtpiqvxswcmdhcprqzp.supabase.co/rest/v1/risco_pa_lsh1_ativos"
    headers = {
        "apikey": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImN2dHBpcXZ4c3djbWRoY3BycXpwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDY3MzA4NTUsImV4cCI6MjA2MjMwNjg1NX0.p9v5ZSyClhgI0eeUX5233bFsGZkSaUtFAyGYiwiW_2g",
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImN2dHBpcXZ4c3djbWRoY3BycXpwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDY3MzA4NTUsImV4cCI6MjA2MjMwNjg1NX0.p9v5ZSyClhgI0eeUX5233bFsGZkSaUtFAyGYiwiW_2g"
    }
    response = requests.get(url, headers=headers)
    return pd.DataFrame(response.json())

df = carregar_dados()

st.write(df.head())