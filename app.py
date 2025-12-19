import streamlit as st
import pandas as pd
from datetime import datetime, date
import pytz
from io import BytesIO
import base64

# Configuração deve ser a primeira linha
st.set_page_config(page_title="Correção Monetária", layout="wide")

try:
    from utils.indices import (get_indices_disponiveis, calcular_correcao_individual, 
                              calcular_correcao_media, formatar_moeda, limpar_cache)
    from utils.parser import extract_payment_data
except ImportError:
    st.error("Erro nos módulos. Verifique 'utils'.")
    st.stop()

st.title("📈 Correção Monetária Rápida")

# === SIDEBAR OTIMIZADA (Sem Ping em API) ===
st.sidebar.header("Configurações")
if st.sidebar.button("🧹 Limpar Cache"):
    limpar_cache()
    st.rerun()

indices = get_indices_disponiveis() # Agora retorna instantaneamente
lista_indices = list(indices.keys())

metodo = st.sidebar.radio("Método", ["Índice Único", "Média"])
selecionados = []

if metodo == "Índice Único":
    idx = st.sidebar.selectbox("Índice", lista_indices)
    selecionados = [idx]
else:
    selecionados = st.sidebar.multiselect("Índices", lista_indices, default=lista_indices)

# Data corrigida para português
data_ref = st.sidebar.date_input("Data Referência", value=date.today(), format="DD/MM/YYYY")

modo = st.sidebar.radio("Modo", ["PDF", "Manual"])

# === LÓGICA PRINCIPAL ===
def processar_calculo(df_parcelas):
    resultados = []
    bar = st.progress(0)
    
    for i, row in df_parcelas.iterrows():
        bar.progress((i + 1) / len(df_parcelas))
        
        # Garante que é objeto date
        dt_venc = row['Dt Vencim']
        if isinstance(dt_venc, str):
            dt_venc = datetime.strptime(dt_venc, "%Y-%m-%d").date()
            
        valor = row['Valor Original']
        
        if metodo == "Índice Único":
            res = calcular_correcao_individual(valor, dt_venc, data_ref, selecionados[0])
        else:
            res = calcular_correcao_media(valor, dt_venc, data_ref, selecionados)
            
        resultados.append({
            "Parcela": row['Parcela'],
            "Vencimento": dt_venc.strftime("%d/%m/%Y"),
            "Valor Original": valor,
            "Valor Corrigido": res['valor_corrigido'],
            "Fator": res['fator']
        })
        
    bar.empty()
    return pd.DataFrame(resultados)

# === MODO PDF ===
if modo == "PDF":
    st.info("📂 Carregue o PDF do relatório Aquarela")
    arquivo = st.file_uploader("Upload", type=["pdf"])
    
    if arquivo:
        # Usa o parser específico
        df = extract_payment_data(arquivo)
        
        if not df.empty:
            st.write(f"**{len(df)} parcelas encontradas.**")
            with st.expander("Ver dados extraídos"):
                st.dataframe(df)
            
            if st.button("Calcular Correção", type="primary"):
                with st.spinner("Buscando índices e calculando..."):
                    df_res = processar_calculo(df)
                    
                    # Exibição
                    st.success("Cálculo Finalizado!")
                    st.dataframe(df_res.style.format({
                        "Valor Original": "R$ {:,.2f}",
                        "Valor Corrigido": "R$ {:,.2f}",
                        "Fator": "{:.6f}"
                    }))
                    
                    # Totais
                    c1, c2 = st.columns(2)
                    tot_orig = df_res["Valor Original"].sum()
                    tot_corr = df_res["Valor Corrigido"].sum()
                    c1.metric("Total Original", formatar_moeda(tot_orig))
                    c2.metric("Total Corrigido", formatar_moeda(tot_corr), delta=formatar_moeda(tot_corr - tot_orig))
        else:
            st.warning("Nenhuma parcela encontrada. O layout do PDF pode ser imagem ou diferente do padrão.")

# === MODO MANUAL ===
else:
    st.subheader("Entrada Manual")
    if "manuais" not in st.session_state: st.session_state.manuais = []
    
    c1, c2, c3 = st.columns([2, 2, 1])
    # Data com formato BR
    v_val = c1.number_input("Valor", value=1000.0)
    v_data = c2.date_input("Data Vencimento", value=date(2023,1,1), format="DD/MM/YYYY")
    
    if c3.button("Adicionar"):
        st.session_state.manuais.append({"Parcela": f"M{len(st.session_state.manuais)+1}", "Dt Vencim": v_data, "Valor Original": v_val})
        st.rerun()
        
    if st.session_state.manuais:
        df_man = pd.DataFrame(st.session_state.manuais)
        st.dataframe(df_man.style.format({"Valor Original": "R$ {:,.2f}"}))
        
        if st.button("Calcular Manual", type="primary"):
            with st.spinner("Calculando..."):
                df_res = processar_calculo(df_man)
                st.dataframe(df_res.style.format({
                    "Valor Original": "R$ {:,.2f}",
                    "Valor Corrigido": "R$ {:,.2f}",
                    "Fator": "{:.6f}"
                }))
