import streamlit as st
import pandas as pd
import re
from datetime import datetime, date
from io import BytesIO
import base64

# Configuração da página (DEVE SER A PRIMEIRA LINHA)
st.set_page_config(page_title="Correção Monetária", layout="wide")
st.title("📈 Correção Monetária")

# Importações com tratamento de erro
try:
    from utils.indices import (
        get_indices_disponiveis,
        calcular_correcao_individual,
        calcular_correcao_media,
        formatar_moeda,
        limpar_cache
    )
    from utils.parser import extract_payment_data
except ImportError as e:
    st.error(f"Erro crítico nos arquivos do sistema: {e}")
    st.stop()

# ===== Classes de Dados =====
class Cliente:
    def __init__(self, codigo="N/A", nome="Não Identificado"):
        self.codigo = codigo
        self.nome = nome

class Venda:
    def __init__(self, numero="N/A", data="", valor=0.0):
        self.numero = numero
        self.data = data
        self.valor = valor

# ===== Processador do Cabeçalho (Cliente/Venda) =====
def extract_header_info(full_text):
    cliente = Cliente()
    venda = Venda()
    
    # Regex ajustada para o seu dump (Cliente: 3287 - Renato...)
    # Procura "Cliente:" seguido opcionalmente de aspas, números e traços
    match_cli = re.search(r'Cliente\s*[:\.]?\s*["\']?(\d+)?["\']?\s*[-–]?\s*["\']?([^\n\r",]+)', full_text, re.IGNORECASE)
    if match_cli:
        cliente.codigo = match_cli.group(1) if match_cli.group(1) else "N/A"
        cliente.nome = match_cli.group(2).strip()
    
    # Regex Venda (Venda: 495...)
    match_venda = re.search(r'Venda\s*[:\.]?\s*["\']?(\d+)', full_text, re.IGNORECASE)
    if match_venda:
        venda.numero = match_venda.group(1)
        
    # Regex Data Venda
    match_dt = re.search(r'Dt\.?\s*Venda\s*[:\.]?\s*(\d{2}/\d{2}/\d{4})', full_text, re.IGNORECASE)
    if match_dt:
        venda.data = match_dt.group(1)

    return cliente, venda

# ===== Interface Principal =====
def main():
    # --- Sidebar ---
    st.sidebar.header("Configurações")
    if st.sidebar.button("🧹 Limpar Cache (Resolver bugs)"):
        limpar_cache()
        st.rerun()

    # Busca índices (simples e rápido)
    indices_dict = get_indices_disponiveis()
    lista_indices = list(indices_dict.keys())
    
    metodo = st.sidebar.radio("Método", ["Índice Único", "Média"])
    sel_indices = []
    
    if metodo == "Índice Único":
        idx = st.sidebar.selectbox("Índice", lista_indices)
        if idx: sel_indices = [idx]
    else:
        sel_indices = st.sidebar.multiselect("Índices", lista_indices, default=lista_indices)
    
    data_ref = st.sidebar.date_input("Data Referência", value=date.today(), format="DD/MM/YYYY")
    modo = st.sidebar.radio("Modo", ["PDF Automático", "Manual"])

    # --- Área Principal ---
    if modo == "PDF Automático":
        uploaded_file = st.file_uploader("Arraste seu PDF aqui", type=["pdf"])
        
        if uploaded_file:
            # 1. Extração de Texto para Debug e Cabeçalho
            import pdfplumber
            full_text = ""
            with pdfplumber.open(uploaded_file) as pdf:
                for page in pdf.pages:
                    full_text += page.extract_text() or ""
            
            # --- DEBUGGER ---
            with st.expander("🔍 Debug / Ver Texto Extraído (Clique aqui se os dados falharem)"):
                st.text_area("Texto Bruto", full_text, height=200)
                st.info("O sistema tenta ler o formato CSV/Tabela que aparece neste texto.")

            # 2. Processa Cabeçalho
            cli, ven = extract_header_info(full_text)
            
            col1, col2 = st.columns(2)
            col1.info(f"👤 **Cliente:** {cli.codigo} - {cli.nome}")
            col2.info(f"💰 **Venda:** {ven.numero} ({ven.data})")

            # 3. Processa Parcelas (Usando utils/parser.py)
            uploaded_file.seek(0) # Reseta ponteiro do arquivo
            df = extract_payment_data(uploaded_file)
            
            if not df.empty:
                st.success(f"✅ {len(df)} parcelas encontradas!")
                
                # Mostra dados brutos
                with st.expander("Ver Tabela Original", expanded=True):
                    st.dataframe(df.style.format({"Valor Original": "R$ {:,.2f}", "Valor Pago": "R$ {:,.2f}"}))

                if st.button("🚀 Calcular Correção", type="primary"):
                    if not sel_indices:
                        st.error("Selecione pelo menos um índice na barra lateral!")
                    else:
                        # Processamento do Cálculo
                        resultados = []
                        bar = st.progress(0)
                        
                        for i, row in df.iterrows():
                            bar.progress((i+1)/len(df))
                            
                            valor_base = row['Valor Original']
                            dt_venc = row['Dt Vencim']
                            if not isinstance(dt_venc, date): continue

                            # Correção do Valor Original
                            if metodo == "Índice Único":
                                res = calcular_correcao_individual(valor_base, dt_venc, data_ref, sel_indices[0])
                            else:
                                res = calcular_correcao_media(valor_base, dt_venc, data_ref, sel_indices)
                            
                            resultados.append({
                                "Parcela": row['Parcela'],
                                "Vencimento": dt_venc.strftime("%d/%m/%Y"),
                                "Original": valor_base,
                                "Corrigido": res.get('valor_corrigido', 0),
                                "Fator": res.get('fator_correcao', 1.0)
                            })
                        
                        bar.empty()
                        df_res = pd.DataFrame(resultados)
                        
                        # Exibição Final
                        st.divider()
                        st.subheader("Resultado da Correção")
                        st.dataframe(df_res.style.format({
                            "Original": "R$ {:,.2f}", 
                            "Corrigido": "R$ {:,.2f}",
                            "Fator": "{:.6f}"
                        }))
                        
                        # Totais
                        c1, c2, c3 = st.columns(3)
                        t_orig = df_res["Original"].sum()
                        t_corr = df_res["Corrigido"].sum()
                        c1.metric("Total Original", formatar_moeda(t_orig))
                        c2.metric("Total Corrigido", formatar_moeda(t_corr))
                        c3.metric("Diferença", formatar_moeda(t_corr - t_orig))
                        
                        # Download
                        csv = df_res.to_csv(index=False).encode('utf-8')
                        st.download_button("📥 Baixar Relatório CSV", csv, "relatorio_corrigido.csv", "text/csv")
            else:
                st.warning("⚠️ Nenhuma parcela identificada. Verifique se o PDF é legível ou abra o Debug acima.")
    
    # --- Modo Manual ---
    else:
        st.subheader("Cálculo Manual")
        
        # Estado da sessão para lista manual
        if "lista_manual" not in st.session_state:
            st.session_state.lista_manual = []

        # Inputs
        c1, c2, c3 = st.columns([2,2,1])
        val = c1.number_input("Valor Original", value=1000.0)
        dt = c2.date_input("Data Vencimento", value=date(2023,1,1), format="DD/MM/YYYY")
        
        if c3.button("Adicionar à Lista"):
            st.session_state.lista_manual.append({
                "Valor Original": val,
                "Dt Vencim": dt
            })
            st.rerun()

        # Mostra e Calcula Lista
        if st.session_state.lista_manual:
            df_man = pd.DataFrame(st.session_state.lista_manual)
            st.dataframe(df_man.style.format({"Valor Original": "R$ {:,.2f}"}))
            
            if st.button("Calcular Todos Manualmente", type="primary"):
                if not sel_indices:
                     st.error("Selecione um índice!")
                else:
                    res_manual = []
                    for idx, row in df_man.iterrows():
                        if metodo == "Índice Único":
                            r = calcular_correcao_individual(row["Valor Original"], row["Dt Vencim"], data_ref, sel_indices[0])
                        else:
                            r = calcular_correcao_media(row["Valor Original"], row["Dt Vencim"], data_ref, sel_indices)
                        
                        res_manual.append({
                            "Vencimento": row["Dt Vencim"],
                            "Original": row["Valor Original"],
                            "Corrigido": r.get('valor_corrigido', 0),
                            "Fator": r.get('fator_correcao', 1)
                        })
                    
                    df_res_man = pd.DataFrame(res_manual)
                    st.success("Calculado!")
                    st.dataframe(df_res_man.style.format({"Original": "R$ {:,.2f}", "Corrigido": "R$ {:,.2f}", "Fator": "{:.6f}"}))

if __name__ == "__main__":
    main()
