import streamlit as st
import pandas as pd
from datetime import datetime
from parser import extract_pdf_data

# Configuração da página (PRIMEIRO COMANDO)
st.set_page_config(
    page_title="Extrator de Parcelas PDF",
    layout="wide",
    page_icon="📄"
)

# Opções de colunas disponíveis
COLUMN_OPTIONS = [
    'Parcela',
    'Dt Vencim',
    'Valor Parc.',
    'Dt. Receb.',
    'Vlr da Parcela',
    'Correção',
    'Multa',
    'Juros Atr.',
    'Desconto',
    'Atraso'
]

def main():
    st.title("📄 Extrator de Parcelas de PDF")
    st.markdown("""
    Carregue um relatório PDF e selecione as colunas que deseja extrair.
    """)
    
    # Upload do arquivo
    uploaded_file = st.file_uploader(
        "Carregue seu arquivo PDF",
        type=["pdf"],
        accept_multiple_files=False
    )
    
    # Seletor de colunas
    st.sidebar.header("🔧 Configurações")
    selected_columns = st.sidebar.multiselect(
        "Selecione as colunas para extrair",
        options=COLUMN_OPTIONS,
        default=['Parcela', 'Dt Vencim', 'Valor Parc.', 'Vlr da Parcela']
    )
    
    if uploaded_file and selected_columns:
        with st.spinner("Processando PDF..."):
            df = extract_pdf_data(uploaded_file, selected_columns)
            
            if not df.empty:
                st.success(f"✅ {len(df)} parcelas extraídas com sucesso!")
                
                # Mostrar dados
                st.dataframe(df)
                
                # Opção para exportar
                if st.button("Exportar para Excel"):
                    csv = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Baixar CSV",
                        data=csv,
                        file_name="parcelas_extraidas.csv",
                        mime="text/csv"
                    )
            else:
                st.warning("Nenhuma parcela foi identificada com as colunas selecionadas.")

if __name__ == "__main__":
    main()
