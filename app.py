import streamlit as st
import pandas as pd
import pdfplumber
import re
from datetime import datetime
import pytz
from utils.indices import *

def extract_payments_from_pdf(pdf_file):
    """Função otimizada para extrair dados do PDF específico"""
    payments = []
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
                
            lines = text.split('\n')
            start_processing = False
            
            for line in lines:
                # Identifica o início da tabela
                if "Parcela DI Veném Atraso Valor Parc. Di. Receb. Vir da Parcela" in line:
                    start_processing = True
                    continue
                    
                if start_processing:
                    # Verifica se é uma linha de pagamento
                    if re.match(r'^(E|P)\.\d+/\d+', line.strip()):
                        try:
                            # Extrai os componentes usando posições fixas (ajuste conforme necessário)
                            parcela = line[0:10].strip()
                            dt_venc = line[10:20].strip()
                            valor_parc = line[30:45].strip()
                            dt_receb = line[45:55].strip()
                            valor_receb = line[55:70].strip()
                            
                            payments.append({
                                'Parcela': parcela,
                                'Dt Vencimento': dt_venc,
                                'Valor Parcela': valor_parc,
                                'Dt Recebimento': dt_receb,
                                'Valor Recebido': valor_receb
                            })
                            
                        except Exception as e:
                            st.warning(f"Erro ao processar linha: {line[:50]}... | Erro: {str(e)}")
                            continue
                    
                    # Finaliza ao encontrar o total
                    if "Total a pagar:" in line:
                        break
    
    return payments

def parse_custom_date(date_str):
    """Converte datas no formato DDMM/AAAA"""
    try:
        date_str = str(date_str).strip()
        if not date_str or date_str.lower() == 'nan':
            return None
        
        # Formato DDMM/AAAA
        if len(date_str) == 8 and '/' in date_str:
            day = int(date_str[:2])
            month = int(date_str[2:4])
            year = int(date_str[5:9])
            return datetime(year, month, day).date()
        
        return None
    except:
        return None

def parse_custom_currency(value_str):
    """Converte valores monetários brasileiros"""
    try:
        value_str = str(value_str).strip()
        if not value_str or value_str.lower() == 'nan':
            return 0.0
            
        # Remove caracteres não numéricos e converte vírgula para ponto
        cleaned = re.sub(r'[^\d,]', '', value_str).replace(',', '.')
        return float(cleaned)
    except:
        return 0.0

def format_currency(value):
    """Formata valores monetários para exibição"""
    try:
        return f"R$ {float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "R$ 0,00"

# Interface principal
st.set_page_config(page_title="Correção Monetária", layout="wide")
st.title("📊 Correção Monetária de Parcelas")

uploaded_file = st.file_uploader("Carregue o arquivo PDF", type=["pdf"])

if uploaded_file:
    try:
        with st.spinner("Processando arquivo..."):
            payments = extract_payments_from_pdf(uploaded_file)
            
            if not payments:
                st.error("Nenhum pagamento encontrado no documento. Estrutura do arquivo:")
                with pdfplumber.open(uploaded_file) as pdf:
                    st.text(pdf.pages[0].extract_text()[:1000])  # Mostra parte do conteúdo para debug
                st.stop()
            
            df = pd.DataFrame(payments)
            
            # Processa colunas importantes
            df['Data Vencimento'] = df['Dt Vencimento'].apply(parse_custom_date)
            df['Valor Original'] = df['Valor Parcela'].apply(parse_custom_currency)
            df['Valor Recebido'] = df['Valor Recebido'].apply(parse_custom_currency)
            
            st.success(f"✅ {len(df)} parcelas extraídas com sucesso!")
            
            # Mostra dados brutos
            st.subheader("Dados Extraídos")
            st.dataframe(df)
            
            # Configuração da correção
            st.sidebar.header("Configuração")
            
            metodo = st.sidebar.radio("Método", ["Índice Único", "Média de Índices"])
            indices = get_indices_disponiveis()
            
            if metodo == "Índice Único":
                indice = st.sidebar.selectbox("Índice", options=list(indices.keys()))
                indices_para_calculo = [indice]
            else:
                selecionados = st.sidebar.multiselect(
                    "Índices para média",
                    options=list(indices.keys()),
                    default=["IGPM", "IPCA"]
                )
                indices_para_calculo = selecionados if len(selecionados) >= 2 else ["IGPM", "IPCA"]
            
            data_ref = st.sidebar.date_input("Data referência", datetime.now().date())
            
            if st.button("Calcular Correção"):
                resultados = []
                
                for _, row in df.iterrows():
                    try:
                        if pd.isna(row['Data Vencimento']) or pd.isna(row['Valor Original']):
                            continue
                            
                        if metodo == "Índice Único":
                            correcao = calcular_correcao_individual(
                                row['Valor Original'],
                                row['Data Vencimento'],
                                data_ref,
                                indices_para_calculo[0]
                            )
                        else:
                            correcao = calcular_correcao_media(
                                row['Valor Original'],
                                row['Data Vencimento'],
                                data_ref,
                                indices_para_calculo
                            )
                        
                        resultados.append({
                            'Parcela': row['Parcela'],
                            'Data Vencimento': row['Data Vencimento'].strftime("%d/%m/%Y"),
                            'Valor Original': row['Valor Original'],
                            'Índice(s)': ', '.join(indices_para_calculo),
                            'Valor Corrigido': correcao['valor_corrigido'],
                            'Variação (%)': correcao['variacao_percentual']
                        })
                        
                    except Exception as e:
                        st.warning(f"Erro na parcela {row['Parcela']}: {str(e)}")
                
                if resultados:
                    df_resultados = pd.DataFrame(resultados)
                    
                    st.subheader("Resultados")
                    st.dataframe(df_resultados.style.format({
                        'Valor Original': format_currency,
                        'Valor Corrigido': format_currency,
                        'Variação (%)': "{:.4f}%"
                    }))
                    
                    # Resumo
                    st.subheader("Resumo")
                    col1, col2 = st.columns(2)
                    col1.metric("Total Original", format_currency(df_resultados['Valor Original'].sum()))
                    col2.metric("Total Corrigido", format_currency(df_resultados['Valor Corrigido'].sum()))
                    
                    # Exportação
                    st.download_button(
                        "Exportar CSV",
                        df_resultados.to_csv(index=False),
                        "correcao_monetaria.csv",
                        "text/csv"
                    )
                else:
                    st.error("Nenhum cálculo foi realizado. Verifique os dados.")
                    
    except Exception as e:
        st.error(f"Falha no processamento: {str(e)}")

st.markdown("---")
st.markdown("Sistema de Correção Monetária")
