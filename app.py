import streamlit as st
import pandas as pd
from datetime import datetime
from parser import extract_payment_data
from utils.indices import *

# Configuração da página
st.set_page_config(page_title="Correção Monetária Completa", layout="wide")
st.title("📈 Sistema de Correção Monetária")

# Upload do arquivo
uploaded_file = st.file_uploader("Carregue o arquivo PDF", type=["pdf"])

if uploaded_file:
    # Extração dos dados
    with st.spinner("Processando arquivo PDF..."):
        try:
            data = extract_payment_data(uploaded_file)
            
            # Mostra informações do cliente
            st.subheader("Informações do Cliente")
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**Nome:** {data['client_info'].get('name', '')}")
                st.write(f"**Contrato N°:** {data['client_info'].get('contract_number', '')}")
                st.write(f"**Data do Contrato:** {data['client_info'].get('contract_date', '')}")
                st.write(f"**Empreendimento:** {data['client_info'].get('project', '')}")
                
            with col2:
                st.write(f"**Endereço:** {data['client_info'].get('address', '')}")
                st.write(f"**Cidade/UF:** {data['client_info'].get('city', '')}/{data['client_info'].get('state', '')}")
                st.write(f"**Valor do Contrato:** {data['client_info'].get('contract_value', '')}")
                st.write(f"**Status:** {data['client_info'].get('status', '')}")
            
            # Converte pagamentos para DataFrame
            df_payments = pd.DataFrame(data['payments'])
            
            # Processa colunas de data e valor
            df_payments['due_date'] = pd.to_datetime(df_payments['due_date'], dayfirst=True, errors='coerce')
            df_payments['payment_date'] = pd.to_datetime(df_payments['payment_date'], dayfirst=True, errors='coerce')
            
            for col in ['installment_value', 'correction', 'fine', 'interest', 'total_value']:
                df_payments[col] = df_payments[col].apply(parse_currency)
            
            # Mostra tabela de pagamentos
            st.subheader("Tabela de Pagamentos")
            st.dataframe(df_payments)
            
            # Configuração da correção monetária
            st.sidebar.header("Configuração da Correção")
            
            metodo = st.sidebar.radio("Método de Correção", ["Índice Único", "Média de Índices"])
            indices = get_indices_disponiveis()
            
            if metodo == "Índice Único":
                indice = st.sidebar.selectbox("Selecione o Índice", options=list(indices.keys()))
                indices_para_calculo = [indice]
            else:
                selecionados = st.sidebar.multiselect(
                    "Selecione os Índices para Média",
                    options=list(indices.keys()),
                    default=["IGPM", "IPCA"]
                )
                indices_para_calculo = selecionados if len(selecionados) >= 2 else ["IGPM", "IPCA"]
            
            data_ref = st.sidebar.date_input("Data de Referência", datetime.now().date())
            
            if st.button("Calcular Correção Monetária"):
                with st.spinner("Calculando correções..."):
                    # Aplica correção monetária
                    resultados = []
                    
                    for _, row in df_payments.iterrows():
                        if pd.isna(row['due_date']) or pd.isna(row['installment_value']):
                            continue
                            
                        if metodo == "Índice Único":
                            correcao = calcular_correcao_individual(
                                row['installment_value'],
                                row['due_date'].date(),
                                data_ref,
                                indices_para_calculo[0]
                            )
                        else:
                            correcao = calcular_correcao_media(
                                row['installment_value'],
                                row['due_date'].date(),
                                data_ref,
                                indices_para_calculo
                            )
                        
                        resultados.append({
                            'Parcela': row['installment'],
                            'Data Vencimento': row['due_date'].strftime("%d/%m/%Y"),
                            'Valor Original': row['installment_value'],
                            'Índice(s)': ', '.join(indices_para_calculo),
                            'Valor Corrigido': correcao['valor_corrigido'],
                            'Variação (%)': correcao['variacao_percentual'],
                            'Status': 'Pago' if pd.notna(row['payment_date']) else 'Pendente'
                        })
                    
                    if resultados:
                        df_resultados = pd.DataFrame(resultados)
                        
                        st.subheader("Resultados da Correção Monetária")
                        st.dataframe(df_resultados)
                        
                        # Resumo financeiro
                        st.subheader("Resumo Financeiro")
                        col1, col2, col3 = st.columns(3)
                        
                        total_original = df_resultados['Valor Original'].sum()
                        total_corrigido = df_resultados['Valor Corrigido'].sum()
                        variacao_total = ((total_corrigido - total_original) / total_original) * 100
                        
                        col1.metric("Total Original", f"R$ {total_original:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
                        col2.metric("Total Corrigido", f"R$ {total_corrigido:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
                        col3.metric("Variação Total", f"{variacao_total:.2f}%")
                        
                        # Gráfico de evolução
                        st.line_chart(df_resultados.set_index('Data Vencimento')[['Valor Original', 'Valor Corrigido']])
                        
                        # Exportação
                        st.download_button(
                            "Exportar Resultados (CSV)",
                            df_resultados.to_csv(index=False),
                            "correcao_monetaria.csv",
                            "text/csv"
                        )
            
        except Exception as e:
            st.error(f"Erro ao processar o arquivo: {str(e)}")

def parse_currency(value):
    """Converte string monetária para float"""
    if pd.isna(value):
        return 0.0
    try:
        return float(str(value).replace('.', '').replace(',', '.'))
    except:
        return 0.0
