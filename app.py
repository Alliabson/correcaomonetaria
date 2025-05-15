import streamlit as st
import pandas as pd
import pdfplumber
import re
from datetime import datetime
import pytz
from utils.indices import *

# Configuração da página
st.set_page_config(page_title="Correção Monetária Completa", layout="wide")

# Título e descrição
st.title("📊 Correção Monetária Avançada")
st.markdown("""
Extração completa de tabelas de PDF e correção monetária seletiva.
""")

# Funções de processamento
def extract_table_from_pdf(pdf_file):
    """Extrai a tabela completa do PDF com abordagem direta"""
    table_data = []
    headers = []
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
                
            lines = text.split('\n')
            in_table = False
            
            for line in lines:
                # Identifica o cabeçalho da tabela
                if "Parcela" in line and "DI Veném" in line and "Valor Parc." in line:
                    in_table = True
                    headers = [h.strip() for h in re.split(r'\s{2,}', line.strip())]
                    continue
                
                if in_table:
                    # Padrão para identificar linhas de parcelas
                    if re.match(r'^(E|P)\.\d+/\d+', line.strip()):
                        # Divide a linha mantendo a estrutura das colunas
                        parts = re.split(r'\s{2,}', line.strip())
                        
                        # Garante que temos valores para todas as colunas
                        if len(parts) >= len(headers):
                            table_data.append(parts[:len(headers)])
                        elif len(parts) > 3:  # Pelo menos Parcela, Data, Valor
                            row = parts + [""] * (len(headers) - len(parts))
                            table_data.append(row)
                    
                    # Finaliza quando encontrar o total
                    if "Total a pagar:" in line:
                        break
    
    return headers, table_data

def parse_custom_date(date_str):
    """Converte datas no formato DDMM/AAAA"""
    try:
        date_str = str(date_str).strip()
        if not date_str or date_str.lower() == 'nan':
            return None
        
        # Remove texto adicional (como "Atraso")
        date_str = re.sub(r'[^0-9/]', '', date_str)
        
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
            
        # Remove R$ e espaços, trata vírgula decimal
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
uploaded_file = st.file_uploader("Carregue o arquivo PDF", type=["pdf"])

if uploaded_file:
    try:
        # Extração da tabela
        with st.spinner("Extraindo tabela do PDF..."):
            headers, table_data = extract_table_from_pdf(uploaded_file)
            
            if not table_data:
                st.error("Nenhuma tabela foi identificada no documento.")
                st.stop()
            
            # Cria DataFrame com todas as colunas originais
            df = pd.DataFrame(table_data, columns=headers)
            
            # Processa colunas de data e valor
            df['Data Vencimento'] = df['DI Veném Atraso'].apply(parse_custom_date)
            df['Valor Original'] = df['Valor Parc.'].apply(parse_custom_currency)
            df['Valor Recebido'] = df['Vlr da Parcela'].apply(parse_custom_currency)
            
            st.success(f"Tabela extraída com {len(df)} parcelas!")

            # Seleção de colunas para exibição
            st.subheader("Selecione as colunas para exibir")
            colunas_selecionadas = st.multiselect(
                "Colunas disponíveis",
                options=headers + ['Data Vencimento', 'Valor Original', 'Valor Recebido'],
                default=['Parcela', 'Data Vencimento', 'Valor Original', 'Valor Recebido']
            )
            
            # Mostra tabela filtrada
            st.dataframe(df[colunas_selecionadas].style.format({
                'Valor Original': format_currency,
                'Valor Recebido': format_currency
            }))
            
            # Configuração da correção
            st.sidebar.header("Parâmetros de Correção")
            
            metodo_correcao = st.sidebar.radio(
                "Método de Correção",
                options=["Índice Único", "Média de Índices"],
                index=0
            )
            
            indices_disponiveis = get_indices_disponiveis()
            
            if metodo_correcao == "Índice Único":
                indice_selecionado = st.sidebar.selectbox(
                    "Selecione o índice econômico",
                    options=list(indices_disponiveis.keys()),
                    index=0
                )
                indices_para_calculo = [indice_selecionado]
            else:
                indices_selecionados = st.sidebar.multiselect(
                    "Selecione os índices para cálculo da média",
                    options=list(indices_disponiveis.keys()),
                    default=["IGPM", "IPCA", "INCC"]
                )
                indices_para_calculo = indices_selecionados if len(indices_selecionados) >= 2 else ["IGPM", "IPCA", "INCC"]
            
            data_referencia = st.sidebar.date_input(
                "Data de referência para correção",
                value=datetime.now(pytz.timezone('America/Sao_Paulo')).date(),
                format="DD/MM/YYYY"
            )
            
            if st.button("Aplicar Correção Monetária"):
                # Aplicar correção
                resultados = []
                
                for idx, row in df.iterrows():
                    try:
                        if pd.isna(row['Data Vencimento']) or pd.isna(row['Valor Original']):
                            continue
                            
                        if metodo_correcao == "Índice Único":
                            correcao = calcular_correcao_individual(
                                row['Valor Original'],
                                row['Data Vencimento'],
                                data_referencia,
                                indices_para_calculo[0]
                            )
                        else:
                            correcao = calcular_correcao_media(
                                row['Valor Original'],
                                row['Data Vencimento'],
                                data_referencia,
                                indices_para_calculo
                            )
                        
                        # Adiciona resultado
                        resultado = {
                            'Parcela': row['Parcela'],
                            'Data Vencimento': row['Data Vencimento'].strftime("%d/%m/%Y"),
                            'Valor Original': row['Valor Original'],
                            'Valor Recebido': row['Valor Recebido'],
                            'Índice(s)': ', '.join(indices_para_calculo) if metodo_correcao == "Média de Índices" else indices_para_calculo[0],
                            'Fator de Correção': correcao['fator_correcao'],
                            'Variação (%)': correcao['variacao_percentual'],
                            'Valor Corrigido': correcao['valor_corrigido']
                        }
                        resultados.append(resultado)
                        
                    except Exception as e:
                        st.warning(f"Erro ao corrigir parcela {row['Parcela']}: {str(e)}")
                        continue
                
                if resultados:
                    df_resultados = pd.DataFrame(resultados)
                    
                    st.subheader("Resultados da Correção")
                    st.dataframe(df_resultados.style.format({
                        'Valor Original': format_currency,
                        'Valor Recebido': format_currency,
                        'Valor Corrigido': format_currency,
                        'Fator de Correção': "{:.6f}",
                        'Variação (%)': "{:.4f}%"
                    }))
                    
                    # Resumo
                    st.subheader("Resumo Financeiro")
                    col1, col2, col3 = st.columns(3)
                    total_original = df_resultados['Valor Original'].sum()
                    total_corrigido = df_resultados['Valor Corrigido'].sum()
                    variacao = total_corrigido - total_original
                    
                    col1.metric("Total Original", format_currency(total_original))
                    col2.metric("Total Corrigido", format_currency(total_corrigido))
                    col3.metric("Variação Total", format_currency(variacao))
                    
                    # Exportação
                    st.subheader("Exportar Resultados")
                    csv = df_resultados.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "Baixar como CSV",
                        data=csv,
                        file_name="resultado_correcao.csv",
                        mime="text/csv"
                    )
                else:
                    st.error("Nenhum valor foi corrigido. Verifique os dados de entrada.")
                    
    except Exception as e:
        st.error(f"Erro ao processar o arquivo: {str(e)}")

# Rodapé
st.markdown("---")
st.markdown("Desenvolvido por Dev.Alli | Project - © 2025")
