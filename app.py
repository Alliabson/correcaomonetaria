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
    """Extrai a tabela completa do PDF usando múltiplas estratégias"""
    table_data = []
    headers = []
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            # Primeiro tentamos extrair tabelas usando o algoritmo padrão
            tables = page.extract_tables({
                "vertical_strategy": "text",
                "horizontal_strategy": "text",
                "intersection_y_tolerance": 15
            })
            
            for table in tables:
                if len(table) > 1:  # Ignora tabelas vazias
                    if any("Parcela" in str(cell) for cell in table[0]):
                        headers = [str(cell).strip() for cell in table[0]]
                        for row in table[1:]:
                            if len(row) == len(headers):
                                table_data.append(row)
                        break
            
            # Se não encontrou tabela, tenta extração manual
            if not table_data:
                text = page.extract_text()
                if not text:
                    continue
                    
                lines = text.split('\n')
                in_table = False
                
                for line in lines:
                    # Identifica o cabeçalho da tabela
                    if "Parcela" in line and ("DI Veném" in line or "Dt Vencim" in line) and "Valor Parc." in line:
                        in_table = True
                        headers = [h.strip() for h in re.split(r'\s{2,}', line.strip())]
                        continue
                    
                    if in_table:
                        # Verifica se é uma linha de parcela
                        if re.match(r'^(E|P|PR|BR|SM)\.\d+/\d+', line.strip()):
                            # Processa a linha considerando espaços como delimitadores
                            parts = [p.strip() for p in re.split(r'\s{2,}', line.strip()) if p.strip()]
                            
                            # Garante que temos valores para todas as colunas
                            if len(parts) >= len(headers):
                                table_data.append(parts[:len(headers)])
                            elif len(parts) > 3:  # Pelo menos Parcela, Data, Valor
                                # Preenche as colunas faltantes com valores vazios
                                row = parts + [""] * (len(headers) - len(parts))
                                table_data.append(row)
                        
                        # Finaliza quando encontrar o total
                        if "Total a pagar:" in line or "TOTAL GERAL:" in line:
                            break
    
    return headers, table_data

def parse_custom_date(date_str):
    """Converte datas no formato DDMM/AAAA ou similar"""
    try:
        date_str = str(date_str).strip()
        if not date_str or date_str.lower() == 'nan':
            return None
        
        # Remove texto adicional (como "Atraso")
        date_str = re.sub(r'[^0-9/]', '', date_str)
        
        # Tenta vários formatos de data
        for fmt in ['%d%m/%Y', '%d/%m/%Y', '%d-%m-%Y']:
            try:
                if fmt == '%d%m/%Y':
                    clean_date = date_str.replace('/', '')
                    if len(clean_date) == 8:
                        day = int(clean_date[:2])
                        month = int(clean_date[2:4])
                        year = int(clean_date[4:8])
                        return datetime(year, month, day).date()
                else:
                    return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        return None
    except:
        return None

def parse_custom_currency(value_str):
    """Converte valores monetários brasileiros"""
    try:
        value_str = str(value_str).strip()
        if not value_str or value_str.lower() == 'nan':
            return 0.0
            
        # Remove caracteres não numéricos exceto vírgula e ponto
        cleaned = re.sub(r'[^\d,.-]', '', value_str)
        
        # Caso 1.234,56 → 1234.56
        if '.' in cleaned and ',' in cleaned:
            return float(cleaned.replace('.', '').replace(',', '.'))
        # Caso 1,234.56 → 1234.56
        elif ',' in cleaned and cleaned.count(',') == 1 and len(cleaned.split(',')[1]) == 2:
            return float(cleaned.replace(',', ''))
        # Caso 1234,56 → 1234.56
        elif ',' in cleaned:
            return float(cleaned.replace(',', '.'))
        else:
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
                st.error("Nenhuma tabela foi identificada no documento. Visualização do conteúdo bruto:")
                
                # Mostra conteúdo do PDF para debug
                with pdfplumber.open(uploaded_file) as pdf:
                    for page in pdf.pages:
                        st.text(page.extract_text())
                st.stop()
            
            # Cria DataFrame com todas as colunas originais
            df = pd.DataFrame(table_data, columns=headers)
            
            # Processa colunas importantes
            date_columns = [col for col in headers if any(x in col.lower() for x in ['vencim', 'veném', 'receb'])]
            for col in date_columns:
                df[col] = df[col].apply(parse_custom_date)
            
            currency_columns = [col for col in headers if any(x in col.lower() for x in ['valor', 'vlr', 'parcela'])]
            for col in currency_columns:
                df[col] = df[col].apply(parse_custom_currency)
            
            st.success(f"Tabela extraída com {len(df)} parcelas e {len(headers)} colunas!")
            
            # Mostra tabela completa
            st.subheader("Tabela Completa Extraída")
            st.dataframe(df)
            
            # Configuração da correção
            st.subheader("Configuração da Correção Monetária")
            
            col1, col2 = st.columns(2)
            
            with col1:
                coluna_valor_original = st.selectbox(
                    "Selecione a coluna com os valores originais",
                    options=headers,
                    index=next((i for i, x in enumerate(headers) if 'valor' in x.lower()), 0)
                )
                
            with col2:
                coluna_valor_recebido = st.selectbox(
                    "Selecione a coluna com os valores recebidos",
                    options=headers,
                    index=next((i for i, x in enumerate(headers) if 'vlr' in x.lower()), 0)
                )
            
            # Configuração dos índices
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
                # Aplicar correção às colunas selecionadas
                resultados = []
                
                for idx, row in df.iterrows():
                    try:
                        valor_original = parse_custom_currency(row[coluna_valor_original])
                        
                        # Tenta encontrar a data de vencimento
                        data_vencimento = None
                        for col in date_columns:
                            temp_date = parse_custom_date(row[col])
                            if temp_date:
                                data_vencimento = temp_date
                                break
                        
                        if not data_vencimento or pd.isna(valor_original):
                            continue
                            
                        if metodo_correcao == "Índice Único":
                            correcao = calcular_correcao_individual(
                                valor_original,
                                data_vencimento,
                                data_referencia,
                                indices_para_calculo[0]
                            )
                        else:
                            correcao = calcular_correcao_media(
                                valor_original,
                                data_vencimento,
                                data_referencia,
                                indices_para_calculo
                            )
                        
                        # Adiciona resultado mantendo todas as colunas originais
                        resultado = dict(row)
                        resultado.update({
                            'Valor Original': valor_original,
                            'Data Vencimento': data_vencimento.strftime("%d/%m/%Y"),
                            'Índice(s)': ', '.join(indices_para_calculo) if metodo_correcao == "Média de Índices" else indices_para_calculo[0],
                            'Fator de Correção': correcao['fator_correcao'],
                            'Variação (%)': correcao['variacao_percentual'],
                            'Valor Corrigido': correcao['valor_corrigido']
                        })
                        resultados.append(resultado)
                        
                    except Exception as e:
                        st.warning(f"Erro ao corrigir linha {idx}: {str(e)}")
                        continue
                
                if resultados:
                    df_resultados = pd.DataFrame(resultados)
                    
                    st.subheader("Resultados da Correção Monetária")
                    st.dataframe(df_resultados.style.format({
                        'Valor Original': format_currency,
                        coluna_valor_original: format_currency,
                        coluna_valor_recebido: format_currency,
                        'Valor Corrigido': format_currency,
                        'Fator de Correção': "{:.8f}",
                        'Variação (%)': "{:.6f}%"
                    }))
                    
                    # Cálculo dos totais
                    st.subheader("Resumo Financeiro")
                    col1, col2, col3 = st.columns(3)
                    
                    total_original = df_resultados['Valor Original'].sum()
                    total_corrigido = df_resultados['Valor Corrigido'].sum()
                    variacao_total = total_corrigido - total_original
                    
                    col1.metric("Total Original", format_currency(total_original))
                    col2.metric("Total Corrigido", format_currency(total_corrigido))
                    col3.metric("Variação Total", format_currency(variacao_total))
                    
                    # Exportação
                    st.subheader("Exportar Resultados")
                    
                    output = pd.ExcelWriter("resultado_correcao.xlsx", engine='xlsxwriter')
                    df_resultados.to_excel(output, index=False)
                    output.close()
                    
                    with open("resultado_correcao.xlsx", "rb") as file:
                        st.download_button(
                            label="Baixar em Excel",
                            data=file,
                            file_name="resultado_correcao.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                else:
                    st.warning("Nenhum valor foi corrigido. Verifique os dados e parâmetros.")
                    
    except Exception as e:
        st.error(f"Erro ao processar o arquivo: {str(e)}")
        st.text("Conteúdo do erro para análise:")
        st.exception(e)


# Rodapé
st.markdown("---")
st.markdown("Desenvolvido por Dev.Alli | Project - © 2025")
