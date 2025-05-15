import streamlit as st
import pandas as pd
from datetime import datetime, date
import pytz
import pdfplumber
import re
from utils.indices import *

# Configuração da página
st.set_page_config(page_title="Correção Monetária de Relatórios", layout="wide")

# Título e descrição
st.title("📈 Correção Monetária de Relatórios")
st.markdown("""
Aplicativo para correção monetária de valores de parcelas em relatórios financeiros.
Carregue um relatório no formato similar ao exemplo e selecione os índices para correção.
""")

# Funções de extração de dados diretamente no app.py
def extract_payment_data(file):
    """Função principal para extração de dados"""
    if file.name.lower().endswith('.pdf'):
        return extract_from_pdf(file)
    elif file.name.lower().endswith(('.xls', '.xlsx')):
        return extract_from_excel(file)
    else:
        raise ValueError("Formato de arquivo não suportado")

def extract_from_pdf(pdf_file):
    """Extrai dados do PDF com parser robusto"""
    parcelas = []
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
                
            lines = text.split('\n')
            in_payment_section = False
            
            for line in lines:
                # Identifica o início da seção de pagamentos
                if "Parcela" in line and ("DI Veném" in line or "Dt Vencim" in line) and "Valor Parc." in line:
                    in_payment_section = True
                    continue
                
                if in_payment_section:
                    # Padrão robusto para identificar parcelas (E., P., PR., BR., SM., etc.)
                    if re.match(r'^(E|P|PR|BR|SM)\.\d+/\d+', line.strip()):
                        try:
                            # Processa a linha usando espaços como delimitadores
                            parts = re.split(r'\s{2,}', line.strip())
                            
                            # Extrai informações básicas (com tratamento de erro para índices)
                            parcela = parts[0] if len(parts) > 0 else ""
                            dt_vencim = parse_date(parts[1] if len(parts) > 1 else "")
                            valor_parc = parse_currency(parts[2] if len(parts) > 2 else "0")
                            
                            # Data e valor de recebimento (opcionais)
                            dt_receb = None
                            valor_recebido = 0.0
                            
                            # Procura por padrão de data (dd/mm/aaaa)
                            for i, part in enumerate(parts[3:], 3):
                                if re.match(r'\d{2}/\d{2}/\d{4}', str(part)):
                                    dt_receb = parse_date(part)
                                    if i+1 < len(parts):
                                        valor_recebido = parse_currency(parts[i+1])
                                    break
                            
                            # Adiciona à lista de parcelas
                            parcelas.append({
                                'Parcela': parcela,
                                'Dt Vencim': dt_vencim,
                                'Valor Parcela': valor_parc,
                                'Dt Recebimento': dt_receb,
                                'Valor Recebido': valor_recebido,
                                'Status Pagamento': 'Pago' if valor_recebido > 0 else 'Pendente',
                                'Arquivo Origem': pdf_file.name
                            })
                            
                        except Exception as e:
                            st.warning(f"Erro ao processar linha: {line}. Erro: {str(e)}")
                            continue
                    
                    # Finaliza quando encontrar o total
                    if "Total a pagar:" in line or "TOTAL GERAL:" in line:
                        break
    
    # Cria DataFrame com tratamento seguro
    df = pd.DataFrame(parcelas)
    
    if not df.empty:
        # Calcula dias de atraso apenas para parcelas pagas
        df['Dias Atraso'] = df.apply(
            lambda x: (x['Dt Recebimento'] - x['Dt Vencim']).days 
            if pd.notna(x['Dt Recebimento']) and x['Dt Recebimento'] > x['Dt Vencim']
            else 0,
            axis=1
        )
        
        df['Valor Pendente'] = df['Valor Parcela'] - df['Valor Recebido']
    
    return df

def parse_date(date_str):
    """Converte string de data para objeto date"""
    try:
        date_str = str(date_str).strip()
        if not date_str or date_str.lower() == 'nan':
            return None
        
        # Remove qualquer texto adicional (como "Atraso")
        date_str = re.sub(r'[^0-9/]', '', date_str)
        
        # Tenta vários formatos de data
        for fmt in ['%d/%m/%Y', '%d%m/%Y', '%d-%m-%Y']:
            try:
                # Remove barras extras para formato %d%m/%Y
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

def parse_currency(value_str):
    """Converte valores monetários para float"""
    try:
        value_str = str(value_str).strip()
        if not value_str or value_str.lower() == 'nan':
            return 0.0
            
        # Remove R$ e espaços
        value_str = re.sub(r'[^\d,-.]', '', value_str)
        
        # Caso tenha tanto ponto quanto vírgula (1.234,56)
        if '.' in value_str and ',' in value_str:
            return float(value_str.replace('.', '').replace(',', '.'))
        # Caso tenha apenas vírgula (1234,56)
        elif ',' in value_str:
            return float(value_str.replace(',', '.'))
        # Caso tenha apenas ponto (1234.56 ou 1.234)
        else:
            return float(value_str)
    except:
        return 0.0

def formatar_moeda(valor):
    """Formata valores monetários para exibição"""
    try:
        return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "R$ 0,00"

# Upload do arquivo
uploaded_file = st.file_uploader("Carregue seu relatório (PDF ou Excel)", type=["pdf", "xlsx", "xls"])

# Seção de configuração da correção
st.sidebar.header("Configurações de Correção")

# Seleção do método de correção
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
    st.sidebar.info("Selecione pelo menos 2 índices para calcular a média.")

# Data de referência para correção (formatada em PT-BR)
data_referencia = st.sidebar.date_input(
    "Data de referência para correção",
    value=datetime.now(pytz.timezone('America/Sao_Paulo')).date(),
    format="DD/MM/YYYY"
)

# Botão para executar a simulação
calcular = st.sidebar.button("Calcular Correção", type="primary")

if uploaded_file is not None:
    try:
        # Extrair dados do arquivo
        with st.spinner("Processando arquivo..."):
            parcelas_df = extract_payment_data(uploaded_file)
        
        if not parcelas_df.empty:
            st.success(f"{len(parcelas_df)} parcelas identificadas com sucesso!")
            
            # Mostrar dados brutos
            st.subheader("Visualização das Parcelas")
            st.dataframe(parcelas_df.style.format({
                'Valor Parcela': formatar_moeda,
                'Valor Recebido': formatar_moeda,
                'Valor Pendente': formatar_moeda
            }))
            
            # Só processa quando o botão for clicado
            if calcular:
                # Aplicar correção monetária
                st.subheader("Resultado da Correção Monetária")
                
                # Lista para armazenar resultados
                resultados = []
                
                for idx, row in parcelas_df.iterrows():
                    valor_original = row['Valor Parcela']
                    data_vencimento = row['Dt Vencim']
                    
                    try:
                        # Converter para date se for datetime
                        if isinstance(data_vencimento, pd.Timestamp):
                            data_vencimento = data_vencimento.date()
                        
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
                        
                        # Adicionar ao dataframe de resultados
                        resultados.append({
                            'Parcela': row['Parcela'],
                            'Dt Vencim': data_vencimento.strftime("%d/%m/%Y"),
                            'Valor Original': valor_original,
                            'Índice(s)': ', '.join(indices_para_calculo) if metodo_correcao == "Média de Índices" else indices_para_calculo[0],
                            'Fator de Correção': correcao['fator_correcao'],
                            'Variação (%)': correcao['variacao_percentual'],
                            'Valor Corrigido': correcao['valor_corrigido']
                        })
                    
                    except Exception as e:
                        st.error(f"Erro ao corrigir parcela {row['Parcela']}: {str(e)}")
                        continue
                
                if resultados:
                    # Criar DataFrame com resultados
                    df_resultados = pd.DataFrame(resultados)
                    
                    # Mostrar resultados formatados
                    st.dataframe(df_resultados.style.format({
                        'Valor Original': formatar_moeda,
                        'Fator de Correção': "{:.8f}",
                        'Variação (%)': "{:.6f}%",
                        'Valor Corrigido': formatar_moeda
                    }))
                    
                    # Resumo estatístico
                    st.subheader("Resumo Estatístico")
                    col1, col2, col3 = st.columns(3)
                    
                    total_original = df_resultados['Valor Original'].sum()
                    total_corrigido = df_resultados['Valor Corrigido'].sum()
                    variacao_total = total_corrigido - total_original
                    
                    col1.metric("Total Original", formatar_moeda(total_original))
                    col2.metric("Total Corrigido", formatar_moeda(total_corrigido))
                    col3.metric("Variação Total", formatar_moeda(variacao_total))
                    
                    # Opção para exportar resultados
                    st.subheader("Exportar Resultados")
                    
                    # Converter dataframe para Excel
                    output = pd.ExcelWriter("resultado_correcao.xlsx", engine='xlsxwriter')
                    df_resultados.to_excel(output, index=False, sheet_name='Parcelas Corrigidas')
                    output.close()
                    
                    with open("resultado_correcao.xlsx", "rb") as file:
                        st.download_button(
                            label="Baixar Resultados em Excel",
                            data=file,
                            file_name="resultado_correcao.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                else:
                    st.warning("Nenhuma parcela foi corrigida com sucesso.")
            
        else:
            st.warning("Nenhuma parcela foi identificada no documento. Verifique o formato do arquivo.")
            
    except Exception as e:
        st.error(f"Ocorreu um erro ao processar o arquivo: {str(e)}")
        st.info("Dica: Verifique se o documento contém a tabela de parcelas no formato esperado.")
else:
    st.info("Por favor, carregue um arquivo para começar.")

# Rodapé
st.markdown("---")
st.markdown("Desenvolvido por Dev.Alli | Project - © 2025")
