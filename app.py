import streamlit as st
import pandas as pd
from datetime import datetime, date
import pytz
try:
    from utils.parser import extract_payment_data
    from utils.indices import *
except ImportError:
    # Força o caminho absoluto para desenvolvimento local
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from utils.parser import extract_payment_data
    from utils.indices import *

# Configuração da página
st.set_page_config(page_title="Correção Monetária de Relatórios", layout="wide")

# Título e descrição
st.title("📈 Correção Monetária de Relatórios")
st.markdown("""
Aplicativo para correção monetária de valores de parcelas em relatórios financeiros.
Carregue um relatório no formato similar ao exemplo e selecione os índices para correção.
""")

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
        parcelas_df = extract_payment_data(uploaded_file)
        
        if not parcelas_df.empty:
            st.success("Dados extraídos com sucesso!")
            
            # Mostrar dados brutos
            st.subheader("Parcelas Identificadas")
            st.dataframe(parcelas_df.style.format({
                'Valor Parcela': lambda x: formatar_moeda(x)
            }))
            
            # Só processa quando o botão for clicado
            if calcular:
                # Aplicar correção monetária
                st.subheader("Correção Monetária Aplicada")
                
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
            st.warning("Nenhuma parcela foi identificada no documento.")
            
    except Exception as e:
        st.error(f"Ocorreu um erro ao processar o arquivo: {str(e)}")
else:
    st.info("Por favor, carregue um arquivo para começar.")

# Rodapé
st.markdown("---")
st.markdown("Desenvolvido por Dev.Alli | Project - © 2025")
