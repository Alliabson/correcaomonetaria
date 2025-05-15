# app.py (atualizado)
import streamlit as st
import pandas as pd
from datetime import datetime
import pytz
from utils.parser import extract_payment_data
from utils.indices import calcular_correcao, get_indices_disponiveis

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
            st.dataframe(parcelas_df)
            
            # Só processa quando o botão for clicado
            if calcular:
                # Aplicar correção monetária
                st.subheader("Correção Monetária Aplicada")
                
                # Copiar dataframe para não modificar o original
                df_corrigido = parcelas_df.copy()
                
                # Aplicar correção para cada parcela
                for idx, row in df_corrigido.iterrows():
                    valor_original = row['Valor Parcela']
                    data_vencimento = row['Dt Vencim']
                    
                    try:
                        # Calcular correção
                        valor_corrigido = calcular_correcao(
                            valor_original=valor_original,
                            data_original=data_vencimento,
                            data_referencia=data_referencia,
                            indices=indices_para_calculo
                        )
                        
                        # Adicionar ao dataframe
                        df_corrigido.at[idx, 'Valor Corrigido'] = valor_corrigido
                        df_corrigido.at[idx, 'Variação (%)'] = ((valor_corrigido - valor_original) / valor_original) * 100
                        df_corrigido.at[idx, 'Variação (R$)'] = valor_corrigido - valor_original
                    
                    except Exception as e:
                        st.error(f"Erro ao corrigir parcela {row['Parcela']}: {str(e)}")
                        continue
                
                # Mostrar resultados
                st.dataframe(df_corrigido)
                
                # Resumo estatístico
                st.subheader("Resumo Estatístico")
                col1, col2, col3 = st.columns(3)
                
                total_original = df_corrigido['Valor Parcela'].sum()
                total_corrigido = df_corrigido['Valor Corrigido'].sum()
                variacao_total = total_corrigido - total_original
                
                col1.metric("Total Original", f"R$ {total_original:,.2f}")
                col2.metric("Total Corrigido", f"R$ {total_corrigido:,.2f}")
                col3.metric("Variação Total", f"R$ {variacao_total:,.2f}")
                
                # Opção para exportar resultados
                st.subheader("Exportar Resultados")
                
                # Converter dataframe para Excel
                output = pd.ExcelWriter("resultado_correcao.xlsx", engine='xlsxwriter')
                df_corrigido.to_excel(output, index=False, sheet_name='Parcelas Corrigidas')
                output.close()
                
                with open("resultado_correcao.xlsx", "rb") as file:
                    st.download_button(
                        label="Baixar Resultados em Excel",
                        data=file,
                        file_name="resultado_correcao.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            
        else:
            st.warning("Nenhuma parcela foi identificada no documento.")
            
    except Exception as e:
        st.error(f"Ocorreu um erro ao processar o arquivo: {str(e)}")
else:
    st.info("Por favor, carregue um arquivo para começar.")

# Rodapé
st.markdown("---")
st.markdown("Desenvolvido por Dev.Alli|Project - © 2023")
