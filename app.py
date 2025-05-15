import streamlit as st
import pandas as pd
from datetime import datetime, date
import pytz
import sys
import os
import locale

# Configuração de locale com fallback seguro
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil.1252')
    except locale.Error:
        try:
            locale.setlocale(locale.LC_ALL, 'pt_BR')
        except locale.Error:
            # Fallback para locale padrão do sistema
            locale.setlocale(locale.LC_ALL, '')
            st.warning("Locale específico não disponível. Usando configuração padrão do sistema.")

# Configura caminhos para imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Imports com tratamento de erro detalhado
try:
    from utils.parser import extract_payment_data
    from utils.indices import get_indices_disponiveis, calcular_correcao_individual, calcular_correcao_media
except ImportError as e:
    st.error(f"""
    **Erro crítico ao importar módulos:** {str(e)}
    
    Por favor, verifique:
    1. A pasta `utils` existe no mesmo diretório que `app.py`
    2. Os arquivos `parser.py` e `indices.py` estão dentro da pasta `utils`
    3. Há um arquivo `__init__.py` dentro da pasta `utils`
    """)
    st.stop()

# Configuração da página
st.set_page_config(
    page_title="Correção Monetária de Relatórios", 
    layout="wide",
    page_icon="📈"
)

# Funções auxiliares
def formatar_moeda(valor: Union[float, str]) -> str:
    """Formata valores monetários para exibição"""
    try:
        if pd.isna(valor) or valor == '':
            return "R$ 0,00"
        
        valor_float = float(valor)
        return locale.currency(valor_float, grouping=True, symbol=True)
    except Exception as e:
        st.warning(f"Erro ao formatar valor {valor}: {str(e)}")
        return "R$ 0,00"

def mostrar_resumo(df: pd.DataFrame):
    """Exibe um resumo estatístico do DataFrame"""
    if df.empty:
        return
    
    with st.expander("🔍 Resumo Estatístico", expanded=True):
        cols = st.columns(4)
        
        total_parcelas = len(df)
        total_pago = df[df['Status Pagamento'] == 'Pago'].shape[0]
        total_pendente = total_parcelas - total_pago
        valor_total = df['Valor Parcela'].sum()
        
        cols[0].metric("Total de Parcelas", total_parcelas)
        cols[1].metric("Parcelas Pagas", f"{total_pago} ({total_pago/total_parcelas:.1%})")
        cols[2].metric("Parcelas Pendentes", f"{total_pendente} ({total_pendente/total_parcelas:.1%})")
        cols[3].metric("Valor Total", formatar_moeda(valor_total))
        
        # Gráfico de status
        status_df = pd.DataFrame({
            'Status': ['Pagas', 'Pendentes'],
            'Quantidade': [total_pago, total_pendente]
        })
        
        st.bar_chart(status_df.set_index('Status'))

def mostrar_tabela_parcelas(df: pd.DataFrame):
    """Exibe a tabela de parcelas com formatação"""
    if df.empty:
        st.warning("Nenhuma parcela encontrada para exibição.")
        return
    
    # Seleciona e ordena colunas
    cols_to_show = [
        'Parcela', 'Dt Vencim', 'Valor Parcela', 'Status Pagamento',
        'Dt Recebimento', 'Valor Recebido', 'Dias Atraso', 'Valor Pendente'
    ]
    
    # Filtra colunas existentes
    cols_to_show = [col for col in cols_to_show if col in df.columns]
    
    st.dataframe(
        df[cols_to_show].style.format({
            'Dt Vencim': lambda x: x.strftime('%d/%m/%Y') if not pd.isna(x) else '',
            'Dt Recebimento': lambda x: x.strftime('%d/%m/%Y') if not pd.isna(x) else '',
            'Valor Parcela': formatar_moeda,
            'Valor Recebido': formatar_moeda,
            'Valor Pendente': formatar_moeda,
            'Dias Atraso': lambda x: f"{int(x)} dias" if x > 0 else ""
        }),
        height=600,
        use_container_width=True
    )

def processar_correcao(df: pd.DataFrame, indices: List[str], data_ref: date, metodo: str) -> pd.DataFrame:
    """Aplica correção monetária às parcelas"""
    resultados = []
    
    for _, row in df.iterrows():
        try:
            valor_original = row['Valor Parcela']
            data_vencimento = row['Dt Vencim']
            
            if pd.isna(data_vencimento):
                continue
            
            # Converter para date se for datetime
            if isinstance(data_vencimento, pd.Timestamp):
                data_vencimento = data_vencimento.date()
            
            if metodo == "Índice Único":
                correcao = calcular_correcao_individual(
                    valor_original,
                    data_vencimento,
                    data_ref,
                    indices[0]
                )
            else:
                correcao = calcular_correcao_media(
                    valor_original,
                    data_vencimento,
                    data_ref,
                    indices
                )
            
            resultados.append({
                'Parcela': row['Parcela'],
                'Dt Vencim': data_vencimento.strftime("%d/%m/%Y"),
                'Valor Original': valor_original,
                'Índice(s)': ', '.join(indices) if metodo == "Média de Índices" else indices[0],
                'Fator de Correção': correcao['fator_correcao'],
                'Variação (%)': correcao['variacao_percentual'],
                'Valor Corrigido': correcao['valor_corrigido']
            })
        
        except Exception as e:
            st.error(f"Erro ao corrigir parcela {row['Parcela']}: {str(e)}")
            continue
    
    return pd.DataFrame(resultados) if resultados else pd.DataFrame()

def exportar_resultados(df: pd.DataFrame):
    """Exporta resultados para Excel"""
    if df.empty:
        st.warning("Nenhum dado para exportar")
        return
    
    try:
        output_path = "resultado_correcao.xlsx"
        df.to_excel(output_path, index=False, engine='openpyxl')
        
        with open(output_path, "rb") as file:
            st.download_button(
                label="📥 Baixar Resultados em Excel",
                data=file,
                file_name="resultado_correcao.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Clique para baixar os resultados da correção monetária"
            )
    except Exception as e:
        st.error(f"Erro ao exportar resultados: {str(e)}")

# Interface principal
def main():
    st.title("📈 Correção Monetária de Relatórios")
    st.markdown("""
    Aplicativo para correção monetária de valores de parcelas em relatórios financeiros.
    Carregue um relatório (PDF ou Excel) no formato similar ao exemplo e selecione os índices para correção.
    """)
    
    # Upload do arquivo
    uploaded_file = st.file_uploader(
        "Carregue seu relatório (PDF ou Excel)",
        type=["pdf", "xlsx", "xls"],
        accept_multiple_files=False,
        help="Selecione um arquivo PDF ou Excel contendo as parcelas a serem corrigidas"
    )
    
    # Seção de configuração da correção
    with st.sidebar:
        st.header("⚙️ Configurações de Correção")
        
        # Seleção do método de correção
        metodo_correcao = st.radio(
            "Método de Correção",
            options=["Índice Único", "Média de Índices"],
            index=0,
            help="Selecione se deseja usar um único índice ou a média de vários índices"
        )
        
        indices_disponiveis = get_indices_disponiveis()
        
        if metodo_correcao == "Índice Único":
            indice_selecionado = st.selectbox(
                "Selecione o índice econômico",
                options=list(indices_disponiveis.keys()),
                index=0,
                help="Índice que será usado para a correção monetária"
            )
            indices_para_calculo = [indice_selecionado]
        else:
            indices_selecionados = st.multiselect(
                "Selecione os índices para cálculo da média",
                options=list(indices_disponiveis.keys()),
                default=["IGPM", "IPCA", "INCC"],
                help="Selecione pelo menos 2 índices para calcular a média"
            )
            indices_para_calculo = indices_selecionados if len(indices_selecionados) >= 2 else ["IGPM", "IPCA", "INCC"]
            if len(indices_para_calculo) < 2:
                st.warning("Selecione pelo menos 2 índices para calcular a média")
        
        # Data de referência para correção
        data_referencia = st.date_input(
            "Data de referência para correção",
            value=datetime.now(pytz.timezone('America/Sao_Paulo')).date(),
            format="DD/MM/YYYY",
            help="Data para a qual os valores serão corrigidos"
        )
        
        # Botão para executar a correção
        calcular = st.button(
            "Calcular Correção",
            type="primary",
            help="Clique para processar a correção monetária"
        )
    
    # Processamento do arquivo
    if uploaded_file is not None:
        try:
            with st.spinner("Processando arquivo..."):
                parcelas_df = extract_payment_data(uploaded_file)
            
            if not parcelas_df.empty:
                st.success("✅ Dados extraídos com sucesso!")
                
                # Mostrar dados brutos
                st.subheader("📋 Parcelas Identificadas")
                mostrar_resumo(parcelas_df)
                mostrar_tabela_parcelas(parcelas_df)
                
                # Processar correção monetária
                if calcular and indices_para_calculo:
                    with st.spinner("Aplicando correção monetária..."):
                        df_resultados = processar_correcao(
                            parcelas_df,
                            indices_para_calculo,
                            data_referencia,
                            metodo_correcao
                        )
                    
                    if not df_resultados.empty:
                        st.subheader("📊 Resultados da Correção Monetária")
                        
                        # Mostrar resultados formatados
                        st.dataframe(
                            df_resultados.style.format({
                                'Valor Original': formatar_moeda,
                                'Valor Corrigido': formatar_moeda,
                                'Fator de Correção': "{:.8f}",
                                'Variação (%)': "{:.6f}%"
                            }),
                            height=600,
                            use_container_width=True
                        )
                        
                        # Resumo estatístico
                        st.subheader("🧮 Resumo Financeiro")
                        col1, col2, col3 = st.columns(3)
                        
                        total_original = df_resultados['Valor Original'].sum()
                        total_corrigido = df_resultados['Valor Corrigido'].sum()
                        variacao_total = total_corrigido - total_original
                        
                        col1.metric("Total Original", formatar_moeda(total_original))
                        col2.metric("Total Corrigido", formatar_moeda(total_corrigido))
                        col3.metric("Variação Total", formatar_moeda(variacao_total), 
                                  f"{variacao_total/total_original:.2%}")
                        
                        # Exportar resultados
                        st.subheader("💾 Exportar Resultados")
                        exportar_resultados(df_resultados)
                    else:
                        st.warning("Nenhum resultado de correção foi gerado.")
            else:
                st.warning("⚠️ Nenhuma parcela foi identificada no documento.")
        
        except Exception as e:
            st.error(f"❌ Ocorreu um erro ao processar o arquivo: {str(e)}")
            st.error("Por favor, verifique o formato do arquivo e tente novamente.")
    else:
        st.info("ℹ️ Por favor, carregue um arquivo para começar.")
    
    # Rodapé
    st.markdown("---")
    st.markdown("Desenvolvido por Dev.Alli | Project - © 2025")

if __name__ == "__main__":
    main()
