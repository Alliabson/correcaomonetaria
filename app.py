import streamlit as st
import pandas as pd
from datetime import datetime, date
import base64
import pytz
from io import BytesIO

# Configuração
st.set_page_config(page_title="Correção Monetária Debug", layout="wide")
st.title("🔍 Debug - Correção Monetária")

# Importações
try:
    from utils.parser import extract_payment_data
    from utils.indices import (
        get_indices_disponiveis_rapido as get_indices_disponiveis,
        calcular_correcao_rapida as calcular_correcao_individual,
        formatar_moeda,
        limpar_cache
    )
except ImportError as e:
    st.error(f"Erro de importação: {e}")
    st.stop()

# ===== FUNÇÃO DEBUG PARA VISUALIZAR PDF =====
def debug_pdf_content(file_bytes):
    """Função para debug: mostra o conteúdo do PDF"""
    import pdfplumber
    
    st.subheader("🔍 Conteúdo do PDF (Debug)")
    
    try:
        with pdfplumber.open(file_bytes) as pdf:
            all_text = ""
            
            for page_num, page in enumerate(pdf.pages[:3]):  # Apenas 3 páginas para debug
                st.write(f"**Página {page_num + 1}:**")
                
                # Extrair texto
                text = page.extract_text()
                if text:
                    st.text_area(f"Texto Página {page_num + 1}", 
                                text[:2000], 
                                height=200,
                                key=f"text_{page_num}")
                    all_text += text + "\n"
                
                # Extrair tabelas
                try:
                    tables = page.extract_tables()
                    if tables:
                        st.write(f"**Tabelas na página {page_num + 1}:**")
                        for i, table in enumerate(tables):
                            if table:
                                st.write(f"Tabela {i+1}:")
                                df_table = pd.DataFrame(table)
                                st.dataframe(df_table, use_container_width=True)
                except:
                    pass
            
            # Mostrar estatísticas
            st.write("**Estatísticas do PDF:**")
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Páginas", len(pdf.pages))
            col2.metric("Total Caracteres", len(all_text))
            
            # Padrões encontrados
            import re
            padroes = {
                "Datas (dd/mm/yyyy)": len(re.findall(r'\d{2}/\d{2}/\d{4}', all_text)),
                "Valores Monetários": len(re.findall(r'R\$\s*[\d\.,]+|[\d\.,]+\s*(?:reais|R\$)', all_text, re.IGNORECASE)),
                "Parcela/PR.": len(re.findall(r'PR\.\d+/\d+|PARCELA\s+\d+', all_text, re.IGNORECASE)),
            }
            
            for padrao, count in padroes.items():
                col3.metric(padrao, count)
            
            return all_text
            
    except Exception as e:
        st.error(f"Erro no debug: {str(e)}")
        return ""

# ===== INTERFACE PRINCIPAL =====
def main():
    st.sidebar.header("⚙️ Configurações")
    
    # Modo de operação
    modo = st.sidebar.radio("Modo", ["Normal", "Debug PDF"], index=0)
    
    if st.sidebar.button("🗑️ Limpar Cache"):
        limpar_cache()
        st.rerun()
    
    # Configurações de índice
    indice_opcoes = ["IPCA", "IGPM", "INPC", "INCC", "SELIC"]
    indice_selecionado = st.sidebar.selectbox("Índice", indice_opcoes, index=0)
    
    data_ref = st.sidebar.date_input(
        "Data de referência",
        value=datetime.now(pytz.timezone('America/Sao_Paulo')).date(),
        format="DD/MM/YYYY"
    )
    
    # Upload do arquivo
    uploaded_file = st.file_uploader("📄 Envie o PDF ou Excel", 
                                    type=["pdf", "xlsx", "xls"],
                                    help="Suporta PDF e Excel")
    
    if uploaded_file:
        st.success(f"✅ Arquivo carregado: {uploaded_file.name}")
        
        if modo == "Debug PDF" and uploaded_file.name.endswith('.pdf'):
            # Modo debug
            debug_pdf_content(uploaded_file)
            
            # Também tentar extrair dados
            st.subheader("🔄 Tentando extrair dados...")
            try:
                df_parcelas = extract_payment_data(uploaded_file)
                
                if df_parcelas.empty:
                    st.error("❌ Nenhuma parcela extraída pelo parser")
                else:
                    st.success(f"✅ {len(df_parcelas)} parcelas extraídas!")
                    st.dataframe(df_parcelas, use_container_width=True)
                    
                    # Calcular correção se houver dados
                    if st.button("⚡ Calcular Correção", type="primary"):
                        calcular_correcao(df_parcelas, indice_selecionado, data_ref)
            
            except Exception as e:
                st.error(f"❌ Erro no parser: {str(e)}")
        
        else:
            # Modo normal
            with st.spinner("Processando arquivo..."):
                df_parcelas = extract_payment_data(uploaded_file)
                
                if df_parcelas.empty:
                    st.error("❌ Não foi possível extrair parcelas do arquivo")
                    
                    # Sugestões
                    st.info("""
                    **Sugestões:**
                    1. Use o modo **Debug PDF** para ver o conteúdo do arquivo
                    2. Verifique se o arquivo contém dados de parcelas
                    3. Tente converter para Excel e enviar novamente
                    4. O formato esperado é:
                       - Coluna "Parcela" (ex: PR.01/12)
                       - Coluna "Dt Vencim" (ex: 15/01/2023)
                       - Coluna "Valor Parcela" (ex: 1.500,00)
                    """)
                else:
                    st.success(f"✅ {len(df_parcelas)} parcelas extraídas!")
                    
                    # Mostrar preview
                    with st.expander("📋 Visualizar Dados Extraídos", expanded=True):
                        st.dataframe(df_parcelas, use_container_width=True)
                    
                    # Estatísticas
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total Parcelas", len(df_parcelas))
                    col2.metric("Valor Total", formatar_moeda(df_parcelas['Valor Parcela'].sum()))
                    
                    datas_validas = pd.to_datetime(df_parcelas['Dt Vencim'], errors='coerce', dayfirst=True)
                    if not datas_validas.isna().all():
                        col3.metric("Período", 
                                   f"{datas_validas.min().strftime('%d/%m/%Y')} a {datas_validas.max().strftime('%d/%m/%Y')}")
                    
                    # Botão para cálculo
                    if st.button("⚡ Calcular Correção Monetária", type="primary", use_container_width=True):
                        calcular_correcao(df_parcelas, indice_selecionado, data_ref)
    
    else:
        st.info("👆 Faça upload de um arquivo PDF ou Excel para começar")

def calcular_correcao(df_parcelas, indice, data_referencia):
    """Calcula correção monetária"""
    
    with st.spinner("Calculando correções..."):
        resultados = []
        total_parcelas = len(df_parcelas)
        progress_bar = st.progress(0)
        
        for idx, row in df_parcelas.iterrows():
            progress = (idx + 1) / total_parcelas
            progress_bar.progress(progress)
            
            try:
                # Converter data
                data_venc = datetime.strptime(row['Dt Vencim'], "%d/%m/%Y").date()
                valor = float(row['Valor Parcela'])
                
                # Calcular correção
                correcao = calcular_correcao_individual(valor, data_venc, data_referencia, indice)
                
                if correcao['sucesso']:
                    resultados.append({
                        'Parcela': row['Parcela'] if 'Parcela' in row else f"Parcela {idx+1}",
                        'Dt Vencim': row['Dt Vencim'],
                        'Valor Original': valor,
                        'Valor Corrigido': correcao['valor_corrigido'],
                        'Fator Correção': correcao['fator_correcao'],
                        'Variação %': correcao['variacao_percentual'],
                        'Índice': indice,
                        'Status': '✅'
                    })
                else:
                    resultados.append({
                        'Parcela': row['Parcela'] if 'Parcela' in row else f"Parcela {idx+1}",
                        'Dt Vencim': row['Dt Vencim'],
                        'Valor Original': valor,
                        'Valor Corrigido': valor,
                        'Fator Correção': 1.0,
                        'Variação %': 0.0,
                        'Índice': indice,
                        'Status': '❌'
                    })
                    
            except Exception as e:
                st.warning(f"Erro na parcela {idx+1}: {str(e)}")
                continue
        
        progress_bar.empty()
        
        if resultados:
            df_resultados = pd.DataFrame(resultados)
            
            st.subheader("📊 Resultados da Correção")
            
            # Formatar para exibição
            df_display = df_resultados.copy()
            df_display['Valor Original'] = df_display['Valor Original'].apply(formatar_moeda)
            df_display['Valor Corrigido'] = df_display['Valor Corrigido'].apply(formatar_moeda)
            df_display['Fator Correção'] = df_display['Fator Correção'].apply(lambda x: f"{x:.6f}")
            df_display['Variação %'] = df_display['Variação %'].apply(lambda x: f"{x:.2f}%")
            
            st.dataframe(df_display[['Parcela', 'Dt Vencim', 'Valor Original', 'Valor Corrigido', 
                                    'Variação %', 'Status']], 
                        use_container_width=True)
            
            # Resumo
            st.subheader("💰 Resumo Financeiro")
            col1, col2, col3, col4 = st.columns(4)
            
            total_original = df_resultados['Valor Original'].sum()
            total_corrigido = df_resultados['Valor Corrigido'].sum()
            variacao_total = ((total_corrigido - total_original) / total_original) * 100
            sucesso_rate = (df_resultados['Status'] == '✅').mean() * 100
            
            col1.metric("Total Original", formatar_moeda(total_original))
            col2.metric("Total Corrigido", formatar_moeda(total_corrigido))
            col3.metric("Variação Total", f"{variacao_total:.2f}%")
            col4.metric("Taxa de Sucesso", f"{sucesso_rate:.1f}%")
            
            # Exportação
            st.subheader("💾 Exportar Resultados")
            
            # CSV
            csv = df_resultados.to_csv(index=False, sep=';', decimal=',')
            
            # Excel
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_resultados.to_excel(writer, index=False, sheet_name='Correção')
                
                # Adicionar resumo
                resumo_df = pd.DataFrame([{
                    'Total Original': total_original,
                    'Total Corrigido': total_corrigido,
                    'Variação %': variacao_total,
                    'Índice Utilizado': indice,
                    'Data Referência': data_referencia.strftime('%d/%m/%Y'),
                    'Parcelas Processadas': len(resultados),
                    'Taxa de Sucesso': f"{sucesso_rate:.1f}%"
                }])
                resumo_df.to_excel(writer, index=False, sheet_name='Resumo')
            
            col_exp1, col_exp2 = st.columns(2)
            with col_exp1:
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name="correcao_monetaria.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            with col_exp2:
                st.download_button(
                    label="📊 Download Excel",
                    data=output.getvalue(),
                    file_name="correcao_monetaria.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            
            st.balloons()
        else:
            st.error("❌ Não foi possível calcular nenhuma correção")

if __name__ == "__main__":
    main()
