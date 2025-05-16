import streamlit as st
from parser import PDFParser
import pandas as pd

def main():
    st.set_page_config(
        page_title="Extrator de PDF Financeiro",
        layout="wide",
        page_icon="📄"
    )

    st.title("📄 Extrator de Demonstrativos Financeiros")
    st.markdown("""
    Carregue um arquivo PDF para extrair os dados de parcelas, clientes e resumo financeiro.
    """)

    # Upload do arquivo
    uploaded_file = st.file_uploader(
        "Selecione o arquivo PDF",
        type=["pdf"],
        help="Arquivos no formato de demonstrativo financeiro"
    )

    if uploaded_file:
        parser = PDFParser()
        
        with st.spinner("Processando PDF..."):
            try:
                # Extrai todos os dados do PDF
                extracted_data = parser.extract_data(uploaded_file)
                
                # Converte para DataFrame
                df = parser.to_dataframe(extracted_data)
                
                if not df.empty:
                    st.success("✅ Dados extraídos com sucesso!")
                    
                    # Mostra abas com diferentes visualizações
                    tab1, tab2, tab3 = st.tabs(["Parcelas", "Dados do Cliente", "Resumo Financeiro"])
                    
                    with tab1:
                        st.subheader("Todas as Parcelas")
                        st.dataframe(df)
                        
                        # Opção para exportar
                        csv = df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Exportar para CSV",
                            data=csv,
                            file_name="parcelas.csv",
                            mime="text/csv"
                        )
                    
                    with tab2:
                        st.subheader("Dados do Cliente")
                        client_data = extracted_data.get('client_data', {})
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("Nome", client_data.get('name', '-'))
                            st.metric("Contrato", client_data.get('contract_number', '-'))
                            st.metric("Empreendimento", client_data.get('project', '-'))
                            st.metric("Endereço", client_data.get('address', '-'))
                        
                        with col2:
                            st.metric("Cidade/UF", f"{client_data.get('city', '-')}/{client_data.get('state', '-')}")
                            st.metric("CEP", client_data.get('zip_code', '-'))
                            st.metric("Telefone", client_data.get('phone', '-'))
                            st.metric("Valor do Contrato", client_data.get('contract_value', '-'))
                    
                    with tab3:
                        st.subheader("Resumo Financeiro")
                        
                        # Totais por ano
                        st.markdown("**Totais por Ano**")
                        yearly_totals = extracted_data.get('yearly_totals', {})
                        if yearly_totals:
                            yearly_df = pd.DataFrame(yearly_totals).T
                            st.dataframe(yearly_df.style.format({
                                'paid': "{:,.2f}",
                                'to_pay': "{:,.2f}"
                            }))
                        
                        # Resumo geral
                        st.markdown("**Resumo Geral**")
                        summary = extracted_data.get('summary', {})
                        if summary:
                            col1, col2, col3, col4 = st.columns(4)
                            col1.metric("Total Recebido", summary.get('total_received', '-'))
                            col2.metric("Total a Receber", summary.get('total_to_receive', '-'))
                            col3.metric("% Pago", summary.get('paid_percentage', '-'))
                            col4.metric("% a Receber", summary.get('to_receive_percentage', '-'))
                
                else:
                    st.warning("Nenhuma parcela foi identificada no documento.")
            
            except Exception as e:
                st.error(f"Erro ao processar o arquivo: {str(e)}")

if __name__ == "__main__":
    main()
