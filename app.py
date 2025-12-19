import streamlit as st
import pdfplumber
import pandas as pd
import re
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
import base64
import pytz
from io import BytesIO

# Configuração
st.set_page_config(page_title="Correção Monetária Rápida", layout="wide")
st.title("⚡ Correção Monetária Rápida")

# Otimização: desabilitar warnings do PDF
import warnings
warnings.filterwarnings('ignore')

# Importações otimizadas
try:
    from utils.indices import (
        get_indices_disponiveis_rapido as get_indices_disponiveis,
        calcular_correcao_rapida as calcular_correcao_individual,
        calcular_correcao_multipla_rapida,
        formatar_moeda,
        limpar_cache
    )
except ImportError as e:
    st.error(f"Erro de importação: {e}")
    st.stop()

# ===== CLASSES SIMPLIFICADAS =====
class Parcela:
    def __init__(self, codigo: str, data_vencimento: str, valor: float):
        self.codigo = codigo
        self.data_vencimento = data_vencimento
        self.valor = valor

# ===== PROCESSAMENTO PDF OTIMIZADO =====
@st.cache_data(ttl=300, show_spinner=False)
def processar_pdf_rapido(file_bytes):
    """Processa PDF de forma otimizada"""
    parcelas = []
    
    try:
        with pdfplumber.open(file_bytes) as pdf:
            # Ler apenas primeira página inicialmente (mais rápido)
            primeira_pagina = pdf.pages[0]
            texto = primeira_pagina.extract_text() or ""
            
            # Padrão otimizado para parcelas
            padrao = r'(PR\.\d+/\d+|PARCELA\s+\d+)\s+(\d{2}/\d{2}/\d{4})\s+([\d\.]+,\d{2})'
            
            for match in re.finditer(padrao, texto):
                codigo = match.group(1)
                data_str = match.group(2)
                valor_str = match.group(3).replace('.', '').replace(',', '.')
                
                try:
                    valor = float(valor_str)
                    parcelas.append(Parcela(codigo, data_str, valor))
                except:
                    continue
            
            # Se não encontrou na primeira página, tenta as outras
            if not parcelas and len(pdf.pages) > 1:
                for pagina in pdf.pages[1:3]:  # Apenas 3 páginas
                    texto_pag = pagina.extract_text()
                    if texto_pag:
                        for match in re.finditer(padrao, texto_pag):
                            codigo = match.group(1)
                            data_str = match.group(2)
                            valor_str = match.group(3).replace('.', '').replace(',', '.')
                            
                            try:
                                valor = float(valor_str)
                                parcelas.append(Parcela(codigo, data_str, valor))
                            except:
                                continue
                    
                    if parcelas:  # Parar se encontrou
                        break
    
    except Exception as e:
        st.error(f"Erro no PDF: {str(e)}")
    
    return parcelas

# ===== INTERFACE OTIMIZADA =====
def render_sidebar_rapida():
    st.sidebar.header("⚙️ Configurações")
    
    if st.sidebar.button("🗑️ Limpar Cache", use_container_width=True):
        limpar_cache()
        st.rerun()
    
    # Verificação rápida de índices
    with st.sidebar.expander("📈 Índices Disponíveis", expanded=True):
        try:
            indices = get_indices_disponiveis()
            for nome, status in indices.items():
                icon = "✅" if status['disponivel'] else "❌"
                st.write(f"{icon} **{nome}** - {status['ultima_data']}")
        except:
            st.write("⚠️ Verificando...")
    
    # Configurações simples
    indice_opcoes = ["IPCA", "IGPM", "INPC", "INCC", "SELIC"]
    indice_selecionado = st.sidebar.selectbox("Índice", indice_opcoes, index=0)
    
    data_ref = st.sidebar.date_input(
        "Data de referência",
        value=datetime.now(pytz.timezone('America/Sao_Paulo')).date(),
        format="DD/MM/YYYY"
    )
    
    return {
        "indice": indice_selecionado,
        "data_referencia": data_ref
    }

def calcular_correcoes_em_lote(parcelas, config):
    """Calcula correções em lote (OTIMIZADO)"""
    
    # Preparar dados para cálculo em lote
    valores_datas = []
    parcelas_info = []
    
    for parcela in parcelas:
        try:
            data_venc = datetime.strptime(parcela.data_vencimento, "%d/%m/%Y").date()
            valores_datas.append((parcela.valor, data_venc))
            parcelas_info.append((parcela.codigo, parcela.data_vencimento, parcela.valor, data_venc))
        except:
            continue
    
    if not valores_datas:
        return []
    
    # Usar cálculo em lote otimizado
    resultados = calcular_correcao_multipla_rapida(
        valores_datas, 
        config["data_referencia"], 
        config["indice"]
    )
    
    # Combinar resultados
    dados_finais = []
    for (codigo, data_str, valor_orig, data_venc), resultado in zip(parcelas_info, resultados):
        if resultado['sucesso']:
            dados_finais.append({
                'Parcela': codigo,
                'Dt Vencim': data_str,
                'Valor Original': valor_orig,
                'Valor Corrigido': resultado['corrigido'],
                'Fator Correção': resultado['fator'],
                'Variação %': (resultado['fator'] - 1) * 100
            })
    
    return dados_finais

def main_rapida():
    config = render_sidebar_rapida()
    
    # Upload otimizado
    uploaded_file = st.file_uploader("📄 Envie o PDF", type="pdf", help="Arquivos PDF de relatórios")
    
    if uploaded_file:
        # Barra de progresso simples
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        status_text.text("📖 Lendo PDF...")
        progress_bar.progress(20)
        
        # Processar PDF (com cache)
        parcelas = processar_pdf_rapido(uploaded_file)
        
        if not parcelas:
            st.error("❌ Não foi possível extrair parcelas do PDF")
            return
        
        status_text.text(f"📊 {len(parcelas)} parcelas encontradas")
        progress_bar.progress(40)
        
        # Mostrar preview rápido
        with st.expander("📋 Pré-visualização das Parcelas", expanded=True):
            preview_data = []
            for parcela in parcelas[:10]:  # Apenas 10 para preview
                preview_data.append({
                    'Parcela': parcela.codigo,
                    'Vencimento': parcela.data_vencimento,
                    'Valor': formatar_moeda(parcela.valor)
                })
            
            if preview_data:
                st.table(pd.DataFrame(preview_data))
            
            if len(parcelas) > 10:
                st.caption(f"... e mais {len(parcelas) - 10} parcelas")
        
        # Botão de cálculo
        if st.button("⚡ Calcular Correção", type="primary", use_container_width=True):
            status_text.text("🧮 Calculando correções...")
            progress_bar.progress(60)
            
            # Cálculo em lote otimizado
            resultados = calcular_correcoes_em_lote(parcelas, config)
            
            progress_bar.progress(80)
            
            if resultados:
                df_resultados = pd.DataFrame(resultados)
                
                status_text.text("📊 Gerando relatório...")
                progress_bar.progress(90)
                
                # Mostrar resultados
                st.subheader("📈 Resultados da Correção")
                
                # Formatar valores
                df_display = df_resultados.copy()
                df_display['Valor Original'] = df_display['Valor Original'].apply(formatar_moeda)
                df_display['Valor Corrigido'] = df_display['Valor Corrigido'].apply(formatar_moeda)
                df_display['Fator Correção'] = df_display['Fator Correção'].apply(lambda x: f"{x:.6f}")
                df_display['Variação %'] = df_display['Variação %'].apply(lambda x: f"{x:.2f}%")
                
                st.dataframe(df_display, use_container_width=True)
                
                # Resumo rápido
                st.subheader("💰 Resumo Financeiro")
                col1, col2, col3 = st.columns(3)
                
                total_original = df_resultados['Valor Original'].sum()
                total_corrigido = df_resultados['Valor Corrigido'].sum()
                variacao_total = ((total_corrigido - total_original) / total_original) * 100
                
                col1.metric("Total Original", formatar_moeda(total_original))
                col2.metric("Total Corrigido", formatar_moeda(total_corrigido))
                col3.metric("Variação Total", f"{variacao_total:.2f}%")
                
                # Exportação rápida
                st.subheader("💾 Exportar Resultados")
                
                # CSV
                csv = df_resultados.to_csv(index=False, sep=';', decimal=',')
                b64_csv = base64.b64encode(csv.encode()).decode()
                
                # Excel
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_resultados.to_excel(writer, index=False)
                b64_xlsx = base64.b64encode(output.getvalue()).decode()
                
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
                
                progress_bar.progress(100)
                status_text.text("✅ Cálculo concluído!")
                st.balloons()
            else:
                st.error("❌ Não foi possível calcular as correções")
                progress_bar.progress(100)
    
    else:
        st.info("👆 Faça upload de um arquivo PDF para começar")

# Executar app
if __name__ == "__main__":
    main_rapida()
