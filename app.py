import streamlit as st
import pdfplumber
import pandas as pd
import re
import requests
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
import base64
import pytz
from dateutil.relativedelta import relativedelta
from io import BytesIO
import time

# Configuração da página
st.set_page_config(page_title="Correção Monetária Completa", layout="wide")
st.title("📈 Correção Monetária Completa")

# Importações do módulo de índices
try:
    from utils.indices import (
        get_indices_disponiveis,
        calcular_correcao_individual,
        calcular_correcao_media,
        formatar_moeda,
        limpar_cache
    )
except ImportError as e:
    st.error(f"Erro ao importar módulos: {e}. Verifique se requirements.txt foi instalado.")
    st.stop()

# ===== Classes para modelagem dos dados =====
class Cliente:
    def __init__(self, codigo: str = "", nome: str = ""):
        self.codigo = codigo
        self.nome = nome

class Venda:
    def __init__(self, numero: str = "", data: str = "", valor: float = 0.0):
        self.numero = numero
        self.data = data
        self.valor = valor

class Parcela:
    def __init__(self, codigo: str = "", data_vencimento: str = "", valor_original: float = 0.0, 
                 data_recebimento: Optional[str] = None, valor_pago: float = 0.0):
        self.codigo = codigo
        self.data_vencimento = data_vencimento
        self.valor_original = valor_original
        self.data_recebimento = data_recebimento
        self.valor_pago = valor_pago
    
    def to_dict(self):
        return {
            "Parcela": self.codigo,
            "Dt Vencim": self.data_vencimento,
            "Valor Original": self.valor_original,
            "Dt Receb": self.data_recebimento if self.data_recebimento else "",
            "Valor Pago": self.valor_pago
        }

# ===== Funções de utilidade =====
def parse_date(date_str: str) -> Optional[date]:
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").date()
    except:
        return None

def parse_monetary(value: str) -> float:
    if not value or value.strip() == "":
        return 0.0
    try:
        return float(value.replace('.', '').replace(',', '.'))
    except:
        return 0.0

# ===== Componentes =====
def FileUploader() -> Optional[bytes]:
    return st.file_uploader("Carregue seu arquivo PDF", type="pdf")

def InfoBox(title: str, value: str, color: str = "blue"):
    colors = {
        "blue": ("#1E88E5", "#E3F2FD"),
        "green": ("#43A047", "#E8F5E9"),
        "yellow": ("#FFB300", "#FFF8E1")
    }
    bg_color = colors.get(color, colors["blue"])[1]
    text_color = colors.get(color, colors["blue"])[0]
    
    st.markdown(
        f"""
        <div style="background-color: {bg_color}; padding: 1rem; border-radius: 0.5rem; margin-bottom: 1rem;">
            <h3 style="color: {text_color}; margin: 0 0 0.5rem 0;">{title}</h3>
            <p style="font-size: 1.5rem; font-weight: bold; color: {text_color}; margin: 0;">{value}</p>
        </div>
        """,
        unsafe_allow_html=True
    )

# ===== Processamento do PDF =====
class PDFProcessor:
    def __init__(self):
        self.cliente = Cliente()
        self.venda = Venda()
        self.parcelas: List[Parcela] = []
        self.total_recebido: float = 0.0
        self.total_original: float = 0.0
    
    def process_pdf(self, file: bytes) -> bool:
        try:
            with pdfplumber.open(file) as pdf:
                full_text = ""
                for page in pdf.pages:
                    full_text += page.extract_text() or ""
            
            self._extract_cliente(full_text)
            self._extract_venda(full_text)
            self._extract_parcelas(full_text)
            self._calculate_totais()
            return True
        except Exception as e:
            st.error(f"Erro ao processar o PDF: {str(e)}")
            return False
    
    def _extract_cliente(self, text: str):
        cliente_regex = r'Cliente\s*:\s*(\d+)\s*-\s*([^\n]+)'
        match = re.search(cliente_regex, text)
        if match:
            self.cliente = Cliente(codigo=match.group(1).strip(), nome=match.group(2).strip())
        else:
            st.warning("Não foi possível extrair informações do cliente")
    
    def _extract_venda(self, text: str):
        venda_regex = r'Venda:\s*(\d+)\s+Dt\.?\s*Venda:\s*(\d{2}/\d{2}/\d{4})\s+Valor\s*da\s*venda:\s*([\d\.,]+)'
        match = re.search(venda_regex, text)
        if match:
            self.venda = Venda(
                numero=match.group(1).strip(),
                data=match.group(2).strip(),
                valor=parse_monetary(match.group(3))
            )
        else:
            st.warning("Não foi possível extrair informações da venda")
    
    def _extract_parcelas(self, text: str):
        padrao_parcela = (
            r'([A-Z]?\.?\d+/\d+)\s+'  
            r'(\d{2}/\d{2}/\d{4})\s+'  
            r'(?:\d+\s+)?'  
            r'([\d\.,]+)\s+'  
            r'(?:\d{2}/\d{2}/\d{4}\s+)?'  
            r'([\d\.,]*)'  
        )
        
        matches = re.finditer(padrao_parcela, text)
        self.parcelas = []
        
        for match in matches:
            codigo = match.group(1).strip()
            if codigo.startswith('Total') or not any(c.isdigit() for c in codigo):
                continue
                
            data_vencimento = match.group(2).strip()
            valor_original = parse_monetary(match.group(3))
            
            valor_pago_str = match.group(4) if match.group(4) else "0,00"
            valor_pago = parse_monetary(valor_pago_str)
            
            data_pagamento = None
            pagamento_match = re.search(
                r'{}\s+{}\s+(?:\d+\s+)?[\d\.,]+\s+(\d{{2}}/\d{{2}}/\d{{4}})'.format(
                    re.escape(codigo), re.escape(data_vencimento)
                ), text
            )
            if pagamento_match:
                data_pagamento = pagamento_match.group(1)
            
            parcela = Parcela(
                codigo=codigo,
                data_vencimento=data_vencimento,
                valor_original=valor_original,
                data_recebimento=data_pagamento,
                valor_pago=valor_pago
            )
            self.parcelas.append(parcela)
            
        if not self.parcelas:
            st.warning("Nenhuma parcela foi identificada no documento")

        total_recebido_match = re.search(
            r'RECEBIDO\s*:\s*([\d\.,]+)\s+([\d\.,]+)',
            text
        )
        if total_recebido_match:
            self.total_recebido = parse_monetary(total_recebido_match.group(2))
        
        self.total_original = sum(p.valor_original for p in self.parcelas)
    
    def _calculate_totais(self):
        self.total_recebido = sum(p.valor_pago for p in self.parcelas)
        self.total_original = sum(p.valor_original for p in self.parcelas)

# ===== Interface do Usuário =====
def render_sidebar():
    st.sidebar.header("Configurações de Correção")
    
    if st.sidebar.button("🗑️ Limpar Cache", help="Limpa dados locais e re-baixa das fontes"):
        limpar_cache()
        st.rerun()
    
    # Verificar índices disponíveis
    with st.sidebar.expander("📊 Status dos Índices", expanded=True):
        indices_disponiveis = get_indices_disponiveis()
        
        # Mostra status visual
        for nome, status in indices_disponiveis.items():
            icon = "✅" if status['disponivel'] else "❌"
            dt = status.get('ultima_data', '-')
            fonte = status.get('fonte_principal', 'API')
            st.caption(f"{icon} {nome} (Até: {dt}) [{fonte}]")
    
    if not indices_disponiveis:
        st.sidebar.error("Nenhum índice disponível. Verifique sua conexão.")
    
    modo = st.sidebar.radio(
        "Modo de Operação",
        options=["Corrigir Valores do PDF", "Corrigir Valor Manual"],
        index=0
    )
    
    metodo_correcao = st.sidebar.radio(
        "Método de Correção",
        options=["Índice Único", "Média de Índices"],
        index=0
    )
    
    # Filtrar apenas índices disponíveis
    indices_disponiveis_lista = [k for k, v in indices_disponiveis.items() if v.get('disponivel', False)]
    
    if metodo_correcao == "Índice Único":
        indice_selecionado = st.sidebar.selectbox(
            "Selecione o índice econômico",
            options=indices_disponiveis_lista,
            index=0 if indices_disponiveis_lista else None
        )
        indices_para_calculo = [indice_selecionado] if indice_selecionado else []
    else:
        if 'multiselect_indices' not in st.session_state:
            st.session_state.multiselect_indices = indices_disponiveis_lista[:2] if len(indices_disponiveis_lista) >= 2 else indices_disponiveis_lista
        
        st.sidebar.multiselect(
            "Selecione os índices para cálculo da média",
            options=indices_disponiveis_lista,
            default=st.session_state.multiselect_indices,
            key="multiselect_indices"
        )
        
        indices_para_calculo = st.session_state.multiselect_indices
        
        if len(indices_para_calculo) < 2:
            st.sidebar.warning("Selecione pelo menos 2 índices.")
            
    data_referencia = st.sidebar.date_input(
        "Data de referência para correção",
        value=datetime.now(pytz.timezone('America/Sao_Paulo')).date(),
        format="DD/MM/YYYY"
    )
    
    return {
        "modo": modo,
        "metodo_correcao": metodo_correcao,
        "indices_para_calculo": indices_para_calculo,
        "data_referencia": data_referencia
    }

def render_correcao_manual(config: Dict):
    st.subheader("Correção Monetária Manual")
    
    if "valores_manuais" not in st.session_state:
        st.session_state.valores_manuais = []
    
    with st.expander("Adicionar Novo Valor para Correção", expanded=True):
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            novo_valor = st.number_input(
                "Valor (R$)",
                min_value=0.0,
                value=1000.0,
                step=100.0,
                key="novo_valor"
            )
        with col2:
            nova_data = st.date_input(
                "Data do valor",
                value=date(2023, 1, 1),
                format="DD/MM/YYYY",
                key="nova_data"
            )
        with col3:
            st.write("")
            st.write("")
            if st.button("➕ Adicionar", key="btn_adicionar_valor"):
                st.session_state.valores_manuais.append({
                    "valor": novo_valor,
                    "data": nova_data,
                    "id": str(len(st.session_state.valores_manuais))
                })
                st.rerun()
    
    if st.session_state.valores_manuais:
        st.subheader("Valores para Correção")
        
        cols = st.columns([3, 2, 2, 1])
        with cols[0]: st.markdown("**Valor (R$)**")
        with cols[1]: st.markdown("**Data**")
        with cols[2]: st.markdown("**Ações**")
        
        with st.form(key="form_remover_valores"):
            to_remove = []
            for i, item in enumerate(st.session_state.valores_manuais):
                cols = st.columns([3, 2, 2, 1])
                with cols[0]: st.markdown(f"R$ {item['valor']:,.2f}")
                with cols[1]: st.markdown(item['data'].strftime("%d/%m/%Y"))
                with cols[2]:
                    if st.checkbox(f"Remover", key=f"remove_{item['id']}"):
                        to_remove.append(i)
            
            if st.form_submit_button("✅ Confirmar Remoções", type="primary"):
                if to_remove:
                    for i in sorted(to_remove, reverse=True):
                        if 0 <= i < len(st.session_state.valores_manuais):
                            st.session_state.valores_manuais.pop(i)
                    st.success(f"{len(to_remove)} valor(es) removido(s)!")
                    st.rerun()
        
        if st.button("🎯 Calcular Correção para Todos", type="primary", key="btn_calcular_todos"):
            with st.spinner("Calculando correções..."):
                resultados = []
                
                for item in st.session_state.valores_manuais:
                    valor = item["valor"]
                    data_valor = item["data"]
                    
                    if data_valor > config["data_referencia"]:
                        st.warning(f"Data de referência deve ser posterior à data do valor {valor}")
                        continue
                    
                    try:
                        if config["metodo_correcao"] == "Índice Único" and config["indices_para_calculo"]:
                            correcao = calcular_correcao_individual(
                                valor,
                                data_valor,
                                config["data_referencia"],
                                config["indices_para_calculo"][0]
                            )
                        elif config["indices_para_calculo"]:
                            correcao = calcular_correcao_media(
                                valor,
                                data_valor,
                                config["data_referencia"],
                                config["indices_para_calculo"]
                            )
                        else:
                            st.error("Nenhum índice selecionado para cálculo")
                            break
                        
                        if correcao['sucesso']:
                            resultados.append({
                                "Valor Original": valor,
                                "Data Original": data_valor.strftime("%d/%m/%Y"),
                                "Valor Corrigido": correcao["valor_corrigido"],
                                "Índice(s)": correcao.get('indice', ', '.join(correcao.get('indices_com_falha', []))),
                                "Fator de Correção": correcao["fator_correcao"],
                                "Variação (%)": correcao["variacao_percentual"],
                                "Fonte": correcao.get('fonte', 'API')
                            })
                        else:
                            st.warning(f"Erro ao corrigir valor R$ {valor:,.2f}: {correcao.get('mensagem', 'Erro desconhecido')}")
                    except Exception as e:
                        st.error(f"Erro ao processar valor R$ {valor:,.2f}: {str(e)}")
                
                if resultados:
                    df_resultados = pd.DataFrame(resultados)
                    st.subheader("📊 Resultados da Correção")
                    st.dataframe(df_resultados.style.format({
                        "Valor Original": "R$ {:.2f}",
                        "Valor Corrigido": "R$ {:.2f}",
                        "Fator de Correção": "{:.6f}",
                        "Variação (%)": "{:.2f}%"
                    }))
                    
                    # Totais
                    col1, col2, col3 = st.columns(3)
                    total_orig = df_resultados["Valor Original"].sum()
                    total_corr = df_resultados["Valor Corrigido"].sum()
                    variacao = ((total_corr - total_orig) / total_orig) * 100 if total_orig else 0
                    
                    col1.metric("Total Original", formatar_moeda(total_orig))
                    col2.metric("Total Corrigido", formatar_moeda(total_corr))
                    col3.metric("Variação Total", f"{variacao:+.2f}%")

                    # Exportar
                    col1, col2 = st.columns(2)
                    with col1:
                        csv = df_resultados.to_csv(index=False, sep=';', decimal=',')
                        b64_csv = base64.b64encode(csv.encode()).decode()
                        href_csv = f'<a href="data:file/csv;base64,{b64_csv}" download="correcao_manual.csv" style="background-color: #4CAF50; color: white; padding: 10px 20px; border-radius: 5px; text-decoration: none;">📥 Baixar CSV</a>'
                        st.markdown(href_csv, unsafe_allow_html=True)
                    with col2:
                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                            df_resultados.to_excel(writer, index=False, sheet_name='Resultados')
                        b64_xlsx = base64.b64encode(output.getvalue()).decode()
                        href_xlsx = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64_xlsx}" download="correcao_manual.xlsx" style="background-color: #2196F3; color: white; padding: 10px 20px; border-radius: 5px; text-decoration: none;">📊 Baixar Excel</a>'
                        st.markdown(href_xlsx, unsafe_allow_html=True)
                else:
                    st.warning("Nenhum resultado foi calculado com sucesso.")
    else:
        st.info("➕ Adicione valores para correção usando o painel acima")

def render_cliente_info(processor: PDFProcessor):
    st.subheader("👤 Informações do Cliente")
    col1, col2 = st.columns(2)
    with col1:
        st.text_input("Código", processor.cliente.codigo, disabled=True)
    with col2:
        st.text_input("Nome", processor.cliente.nome, disabled=True)

def render_venda_info(processor: PDFProcessor):
    st.subheader("💰 Informações da Venda")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.text_input("Número", processor.venda.numero, disabled=True)
    with col2:
        st.text_input("Data", processor.venda.data, disabled=True)
    with col3:
        st.text_input("Valor", formatar_moeda(processor.venda.valor), disabled=True)

def render_pdf_analysis(processor: PDFProcessor, config: Dict):
    render_cliente_info(processor)
    render_venda_info(processor)
    
    st.divider()
    col1, col2 = st.columns(2)
    with col1: InfoBox("Valor Original Total", formatar_moeda(processor.total_original), "blue")
    with col2: InfoBox("Valor Recebido Total", formatar_moeda(processor.total_recebido), "green" if processor.total_recebido > 0 else "yellow")
    
    st.divider()
    
    if not config["indices_para_calculo"]:
        st.error("❌ Nenhum índice disponível para cálculo. Verifique o status dos índices na barra lateral.")
        return
    
    if st.button("🎯 Calcular Correção Monetária", type="primary", key="btn_calcular_correcao"):
        with st.spinner("Calculando correção monetária..."):
            resultados = []
            total_parcelas = len(processor.parcelas)
            
            if total_parcelas == 0:
                st.error("Nenhuma parcela encontrada para correção")
                return
                
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for i, parcela in enumerate(processor.parcelas):
                progress = (i + 1) / total_parcelas
                progress_bar.progress(progress)
                status_text.text(f"Processando parcela {i+1} de {total_parcelas}...")
                
                valor_original = parcela.valor_original
                valor_pago = parcela.valor_pago
                data_vencimento = parse_date(parcela.data_vencimento)
                data_pagamento = parse_date(parcela.data_recebimento) if parcela.data_recebimento else None
                
                if not data_vencimento:
                    continue
                
                try:
                    # Correção Original
                    if config["metodo_correcao"] == "Índice Único":
                        correcao_orig = calcular_correcao_individual(
                            valor_original, data_vencimento, config["data_referencia"], config["indices_para_calculo"][0]
                        )
                    else:
                        correcao_orig = calcular_correcao_media(
                            valor_original, data_vencimento, config["data_referencia"], config["indices_para_calculo"]
                        )
                    
                    # Correção Pago
                    correcao_pago = None
                    if data_pagamento and valor_pago > 0:
                        if config["metodo_correcao"] == "Índice Único":
                            correcao_pago = calcular_correcao_individual(
                                valor_pago, data_pagamento, config["data_referencia"], config["indices_para_calculo"][0]
                            )
                        else:
                            correcao_pago = calcular_correcao_media(
                                valor_pago, data_pagamento, config["data_referencia"], config["indices_para_calculo"]
                            )

                    resultados.append({
                        'Parcela': parcela.codigo,
                        'Dt Vencim': parcela.data_vencimento,
                        'Dt Receb': parcela.data_recebimento if parcela.data_recebimento else "",
                        'Valor Original': valor_original,
                        'Valor Original Corrigido': correcao_orig['valor_corrigido'] if correcao_orig['sucesso'] else valor_original,
                        'Valor Pago': valor_pago,
                        'Valor Pago Corrigido': correcao_pago['valor_corrigido'] if correcao_pago and correcao_pago['sucesso'] else valor_pago,
                        'Índice': config["indices_para_calculo"][0] if len(config["indices_para_calculo"]) == 1 else "Média",
                        'Fator Orig': correcao_orig.get('fator_correcao', 1.0),
                        'Status': '✅' if correcao_orig['sucesso'] else '❌',
                        'Fonte': correcao_orig.get('fonte', 'API')
                    })

                except Exception as e:
                    st.error(f"Erro na parcela {parcela.codigo}: {str(e)}")
                    continue
            
            progress_bar.empty()
            status_text.empty()
            
            if resultados:
                df_res = pd.DataFrame(resultados)
                
                st.subheader("📊 Resultados Detalhados")
                st.dataframe(df_res.style.format({
                    'Valor Original': lambda x: formatar_moeda(x),
                    'Valor Original Corrigido': lambda x: formatar_moeda(x),
                    'Valor Pago': lambda x: formatar_moeda(x),
                    'Valor Pago Corrigido': lambda x: formatar_moeda(x),
                    'Fator Orig': '{:.6f}'
                }))
                
                # Resumo
                st.subheader("📈 Resumo Estatístico")
                col1, col2, col3, col4 = st.columns(4)
                
                tot_orig = df_res['Valor Original'].sum()
                tot_orig_corr = df_res['Valor Original Corrigido'].sum()
                tot_pago = df_res['Valor Pago'].sum()
                tot_pago_corr = df_res['Valor Pago Corrigido'].sum()
                
                col1.metric("Total Original", formatar_moeda(tot_orig))
                col2.metric("Original Corrigido", formatar_moeda(tot_orig_corr), formatar_moeda(tot_orig_corr - tot_orig))
                col3.metric("Total Pago", formatar_moeda(tot_pago))
                col4.metric("Pago Corrigido", formatar_moeda(tot_pago_corr), formatar_moeda(tot_pago_corr - tot_pago))
                
                # Exportação
                st.subheader("💾 Exportar")
                c1, c2 = st.columns(2)
                with c1:
                    csv = df_res.to_csv(index=False, sep=';', decimal=',')
                    b64 = base64.b64encode(csv.encode()).decode()
                    st.markdown(f'<a href="data:file/csv;base64,{b64}" download="relatorio_final.csv" style="background-color:#4CAF50;color:white;padding:10px;border-radius:5px;text-decoration:none">📥 Baixar CSV</a>', unsafe_allow_html=True)
                with c2:
                    out = BytesIO()
                    with pd.ExcelWriter(out, engine='xlsxwriter') as w:
                        df_res.to_excel(w, index=False)
                    b64_xls = base64.b64encode(out.getvalue()).decode()
                    st.markdown(f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64_xls}" download="relatorio_final.xlsx" style="background-color:#2196F3;color:white;padding:10px;border-radius:5px;text-decoration:none">📊 Baixar Excel</a>', unsafe_allow_html=True)

def main():
    try:
        config = render_sidebar()
        if not config:
            return

        if config["modo"] == "Corrigir Valor Manual":
            render_correcao_manual(config)
        else:
            uploaded_file = FileUploader()
            if uploaded_file is not None:
                processor = PDFProcessor()
                if processor.process_pdf(uploaded_file):
                    render_pdf_analysis(processor, config)
                else:
                    st.error("Falha ao processar o PDF.")
            else:
                st.info("📤 Faça upload de um arquivo PDF para começar.")
                
    except Exception as e:
        st.error(f"❌ Ocorreu um erro inesperado: {str(e)}")

if __name__ == "__main__":
    main()
