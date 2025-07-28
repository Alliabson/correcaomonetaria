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

# Configuração da página
st.set_page_config(page_title="Correção Monetária Completa", layout="wide")

# Título do aplicativo
st.title("📈 Correção Monetária Completa")

# Importações do módulo de índices
from utils.indices import (
    get_indices_disponiveis,
    calcular_correcao_individual,
    calcular_correcao_media,
    formatar_moeda
)

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
            with pdfplumber.open(BytesIO(file.getvalue())) as pdf:
                full_text = ""
                for page in pdf.pages:
                    full_text += page.extract_text() or ""
            
            st.text_area("Texto extraído do PDF (para debug)", full_text, height=200)
            
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

    def _calculate_totais(self):
        self.total_recebido = sum(p.valor_pago for p in self.parcelas)
        self.total_original = sum(p.valor_original for p in self.parcelas)

# ===== Interface do Usuário =====
def render_sidebar():
    """Renderiza a barra lateral com configurações"""
    st.sidebar.header("Configurações de Correção")
    
    with st.spinner("Verificando disponibilidade dos índices..."):
        indices_disponiveis = get_indices_disponiveis()
    
    if not indices_disponiveis:
        st.sidebar.error("Não foi possível carregar os índices. Verifique a conexão e tente novamente.")
        return None
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("Regra Específica do IGPM")
    igpm_retroacao = st.sidebar.radio(
        "Retroação do IGPM",
        options=[0, 1],
        index=1,
        format_func=lambda x: f"{x} mês(es) (Cestas/Casos Específicos)" if x==0 else f"{x} mês(es) (Padrão para Débitos)",
        help="Para corrigir débitos em um mês, use a retroação de 1 mês (pois o índice do mês corrente ainda não existe). Para cálculo de cestas ou casos onde o índice já está consolidado, pode-se usar 0."
    )
    st.sidebar.markdown("---")
    
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
            default=list(indices_disponiveis.keys())
        )
        indices_para_calculo = indices_selecionados if len(indices_selecionados) >= 2 else list(indices_disponiveis.keys())
        if len(indices_para_calculo) < 2:
            st.sidebar.info("Selecione pelo menos 2 índices para calcular a média.")
    
    data_referencia = st.sidebar.date_input(
        "Data de referência para correção",
        value=datetime.now(pytz.timezone('America/Sao_Paulo')).date(),
        format="DD/MM/YYYY"
    )
    
    return {
        "modo": modo,
        "metodo_correcao": metodo_correcao,
        "indices_para_calculo": indices_para_calculo,
        "data_referencia": data_referencia,
        "igpm_retroacao": igpm_retroacao
    }

def render_correcao_manual(config: Dict):
    """Renderiza a correção manual com capacidade de adicionar/remover parcelas"""
    st.subheader("Correção Monetária Manual")
    
    if "valores_manuais" not in st.session_state:
        st.session_state.valores_manuais = []
    
    with st.expander("Adicionar Novo Valor para Correção", expanded=True):
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            novo_valor = st.number_input("Valor (R$)", min_value=0.0, value=1000.0, step=100.0, key="novo_valor")
        with col2:
            nova_data = st.date_input("Data do valor", value=date(2023, 1, 1), format="DD/MM/YYYY", key="nova_data")
        with col3:
            st.write("")
            st.write("")
            if st.button("➕ Adicionar", key="btn_adicionar_valor"):
                st.session_state.valores_manuais.append({"valor": novo_valor, "data": nova_data, "id": str(len(st.session_state.valores_manuais))})
                st.rerun()
    
    if st.session_state.valores_manuais:
        st.subheader("Valores para Correção")
        
        cols = st.columns([3, 2, 1])
        cols[0].markdown("**Valor (R$)**")
        cols[1].markdown("**Data**")
        cols[2].markdown("**Ação**")
        
        to_remove = []
        for i, item in enumerate(st.session_state.valores_manuais):
            cols = st.columns([3, 2, 1])
            cols[0].markdown(f"R$ {item['valor']:,.2f}")
            cols[1].markdown(item['data'].strftime("%d/%m/%Y"))
            if cols[2].button(f"❌", key=f"remove_{item['id']}"):
                to_remove.append(i)
        
        if to_remove:
            for i in sorted(to_remove, reverse=True):
                if 0 <= i < len(st.session_state.valores_manuais):
                    st.session_state.valores_manuais.pop(i)
            st.rerun()
            
        if st.button("Calcular Correção para Todos", type="primary", key="btn_calcular_todos"):
            resultados = []
            for item in st.session_state.valores_manuais:
                valor = item["valor"]
                data_valor = item["data"]
                
                if data_valor > config["data_referencia"]:
                    st.warning(f"Data de referência deve ser posterior à data do valor {valor}")
                    continue
                
                if config["metodo_correcao"] == "Índice Único":
                    correcao = calcular_correcao_individual(
                        valor, data_valor, config["data_referencia"], config["indices_para_calculo"][0],
                        igpm_retroacao=config.get("igpm_retroacao", 1)
                    )
                else:
                    correcao = calcular_correcao_media(
                        valor, data_valor, config["data_referencia"], config["indices_para_calculo"],
                        igpm_retroacao=config.get("igpm_retroacao", 1)
                    )
                
                resultados.append({
                    "Valor Original": valor,
                    "Data Original": data_valor.strftime("%d/%m/%Y"),
                    "Valor Corrigido": correcao["valor_corrigido"],
                    "Índice(s)": ', '.join(correcao.get('indices', config['indices_para_calculo'])),
                    "Fator de Correção": correcao["fator_correcao"],
                    "Variação (%)": correcao["variacao_percentual"]
                })
            
            if resultados:
                df_resultados = pd.DataFrame(resultados)
                st.subheader("Resultados da Correção")
                st.dataframe(df_resultados.style.format({
                    "Valor Original": "R$ {:,.2f}", "Valor Corrigido": "R$ {:,.2f}",
                    "Fator de Correção": "{:.6f}", "Variação (%)": "{:.2f}%"
                }))
                
                st.subheader("Exportar Resultados")
                col1, col2 = st.columns(2)
                csv = df_resultados.to_csv(index=False).encode('utf-8')
                col1.download_button("Baixar como CSV", csv, "correcao_manual.csv", "text/csv")
                
                output = BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df_resultados.to_excel(writer, index=False, sheet_name='Resultados')
                excel_data = output.getvalue()
                col2.download_button("Baixar como Excel", excel_data, "correcao_manual.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("Adicione valores para correção usando o painel acima")
        
def render_cliente_info(processor: PDFProcessor):
    st.subheader("Informações do Cliente")
    col1, col2 = st.columns(2)
    col1.text_input("Código", processor.cliente.codigo, disabled=True)
    col2.text_input("Nome", processor.cliente.nome, disabled=True)

def render_venda_info(processor: PDFProcessor):
    st.subheader("Informações da Venda")
    col1, col2, col3 = st.columns(3)
    col1.text_input("Número", processor.venda.numero, disabled=True)
    col2.text_input("Data", processor.venda.data, disabled=True)
    col3.text_input("Valor", formatar_moeda(processor.venda.valor), disabled=True)
        
def render_pdf_analysis(processor: PDFProcessor, config: Dict):
    render_cliente_info(processor)
    render_venda_info(processor)
    
    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        InfoBox("Valor Original Total", formatar_moeda(processor.total_original), "blue")
    with col2:
        InfoBox("Valor Recebido Total", formatar_moeda(processor.total_recebido), "green")
    
    st.divider()
    if st.button("Calcular Correção Monetária", type="primary", key="btn_calcular_correcao"):
        resultados = []
        detalhes_indices = []
        progress_bar = st.progress(0, text="Calculando correções...")
        total_parcelas = len(processor.parcelas)
        
        for i, parcela in enumerate(processor.parcelas):
            progress_bar.progress((i + 1) / total_parcelas, text=f"Processando parcela {parcela.codigo}...")
            
            valor_original = parcela.valor_original
            valor_pago = parcela.valor_pago
            data_vencimento = parse_date(parcela.data_vencimento)
            data_pagamento = parse_date(parcela.data_recebimento) if parcela.data_recebimento else None
            
            if not data_vencimento:
                st.warning(f"Data de vencimento inválida para parcela {parcela.codigo}")
                continue
            
            try:
                if config["metodo_correcao"] == "Índice Único":
                    correcao_original = calcular_correcao_individual(
                        valor_original, data_vencimento, config["data_referencia"], config["indices_para_calculo"][0],
                        igpm_retroacao=config.get("igpm_retroacao", 1)
                    )
                else:
                    correcao_original = calcular_correcao_media(
                        valor_original, data_vencimento, config["data_referencia"], config["indices_para_calculo"],
                        igpm_retroacao=config.get("igpm_retroacao", 1)
                    )

                correcao_recebido = None
                if data_pagamento and valor_pago > 0:
                    if config["metodo_correcao"] == "Índice Único":
                        correcao_recebido = calcular_correcao_individual(
                            valor_pago, data_pagamento, config["data_referencia"], config["indices_para_calculo"][0],
                            igpm_retroacao=config.get("igpm_retroacao", 1)
                        )
                    else:
                        correcao_recebido = calcular_correcao_media(
                            valor_pago, data_pagamento, config["data_referencia"], config["indices_para_calculo"],
                            igpm_retroacao=config.get("igpm_retroacao", 1)
                        )

                resultados.append({
                    'Parcela': parcela.codigo, 'Dt Vencim': parcela.data_vencimento, 'Dt Receb': parcela.data_recebimento or "",
                    'Valor Original': valor_original, 'Valor Original Corrigido': correcao_original['valor_corrigido'],
                    'Valor Pago': valor_pago, 'Valor Pago Corrigido': correcao_recebido['valor_corrigido'] if correcao_recebido else 0.0,
                    'Índice(s)': ', '.join(config["indices_para_calculo"]) if config["metodo_correcao"] == "Média de Índices" else config["indices_para_calculo"][0],
                    'Fator Correção Original': correcao_original['fator_correcao'],
                    'Fator Correção Recebido': correcao_recebido['fator_correcao'] if correcao_recebido else 0.0,
                    'Variação (%) Original': correcao_original['variacao_percentual'],
                    'Variação (%) Recebido': correcao_recebido['variacao_percentual'] if correcao_recebido else 0.0
                })
            except Exception as e:
                st.error(f"Erro ao corrigir parcela {parcela.codigo}: {str(e)}")
        
        if resultados:
            df_resultados = pd.DataFrame(resultados)
            st.subheader("Resultados da Correção Monetária")
            st.dataframe(df_resultados.style.format({
                'Valor Original': formatar_moeda, 'Valor Original Corrigido': formatar_moeda,
                'Valor Pago': formatar_moeda, 'Valor Pago Corrigido': formatar_moeda,
                'Fator Correção Original': "{:.6f}", 'Fator Correção Recebido': "{:.6f}",
                'Variação (%) Original': "{:.2f}%", 'Variação (%) Recebido': "{:.2f}%"
            }), use_container_width=True)
            
            st.subheader("Resumo Estatístico")
            total_original_corrigido = df_resultados['Valor Original Corrigido'].sum()
            total_recebido_corrigido = df_resultados['Valor Pago Corrigido'].sum()
            saldo_devedor_corrigido = total_original_corrigido - total_recebido_corrigido
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Original Corrigido", formatar_moeda(total_original_corrigido))
            col2.metric("Total Recebido Corrigido", formatar_moeda(total_recebido_corrigido))
            col3.metric("Saldo Devedor Corrigido", formatar_moeda(saldo_devedor_corrigido), delta_color="inverse")

            st.subheader("Exportar Resultados")
            col1, col2 = st.columns(2)
            csv = df_resultados.to_csv(index=False).encode('utf-8')
            col1.download_button("Baixar como CSV", csv, "parcelas_corrigidas.csv", "text/csv")
            
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df_resultados.to_excel(writer, index=False, sheet_name='Resultados')
            excel_data = output.getvalue()
            col2.download_button("Baixar como Excel", excel_data, "parcelas_corrigidas.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

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
    
    except requests.exceptions.RequestException:
        st.error("Erro de conexão com a API do Banco Central. Verifique sua conexão com a internet.")
    except Exception as e:
        st.error(f"Ocorreu um erro inesperado: {str(e)}")

if __name__ == "__main__":
    main()
