import streamlit as st
import pdfplumber
import pandas as pd
import re
import requests
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
import base64
import pytz
from io import BytesIO

# Configuração da página deve ser a PRIMEIRA chamada Streamlit
st.set_page_config(page_title="Correção Monetária Completa", layout="wide")

# Título do aplicativo
st.title("📈 Correção Monetária Completa")

# Importações do módulo de índices
try:
    from utils.indices import (
        get_indices_disponiveis,
        calcular_correcao_individual,
        calcular_correcao_media,
        formatar_moeda
    )
    from utils.parser import extract_payment_data
except ImportError:
    st.error("Erro na importação dos módulos locais (utils). Verifique a estrutura de pastas.")
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
            
            # Debug: Mostrar texto extraído (oculto por padrão)
            with st.expander("Ver texto extraído do PDF"):
                st.text(full_text)
            
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
            # Regex específica para encontrar a data de pagamento associada à parcela
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
    st.sidebar.header("Configurações")
    
    # === BOTÃO DE LIMPAR CACHE ===
    if st.sidebar.button("🧹 Limpar Cache / Recarregar Dados", type="secondary"):
        st.cache_data.clear()
        st.rerun()
    
    st.sidebar.divider()
    st.sidebar.header("Parâmetros de Correção")
    
    with st.spinner("Conectando aos bancos de dados econômicos..."):
        indices_disponiveis = get_indices_disponiveis()
    
    if not indices_disponiveis:
        st.sidebar.error("Não foi possível carregar índices. Tente 'Limpar Cache'.")
        return None
    
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
            "Selecione o índice",
            options=list(indices_disponiveis.keys()),
            format_func=lambda x: indices_disponiveis[x]
        )
        indices_para_calculo = [indice_selecionado]
    else:
        indices_selecionados = st.sidebar.multiselect(
            "Selecione índices para média",
            options=list(indices_disponiveis.keys()),
            default=list(indices_disponiveis.keys()),
            format_func=lambda x: indices_disponiveis[x]
        )
        indices_para_calculo = indices_selecionados
        if len(indices_para_calculo) < 2:
            st.sidebar.warning("Selecione ao menos 2 índices para média.")

    data_referencia = st.sidebar.date_input(
        "Data de referência (Atualização)",
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
    
    with st.expander("Adicionar Valor", expanded=True):
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            novo_valor = st.number_input("Valor (R$)", min_value=0.0, value=1000.0, step=100.0, key="novo_valor")
        with col2:
            nova_data = st.date_input("Data Original", value=date(2023, 1, 1), format="DD/MM/YYYY", key="nova_data")
        with col3:
            st.write("")
            st.write("")
            if st.button("➕ Adicionar"):
                st.session_state.valores_manuais.append({
                    "valor": novo_valor,
                    "data": nova_data,
                    "id": str(len(st.session_state.valores_manuais))
                })
                st.rerun()
    
    # Lista de valores
    if st.session_state.valores_manuais:
        st.divider()
        st.markdown("### Valores Adicionados")
        to_remove = []
        for i, item in enumerate(st.session_state.valores_manuais):
            c1, c2, c3 = st.columns([2, 2, 1])
            c1.write(f"**Valor:** {formatar_moeda(item['valor'])}")
            c2.write(f"**Data:** {item['data'].strftime('%d/%m/%Y')}")
            if c3.button("🗑️", key=f"del_{i}"):
                to_remove.append(i)
        
        if to_remove:
            for i in sorted(to_remove, reverse=True):
                st.session_state.valores_manuais.pop(i)
            st.rerun()

        if st.button("Calcular Correção", type="primary"):
            processar_calculo_manual(st.session_state.valores_manuais, config)

def processar_calculo_manual(itens, config):
    resultados = []
    for item in itens:
        if config["metodo_correcao"] == "Índice Único":
            res = calcular_correcao_individual(
                item["valor"], item["data"], config["data_referencia"], config["indices_para_calculo"][0]
            )
        else:
            res = calcular_correcao_media(
                item["valor"], item["data"], config["data_referencia"], config["indices_para_calculo"]
            )
        
        if res['sucesso']:
            resultados.append({
                "Data Origem": item["data"].strftime("%d/%m/%Y"),
                "Valor Original": item["valor"],
                "Valor Corrigido": res["valor_corrigido"],
                "Fator": res["fator_correcao"],
                "Variação %": res["variacao_percentual"]
            })
        else:
            st.error(f"Erro ao calcular valor de {item['data']}: {res.get('mensagem')}")

    if resultados:
        df = pd.DataFrame(resultados)
        st.dataframe(df.style.format({
            "Valor Original": "R$ {:,.2f}",
            "Valor Corrigido": "R$ {:,.2f}",
            "Fator": "{:.6f}",
            "Variação %": "{:.2f}%"
        }))
        exportar_dados(df)

def render_pdf_analysis(processor: PDFProcessor, config: Dict):
    st.subheader("Informações do Cliente")
    c1, c2 = st.columns(2)
    c1.text_input("Código", processor.cliente.codigo, disabled=True)
    c2.text_input("Nome", processor.cliente.nome, disabled=True)
    
    st.divider()
    
    if st.button("Calcular Correção do PDF", type="primary"):
        resultados = []
        bar = st.progress(0)
        
        for i, p in enumerate(processor.parcelas):
            bar.progress((i + 1) / len(processor.parcelas))
            dt_venc = parse_date(p.data_vencimento)
            
            if not dt_venc: continue
            
            # Correção do valor original
            if config["metodo_correcao"] == "Índice Único":
                res = calcular_correcao_individual(
                    p.valor_original, dt_venc, config["data_referencia"], config["indices_para_calculo"][0]
                )
            else:
                res = calcular_correcao_media(
                    p.valor_original, dt_venc, config["data_referencia"], config["indices_para_calculo"]
                )
            
            resultados.append({
                "Parcela": p.codigo,
                "Vencimento": p.data_vencimento,
                "Valor Original": p.valor_original,
                "Valor Corrigido": res.get("valor_corrigido", 0.0),
                "Fator": res.get("fator_correcao", 1.0)
            })
            
        df = pd.DataFrame(resultados)
        st.success("Cálculo finalizado!")
        st.dataframe(df.style.format({
            "Valor Original": "R$ {:,.2f}",
            "Valor Corrigido": "R$ {:,.2f}",
            "Fator": "{:.6f}"
        }))
        
        # Resumo
        total_orig = df["Valor Original"].sum()
        total_corr = df["Valor Corrigido"].sum()
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Original", formatar_moeda(total_orig))
        c2.metric("Total Corrigido", formatar_moeda(total_corr))
        c3.metric("Diferença", formatar_moeda(total_corr - total_orig))
        
        exportar_dados(df)

def exportar_dados(df):
    st.subheader("Exportar")
    c1, c2 = st.columns(2)
    
    csv = df.to_csv(index=False).encode('utf-8')
    c1.download_button("Download CSV", csv, "correcao.csv", "text/csv")
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    c2.download_button("Download Excel", output.getvalue(), "correcao.xlsx", "application/vnd.ms-excel")

def main():
    config = render_sidebar()
    if not config: return
    
    if config["modo"] == "Corrigir Valor Manual":
        render_correcao_manual(config)
    else:
        file = FileUploader()
        if file:
            proc = PDFProcessor()
            if proc.process_pdf(file):
                render_pdf_analysis(proc, config)

if __name__ == "__main__":
    main()
