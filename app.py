import streamlit as st
import pdfplumber
import pandas as pd
import re
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
import base64
import pytz
from dateutil.relativedelta import relativedelta

# Configuração da página
st.set_page_config(page_title="Correção Monetária Completa", layout="wide")

# Título do aplicativo
st.title("📈 Correção Monetária Completa")

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
def formatar_moeda(valor: float) -> str:
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

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
            self.cliente = Cliente(codigo=match.group(1), nome=match.group(2).strip())

    def _extract_venda(self, text: str):
        venda_regex = r'Venda:\s*(\d+)\s*Dt\.\s*Venda:\s*(\d{2}/\d{2}/\d{4})'
        match = re.search(venda_regex, text)
        if match:
            valor_venda = self._extract_valor_venda(text)
            self.venda = Venda(
                numero=match.group(1),
                data=match.group(2),
                valor=valor_venda
            )

    def _extract_valor_venda(self, text: str) -> float:
        regex = r'Valor da venda:\s*([\d\.,]+)'
        match = re.search(regex, text)
        return parse_monetary(match.group(1)) if match else 0.0

    def _extract_parcelas(self, text: str):
        # Padrão corrigido para extrair todas as parcelas pagas
        regex = r'([A-Z]\.\d+/\d+)\s+(\d{2}/\d{2}/\d{4})\s+\d*\s+([\d\.,]+)\s+(\d{2}/\d{2}/\d{4})\s+([\d\.,]+)\s+([\d\.,]*)\s+([\d\.,]*)\s+([\d\.,]*)\s+([\d\.,]*)\s+([\d\.,]*)'
        
        matches = re.finditer(regex, text)
        self.parcelas = []
        
        for match in matches:
            if match.group(4):  # Só inclui parcelas com data de recebimento
                parcela = Parcela(
                    codigo=match.group(1),
                    data_vencimento=match.group(2),
                    valor_original=parse_monetary(match.group(5)),  # Vlr. da Parcela
                    data_recebimento=match.group(4),
                    valor_pago=parse_monetary(match.group(3))  # Valor Parc. (valor pago)
                )
                self.parcelas.append(parcela)

    def _calculate_totais(self):
        """Calcula totais recebidos e valor original"""
        self.total_recebido = sum(p.valor_pago for p in self.parcelas if p.data_recebimento)
        self.total_original = sum(p.valor_original for p in self.parcelas)

# ===== Funções de Correção Monetária =====
def get_indices_disponiveis() -> Dict[str, str]:
    return {
        "IGPM": "Índice Geral de Preços - Mercado",
        "INPC": "Índice Nacional de Preços ao Consumidor",
        "INCC": "Índice Nacional de Custo da Construção",
        "IPCA": "Índice de Preços ao Consumidor Amplo"
    }

def get_indice_real(indice: str, data: date) -> float:
    """Simulação mais realista dos índices"""
    # Valores base mensais fictícios (em %)
    indices_base = {
        "IGPM": [0.82, 0.75, 0.68, 0.71, 0.79, 0.85, 0.90, 0.88, 0.83, 0.77, 0.72, 0.70],
        "INPC": [0.48, 0.45, 0.42, 0.44, 0.47, 0.50, 0.53, 0.52, 0.49, 0.46, 0.43, 0.41],
        "INCC": [0.65, 0.60, 0.55, 0.58, 0.63, 0.68, 0.72, 0.70, 0.66, 0.61, 0.57, 0.54],
        "IPCA": [0.54, 0.51, 0.47, 0.49, 0.52, 0.56, 0.59, 0.57, 0.53, 0.50, 0.48, 0.45]
    }
    
    # Variação anual fictícia
    ano_fator = 1 + (data.year - 2020) * 0.05
    mes_idx = data.month - 1
    
    return (indices_base.get(indice, [0.5]*12)[mes_idx] * ano_fator) / 100

def calcular_correcao_individual(valor: float, data_origem: date, data_referencia: date, indice: str) -> Dict[str, float]:
    """Calcula correção monetária usando um único índice"""
    if data_origem >= data_referencia:
        return {"valor_corrigido": valor, "fator_correcao": 1.0, "variacao_percentual": 0.0}
    
    fator = 1.0
    data_atual = data_origem
    
    while data_atual < data_referencia:
        variacao = get_indice_real(indice, data_atual)
        fator *= (1 + variacao)
        data_atual += relativedelta(months=1)
    
    valor_corrigido = valor * fator
    variacao_percentual = (fator - 1) * 100
    
    return {
        "valor_corrigido": valor_corrigido,
        "fator_correcao": fator,
        "variacao_percentual": variacao_percentual
    }

def calcular_correcao_media(valor: float, data_origem: date, data_referencia: date, indices: List[str]) -> Dict[str, float]:
    """Calcula correção monetária usando média de vários índices"""
    if data_origem >= data_referencia:
        return {"valor_corrigido": valor, "fator_correcao": 1.0, "variacao_percentual": 0.0}
    
    fatores = []
    
    for indice in indices:
        fator = 1.0
        data_atual = data_origem
        
        while data_atual < data_referencia:
            variacao = get_indice_real(indice, data_atual)
            fator *= (1 + variacao)
            data_atual += relativedelta(months=1)
        
        fatores.append(fator)
    
    fator_medio = sum(fatores) / len(fatores)
    valor_corrigido = valor * fator_medio
    variacao_percentual = (fator_medio - 1) * 100
    
    return {
        "valor_corrigido": valor_corrigido,
        "fator_correcao": fator_medio,
        "variacao_percentual": variacao_percentual
    }

# ===== Interface do Usuário =====
def render_sidebar():
    """Renderiza a barra lateral com configurações"""
    st.sidebar.header("Configurações de Correção")
    
    # Modo de operação
    modo = st.sidebar.radio(
        "Modo de Operação",
        options=["Corrigir Valores do PDF", "Corrigir Valor Manual"],
        index=0
    )
    
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
            default=["IGPM", "INPC", "INCC", "IPCA"]
        )
        indices_para_calculo = indices_selecionados if len(indices_selecionados) >= 2 else ["IGPM", "INPC", "INCC", "IPCA"]
        st.sidebar.info("Selecione pelo menos 2 índices para calcular a média.")
    
    # Data de referência para correção
    data_referencia = st.sidebar.date_input(
        "Data de referência para correção",
        value=datetime.now(pytz.timezone('America/Sao_Paulo')).date(),
        format="DD/MM/YYYY"
    )
    
    # Campos para correção manual
    valor_manual = None
    data_manual = None
    
    if modo == "Corrigir Valor Manual":
        valor_manual = st.sidebar.number_input(
            "Valor a ser corrigido (R$)",
            min_value=0.0,
            value=155000.0,
            step=1000.0
        )
        
        data_manual = st.sidebar.date_input(
            "Data do valor",
            value=date(2025, 2, 18),
            format="DD/MM/YYYY"
        )
    
    return {
        "modo": modo,
        "metodo_correcao": metodo_correcao,
        "indices_para_calculo": indices_para_calculo,
        "data_referencia": data_referencia,
        "valor_manual": valor_manual,
        "data_manual": data_manual
    }

def render_correcao_manual(config: Dict):
    """Renderiza a correção manual"""
    st.subheader("Correção Monetária Manual")
    
    if config["valor_manual"] <= 0:
        st.warning("Informe um valor positivo para correção")
        return
    
    if config["data_manual"] > config["data_referencia"]:
        st.warning("A data de referência deve ser posterior à data do valor")
        return
    
    if config["metodo_correcao"] == "Índice Único":
        correcao = calcular_correcao_individual(
            config["valor_manual"],
            config["data_manual"],
            config["data_referencia"],
            config["indices_para_calculo"][0]
        )
    else:
        correcao = calcular_correcao_media(
            config["valor_manual"],
            config["data_manual"],
            config["data_referencia"],
            config["indices_para_calculo"]
        )
    
    col1, col2, col3 = st.columns(3)
    
    col1.metric("Valor Original", formatar_moeda(config["valor_manual"]))
    col2.metric("Valor Corrigido", formatar_moeda(correcao["valor_corrigido"]))
    col3.metric("Variação", f"{correcao['variacao_percentual']:.2f}%")
    
    st.write(f"**Índice(s) utilizado(s):** {', '.join(config['indices_para_calculo']) if config['metodo_correcao'] == 'Média de Índices' else config['indices_para_calculo'][0]}")
    st.write(f"**Período:** {config['data_manual'].strftime('%d/%m/%Y')} a {config['data_referencia'].strftime('%d/%m/%Y')}")
    st.write(f"**Fator de correção:** {correcao['fator_correcao']:.6f}")
def render_cliente_info(processor: PDFProcessor):
    """Renderiza informações do cliente"""
    st.subheader("Informações do Cliente")
    col1, col2 = st.columns(2)
    with col1:
        st.text_input("Código", processor.cliente.codigo, disabled=True)
    with col2:
        st.text_input("Nome", processor.cliente.nome, disabled=True)

def render_venda_info(processor: PDFProcessor):
    """Renderiza informações da venda"""
    st.subheader("Informações da Venda")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.text_input("Número", processor.venda.numero, disabled=True)
    with col2:
        st.text_input("Data", processor.venda.data, disabled=True)
    with col3:
        st.text_input("Valor", formatar_moeda(processor.venda.valor), disabled=True)
        
def render_pdf_analysis(processor: PDFProcessor, config: Dict):
    """Renderiza a análise do PDF"""
    # Exibir informações básicas
    render_cliente_info(processor)
    render_venda_info(processor)
    
    # Cartões com totais
    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        InfoBox(
            "Valor Original Total",
            formatar_moeda(processor.total_original),
            "blue"
        )
    with col2:
        InfoBox(
            "Valor Recebido Total",
            formatar_moeda(processor.total_recebido),
            "green"
        )
    
    # Botão para calcular correção
    st.divider()
    if st.button("Calcular Correção Monetária", type="primary"):
        # Lista para armazenar resultados
        resultados = []
        
        for parcela in processor.parcelas:
            valor_original = parcela.valor_original
            data_vencimento = parse_date(parcela.data_vencimento)
            
            if not data_vencimento:
                st.warning(f"Data de vencimento inválida para parcela {parcela.codigo}")
                continue
            
            try:
                if config["metodo_correcao"] == "Índice Único":
                    correcao = calcular_correcao_individual(
                        valor_original,
                        data_vencimento,
                        config["data_referencia"],
                        config["indices_para_calculo"][0]
                    )
                else:
                    correcao = calcular_correcao_media(
                        valor_original,
                        data_vencimento,
                        config["data_referencia"],
                        config["indices_para_calculo"]
                    )
                
                # Adicionar ao dataframe de resultados
                resultados.append({
                    'Parcela': parcela.codigo,
                    'Dt Vencim': parcela.data_vencimento,
                    'Valor Original': valor_original,
                    'Valor Pago': parcela.valor_pago,
                    'Índice(s)': ', '.join(config["indices_para_calculo"]) if config["metodo_correcao"] == "Média de Índices" else config["indices_para_calculo"][0],
                    'Fator de Correção': correcao['fator_correcao'],
                    'Variação (%)': correcao['variacao_percentual'],
                    'Valor Corrigido': correcao['valor_corrigido']
                })
            
            except Exception as e:
                st.error(f"Erro ao corrigir parcela {parcela.codigo}: {str(e)}")
                continue
        
        if resultados:
            # Criar DataFrame com resultados
            df_resultados = pd.DataFrame(resultados)
            
            # Mostrar resultados
            st.subheader("Resultados da Correção Monetária")
            st.dataframe(df_resultados.style.format({
                'Valor Original': formatar_moeda,
                'Valor Pago': formatar_moeda,
                'Fator de Correção': "{:.6f}",
                'Variação (%)': "{:.2f}%",
                'Valor Corrigido': formatar_moeda
            }), use_container_width=True)
            
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
            
            # Criar link para download
            csv = df_resultados.to_csv(index=False)
            b64 = base64.b64encode(csv.encode()).decode()
            href = f'<a href="data:file/csv;base64,{b64}" download="parcelas_corrigidas.csv">Baixar como CSV</a>'
            st.markdown(href, unsafe_allow_html=True)
        else:
            st.warning("Nenhum resultado foi gerado")

# ===== Aplicação principal =====
def main():
    # Configurações da barra lateral
    config = render_sidebar()
    
    if config["modo"] == "Corrigir Valor Manual":
        render_correcao_manual(config)
    else:
        # Upload do arquivo para modo PDF
        uploaded_file = FileUploader()
        
        if uploaded_file is not None:
            processor = PDFProcessor()
            if processor.process_pdf(uploaded_file):
                render_pdf_analysis(processor, config)

if __name__ == "__main__":
    main()
