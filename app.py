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
            with pdfplumber.open(file) as pdf:
                full_text = ""
                for page in pdf.pages:
                    full_text += page.extract_text() or ""
            
            # Debug: Mostrar texto extraído (pode ser removido após testes)
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
        # Padrão melhorado para cliente
        cliente_regex = r'Cliente\s*:\s*(\d+)\s*-\s*([^\n]+)'
        match = re.search(cliente_regex, text)
        if match:
            self.cliente = Cliente(codigo=match.group(1).strip(), nome=match.group(2).strip())
        else:
            st.warning("Não foi possível extrair informações do cliente")

    def _extract_venda(self, text: str):
        # Padrão melhorado para venda
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
        # Padrão melhorado para extrair parcelas
        padrao_parcela = (
            r'([A-Z]?\.?\d+/\d+)\s+'  # Código da parcela (P.1/12, 1/12, etc)
            r'(\d{2}/\d{2}/\d{4})\s+'  # Data de vencimento
            r'(?:\d+\s+)?'  # Atraso (opcional)
            r'([\d\.,]+)\s+'  # Valor original
            r'(?:\d{2}/\d{2}/\d{4}\s+)?'  # Data de pagamento (opcional)
            r'([\d\.,]*)'  # Valor pago
        )
        
        # Encontrar todas as parcelas no texto
        matches = re.finditer(padrao_parcela, text)
        self.parcelas = []
        
        for match in matches:
            codigo = match.group(1).strip()
            # Ignorar linhas que são totais ou não são parcelas
            if codigo.startswith('Total') or not any(c.isdigit() for c in codigo):
                continue
                
            data_vencimento = match.group(2).strip()
            valor_original = parse_monetary(match.group(3))
            
            # O valor pago pode estar em group 4 ou group 5 dependendo da estrutura
            valor_pago_str = match.group(4) if match.group(4) else "0,00"
            valor_pago = parse_monetary(valor_pago_str)
            
            # Tentar extrair data de pagamento (pode não estar presente)
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
        
        # Debug: Mostrar algumas parcelas extraídas
        st.write(f"Parcelas extraídas: {len(self.parcelas)}")
        for p in self.parcelas[:5]:  # Mostra as 5 primeiras para exemplo
            st.write(p.to_dict())
            
        if not self.parcelas:
            st.warning("Nenhuma parcela foi identificada no documento")

        # Extrair totais do final do documento
        total_recebido_match = re.search(
            r'RECEBIDO\s*:\s*([\d\.,]+)\s+([\d\.,]+)',
            text
        )
        if total_recebido_match:
            self.total_recebido = parse_monetary(total_recebido_match.group(2))
        
        # O valor original total pode ser a soma de todas as parcelas
        self.total_original = sum(p.valor_original for p in self.parcelas)

    def _calculate_totais(self):
        """Calcula totais recebidos e valor original"""
        self.total_recebido = sum(p.valor_pago for p in self.parcelas)
        self.total_original = sum(p.valor_original for p in self.parcelas)

# ===== Interface do Usuário =====
def render_sidebar():
    """Renderiza a barra lateral com configurações"""
    st.sidebar.header("Configurações de Correção")
    
    # Verificar índices disponíveis
    indices_disponiveis = get_indices_disponiveis()
    
    if not indices_disponiveis:
        st.sidebar.error("Nenhum índice econômico disponível no momento. Tente novamente mais tarde.")
        return None
    
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
    
    with st.spinner("Calculando correção monetária..."):
        if config["metodo_correcao"] == "Índice Único":
            correcao = calcular_correcao_individual(
                config["valor_manual"],
                config["data_manual"],
                config["data_referencia"],
                config["indices_para_calculo"][0]
            )
            
            if not correcao.get('sucesso', True):
                st.error(correcao.get('mensagem', 'Erro ao calcular correção'))
            
            # Mostrar detalhes do índice
            if 'detalhes' in correcao and not correcao['detalhes'].empty:
                st.subheader(f"Detalhes do índice {config['indices_para_calculo'][0]}")
                st.dataframe(correcao['detalhes'])
        else:
            correcao = calcular_correcao_media(
                config["valor_manual"],
                config["data_manual"],
                config["data_referencia"],
                config["indices_para_calculo"]
            )
            
            if correcao.get('indices_falha'):
                st.warning(f"Índices com problemas: {', '.join(correcao['indices_falha'])}")
            
            # Mostrar detalhes de cada índice
            if 'resultados_parciais' in correcao:
                for idx, resultado in enumerate(correcao['resultados_parciais']):
                    if 'detalhes' in resultado and not resultado['detalhes'].empty:
                        st.subheader(f"Detalhes do índice {resultado['indice']}")
                        st.dataframe(resultado['detalhes'])
                        if idx < len(correcao['resultados_parciais']) - 1:
                            st.divider()
    
    col1, col2, col3 = st.columns(3)
    
    col1.metric("Valor Original", formatar_moeda(config["valor_manual"]))
    col2.metric("Valor Corrigido", formatar_moeda(correcao["valor_corrigido"]))
    col3.metric("Variação", f"{correcao['variacao_percentual']:.2f}%")
    
    st.write(f"**Índice(s) utilizado(s):** {', '.join(correcao.get('indices', config['indices_para_calculo']))}")
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
    if st.button("Calcular Correção Monetária", type="primary", key="btn_calcular_correcao"):
        # Lista para armazenar resultados
        resultados = []
        detalhes_indices = []
        
        progress_bar = st.progress(0)  # Barra de progresso
        total_parcelas = len(processor.parcelas)
        
        for i, parcela in enumerate(processor.parcelas):
            progress_bar.progress((i + 1) / total_parcelas)
            
            valor_original = parcela.valor_original
            valor_pago = parcela.valor_pago
            data_vencimento = parse_date(parcela.data_vencimento)
            data_pagamento = parse_date(parcela.data_recebimento) if parcela.data_recebimento else None
            
            if not data_vencimento:
                st.warning(f"Data de vencimento inválida para parcela {parcela.codigo}")
                continue
            
            try:
                # Correção do valor original
                if config["metodo_correcao"] == "Índice Único":
                    correcao_original = calcular_correcao_individual(
                        valor_original,
                        data_vencimento,
                        config["data_referencia"],
                        config["indices_para_calculo"][0]
                    )
                    if not correcao_original.get('sucesso', True):
                        st.warning(f"Parcela {parcela.codigo}: {correcao_original.get('mensagem', 'Erro desconhecido')}")
                    
                    if 'detalhes' in correcao_original:
                        detalhes_indices.append({
                            'Parcela': parcela.codigo,
                            'Tipo': 'Original',
                            'Indice': config["indices_para_calculo"][0],
                            'Detalhes': correcao_original['detalhes']
                        })
                else:
                    correcao_original = calcular_correcao_media(
                        valor_original,
                        data_vencimento,
                        config["data_referencia"],
                        config["indices_para_calculo"]
                    )
                    if correcao_original.get('indices_falha'):
                        st.warning(f"Parcela {parcela.codigo}: Problemas com índices {', '.join(correcao_original['indices_falha'])}")
                    
                    if 'resultados_parciais' in correcao_original:
                        for res in correcao_original['resultados_parciais']:
                            if 'detalhes' in res:
                                detalhes_indices.append({
                                    'Parcela': parcela.codigo,
                                    'Tipo': 'Original',
                                    'Indice': res['indice'],
                                    'Detalhes': res['detalhes']
                                })
                
                # Correção do valor recebido (se houver data de pagamento)
                correcao_recebido = None
                if data_pagamento and valor_pago > 0:
                    if config["metodo_correcao"] == "Índice Único":
                        correcao_recebido = calcular_correcao_individual(
                            valor_pago,
                            data_pagamento,
                            config["data_referencia"],
                            config["indices_para_calculo"][0]
                        )
                        if not correcao_recebido.get('sucesso', True):
                            st.warning(f"Parcela {parcela.codigo} (recebido): {correcao_recebido.get('mensagem', 'Erro desconhecido')}")
                        
                        if 'detalhes' in correcao_recebido:
                            detalhes_indices.append({
                                'Parcela': parcela.codigo,
                                'Tipo': 'Recebido',
                                'Indice': config["indices_para_calculo"][0],
                                'Detalhes': correcao_recebido['detalhes']
                            })
                    else:
                        correcao_recebido = calcular_correcao_media(
                            valor_pago,
                            data_pagamento,
                            config["data_referencia"],
                            config["indices_para_calculo"]
                        )
                        if correcao_recebido.get('indices_falha'):
                            st.warning(f"Parcela {parcela.codigo} (recebido): Problemas com índices {', '.join(correcao_recebido['indices_falha'])}")
                        
                        if 'resultados_parciais' in correcao_recebido:
                            for res in correcao_recebido['resultados_parciais']:
                                if 'detalhes' in res:
                                    detalhes_indices.append({
                                        'Parcela': parcela.codigo,
                                        'Tipo': 'Recebido',
                                        'Indice': res['indice'],
                                        'Detalhes': res['detalhes']
                                    })
                
                # Adicionar ao dataframe de resultados
                resultados.append({
                    'Parcela': parcela.codigo,
                    'Dt Vencim': parcela.data_vencimento,
                    'Dt Receb': parcela.data_recebimento if parcela.data_recebimento else "",
                    'Valor Original': valor_original,
                    'Valor Original Corrigido': correcao_original['valor_corrigido'],
                    'Valor Pago': valor_pago,
                    'Valor Pago Corrigido': correcao_recebido['valor_corrigido'] if correcao_recebido else 0.0,
                    'Índice(s)': ', '.join(config["indices_para_calculo"]) if config["metodo_correcao"] == "Média de Índices" else config["indices_para_calculo"][0],
                    'Fator Correção Original': correcao_original['fator_correcao'],
                    'Fator Correção Recebido': correcao_recebido['fator_correcao'] if correcao_recebido else 0.0,
                    'Variação (%) Original': correcao_original['variacao_percentual'],
                    'Variação (%) Recebido': correcao_recebido['variacao_percentual'] if correcao_recebido else 0.0
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
                'Valor Original Corrigido': formatar_moeda,
                'Valor Pago': formatar_moeda,
                'Valor Pago Corrigido': formatar_moeda,
                'Fator Correção Original': "{:.6f}",
                'Fator Correção Recebido': "{:.6f}",
                'Variação (%) Original': "{:.2f}%",
                'Variação (%) Recebido': "{:.2f}%"
            }), use_container_width=True)
            
            # Resumo estatístico
            st.subheader("Resumo Estatístico")
            col1, col2, col3, col4 = st.columns(4)
            
            total_original = df_resultados['Valor Original'].sum()
            total_original_corrigido = df_resultados['Valor Original Corrigido'].sum()
            total_recebido = df_resultados['Valor Pago'].sum()
            total_recebido_corrigido = df_resultados['Valor Pago Corrigido'].sum()
            
            variacao_original = total_original_corrigido - total_original
            variacao_recebido = total_recebido_corrigido - total_recebido
            
            col1.metric("Total Original", formatar_moeda(total_original), formatar_moeda(variacao_original))
            col2.metric("Total Original Corrigido", formatar_moeda(total_original_corrigido))
            col3.metric("Total Recebido", formatar_moeda(total_recebido), formatar_moeda(variacao_recebido))
            col4.metric("Total Recebido Corrigido", formatar_moeda(total_recebido_corrigido))
            
            # Mostrar detalhes dos índices por parcela
            if detalhes_indices:
                st.subheader("Detalhes por Índice")
                for detalhe in detalhes_indices[:5]:  # Mostrar apenas os primeiros para não sobrecarregar
                    with st.expander(f"Detalhes para parcela {detalhe['Parcela']} - {detalhe['Tipo']} - {detalhe['Indice']}"):
                        st.dataframe(detalhe['Detalhes'])
            
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
    try:
        # Configurações da barra lateral
        config = render_sidebar()
        
        if not config:  # Se não há índices disponíveis
            return
            
        if config["modo"] == "Corrigir Valor Manual":
            render_correcao_manual(config)
        else:
            # Upload do arquivo para modo PDF
            uploaded_file = FileUploader()
            
            if uploaded_file is not None:
                processor = PDFProcessor()
                if processor.process_pdf(uploaded_file):
                    render_pdf_analysis(processor, config)
    
    except requests.exceptions.RequestException:
        st.error("Erro de conexão com a API do Banco Central. Verifique sua conexão com a internet.")
    except Exception as e:
        st.error(f"Ocorreu um erro inesperado: {str(e)}")
        
    # Adicionar nota de rodapé
    st.markdown(
        """
        <div style="text-align: right; margin-top: 50px; color: #666; font-size: 0.8em;">
            Desenvolvido por Alliabson Lourenço @2025
        </div>
        """,
        unsafe_allow_html=True
    )
    
if __name__ == "__main__":
    main()
