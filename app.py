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

# Configuração da página
st.set_page_config(page_title="Correção Monetária Completa", layout="wide")
st.title("📈 Correção Monetária Completa")

# Tenta importar os módulos
try:
    from utils.indices import (
        get_indices_disponiveis,
        calcular_correcao_individual,
        calcular_correcao_media,
        formatar_moeda
    )
except ImportError:
    st.error("Erro na importação dos módulos locais (utils). Verifique a pasta.")
    st.stop()

# ===== Classes de Dados =====
class Cliente:
    def __init__(self, codigo: str = "", nome: str = ""):
        self.codigo = codigo
        self.nome = nome

class Parcela:
    def __init__(self, codigo: str, data_vencimento: str, valor_original: float, valor_pago: float = 0.0):
        self.codigo = codigo
        self.data_vencimento = data_vencimento
        self.valor_original = valor_original
        self.valor_pago = valor_pago

# ===== Funções Auxiliares =====
def parse_date(date_str: str) -> Optional[date]:
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").date()
    except:
        return None

def parse_monetary(value_str: str) -> float:
    """Converte string '1.200,50' para float 1200.50"""
    if not value_str: return 0.0
    try:
        # Remove símbolos de moeda e espaços
        clean = re.sub(r'[R$\s]', '', value_str)
        # Se tiver ponto e vírgula, assume padrão BR (1.000,00)
        if ',' in clean and '.' in clean:
            return float(clean.replace('.', '').replace(',', '.'))
        # Se só tiver vírgula (1000,00)
        elif ',' in clean:
            return float(clean.replace(',', '.'))
        # Se só tiver ponto, verifica se é separador de milhar ou decimal
        # (Lógica simples: se tem 3 casas após ponto, é milhar, senão é decimal - arriscado, mas padrão US)
        return float(clean)
    except:
        return 0.0

# ===== Processador de PDF Inteligente =====
class PDFProcessor:
    def __init__(self):
        self.cliente = Cliente()
        self.parcelas: List[Parcela] = []
        self.full_text = ""

    def process_pdf(self, file: bytes) -> bool:
        try:
            with pdfplumber.open(file) as pdf:
                self.full_text = "\n".join([page.extract_text() or "" for page in pdf.pages])
            
            # Mostra o texto para debug (ajuda a identificar o layout)
            with st.expander("🔍 Ver texto bruto extraído (Debug)", expanded=False):
                st.code(self.full_text)
            
            self._find_cliente()
            self._find_parcelas_smart()
            
            return True
        except Exception as e:
            st.error(f"Erro crítico no PDF: {str(e)}")
            return False

    def _find_cliente(self):
        # Tenta vários padrões comuns de cabeçalho
        patterns = [
            r'Cliente\s*:\s*(\d+)\s*[-–]\s*([^\n]+)',  # Cliente: 001 - Nome
            r'Nome\s*:\s*([^\n]+)',                     # Nome: Fulano
            r'Sacado\s*:\s*([^\n]+)'                    # Sacado: Fulano
        ]
        
        for p in patterns:
            match = re.search(p, self.full_text, re.IGNORECASE)
            if match:
                if len(match.groups()) == 2:
                    self.cliente = Cliente(match.group(1), match.group(2).strip())
                else:
                    self.cliente = Cliente("N/A", match.group(1).strip())
                return
        
        self.cliente = Cliente("Não identificado", "Não identificado")

    def _find_parcelas_smart(self):
        """
        Lógica: Lê linha por linha. Se a linha tiver:
        1. Algo que pareça índice (1/12, 01/24)
        2. Uma data válida
        3. Um valor monetário
        Então é uma parcela.
        """
        lines = self.full_text.split('\n')
        self.parcelas = []
        
        # Regex para identificar "X/Y" ou "P.X/Y"
        re_index = r'(?:PARC|P|Nº)?\.?\s*(\d{1,3}/\d{1,3})'
        # Regex para data dd/mm/aaaa
        re_date = r'(\d{2}/\d{2}/\d{4})'
        # Regex para dinheiro (pega sequencias numéricas que parecem dinheiro)
        re_money = r'(\d{1,3}(?:\.\d{3})*,\d{2})'

        for line in lines:
            # Pula linhas de totais
            if 'total' in line.lower() or 'soma' in line.lower():
                continue

            # Tenta encontrar os componentes na linha
            match_index = re.search(re_index, line, re.IGNORECASE)
            dates = re.findall(re_date, line)
            values = re.findall(re_money, line)
            
            # Critério mínimo: Ter índice (1/12), pelo menos 1 data e 1 valor
            if match_index and dates and values:
                codigo = match_index.group(1)
                dt_venc = dates[0] # Assume a primeira data como vencimento
                
                # Assume o primeiro valor como original
                val_original = parse_monetary(values[0])
                
                # Se tiver um segundo valor, pode ser o valor pago ou juros, etc.
                # Aqui simplificamos: se tiver valor pago explícito na mesma linha
                val_pago = 0.0
                if len(values) > 1:
                    # Lógica simples: o maior valor costuma ser o pago (com juros) ou o original
                    # Vamos pegar o segundo como pago por enquanto
                    val_pago = parse_monetary(values[-1]) 

                p = Parcela(codigo, dt_venc, val_original, val_pago)
                self.parcelas.append(p)

        if not self.parcelas:
            st.warning("⚠️ O sistema leu o arquivo mas não reconheceu as linhas de parcelas.")
            st.info("Dica: Verifique se o PDF é uma imagem (escaneado). Se for, o sistema não consegue ler o texto.")

# ===== Interface =====
def render_sidebar():
    st.sidebar.header("Configurações")
    if st.sidebar.button("🧹 Limpar Cache", type="primary"):
        st.cache_data.clear()
        st.rerun()
    
    st.sidebar.divider()
    
    # Carrega índices (assumindo que utils.indices já foi corrigido conforme conversa anterior)
    indices = get_indices_disponiveis()
    if not indices:
        st.sidebar.warning("Usando modo offline (sem índices automáticos) ou erro de conexão.")
        return None

    metodo = st.sidebar.radio("Método", ["Índice Único", "Média"])
    
    selecionados = []
    if metodo == "Índice Único":
        idx = st.sidebar.selectbox("Índice", list(indices.keys()))
        selecionados = [idx]
    else:
        selecionados = st.sidebar.multiselect("Índices", list(indices.keys()), default=list(indices.keys()))
    
    dt_ref = st.sidebar.date_input("Data Referência", value=date.today())
    
    return {"metodo": metodo, "indices": selecionados, "data_ref": dt_ref}

def main():
    config = render_sidebar()
    
    uploaded_file = st.file_uploader("📂 Carregar PDF", type="pdf")
    
    if uploaded_file and config:
        proc = PDFProcessor()
        if proc.process_pdf(uploaded_file):
            
            # Exibe Cliente
            st.subheader(f"Cliente: {proc.cliente.nome}")
            
            # Tabela de Parcelas Encontradas
            if proc.parcelas:
                df_parcelas = pd.DataFrame([vars(p) for p in proc.parcelas])
                st.write(f"**{len(proc.parcelas)} parcelas encontradas:**")
                st.dataframe(df_parcelas)
                
                if st.button("🚀 Calcular Correção"):
                    results = []
                    progress = st.progress(0)
                    
                    for i, p in enumerate(proc.parcelas):
                        dt_venc = parse_date(p.data_vencimento)
                        if not dt_venc: continue
                        
                        if config["metodo"] == "Índice Único":
                            res = calcular_correcao_individual(
                                p.valor_original, dt_venc, config["data_ref"], config["indices"][0]
                            )
                        else:
                            res = calcular_correcao_media(
                                p.valor_original, dt_venc, config["data_ref"], config["indices"]
                            )
                        
                        results.append({
                            "Parcela": p.codigo,
                            "Vencimento": p.data_vencimento,
                            "Valor Orig.": p.valor_original,
                            "Valor Corr.": res.get("valor_corrigido", 0),
                            "Fator": res.get("fator_correcao", 1)
                        })
                        progress.progress((i+1)/len(proc.parcelas))
                    
                    df_res = pd.DataFrame(results)
                    st.success("Cálculo Concluído!")
                    st.dataframe(df_res.style.format({"Valor Orig.": "R$ {:.2f}", "Valor Corr.": "R$ {:.2f}", "Fator": "{:.4f}"}))
            else:
                st.error("Não consegui identificar o padrão das parcelas. Por favor, copie o texto do 'Debug' acima e envie para o suporte.")

if __name__ == "__main__":
    main()
