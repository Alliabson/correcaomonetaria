import streamlit as st
import pdfplumber
import pandas as pd
import re
from datetime import datetime, date
from typing import Dict, List, Optional
import base64
import pytz
from io import BytesIO

# Configuração da página
st.set_page_config(page_title="Correção Monetária Completa", layout="wide")
st.title("📈 Correção Monetária Completa")

# Importações
try:
    from utils.indices import (
        get_indices_disponiveis,
        calcular_correcao_individual,
        calcular_correcao_media,
        formatar_moeda,
        limpar_cache
    )
except ImportError:
    st.error("Erro ao importar utils. Verifique a estrutura.")
    st.stop()

# ===== Classes =====
class Cliente:
    def __init__(self, codigo: str = "N/A", nome: str = "Não Identificado"):
        self.codigo = codigo
        self.nome = nome

class Venda:
    def __init__(self, numero: str = "", data: str = "", valor: float = 0.0):
        self.numero = numero
        self.data = data
        self.valor = valor

class Parcela:
    def __init__(self, codigo: str, data_vencimento: str, valor_original: float, valor_pago: float = 0.0):
        self.codigo = codigo
        self.data_vencimento = data_vencimento
        self.valor_original = valor_original
        self.valor_pago = valor_pago

# ===== Utils =====
def parse_date(date_str: str) -> Optional[date]:
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").date()
    except:
        return None

def parse_monetary(value_str: str) -> float:
    if not value_str: return 0.0
    try:
        clean = re.sub(r'[R$\s]', '', str(value_str))
        if ',' in clean and '.' in clean: # 1.000,00
            return float(clean.replace('.', '').replace(',', '.'))
        if ',' in clean: # 1000,00
            return float(clean.replace(',', '.'))
        return float(clean)
    except:
        return 0.0

# ===== Processador PDF Inteligente =====
class PDFProcessor:
    def __init__(self):
        self.cliente = Cliente()
        self.venda = Venda()
        self.parcelas: List[Parcela] = []
        self.full_text = ""
        self.total_original = 0.0
        self.total_recebido = 0.0

    def process_pdf(self, file: bytes) -> bool:
        try:
            with pdfplumber.open(file) as pdf:
                self.full_text = "\n".join([page.extract_text() or "" for page in pdf.pages])
            
            # Mostra texto bruto em expander para debug do usuário
            with st.expander("🔍 Debug: Ver texto extraído do PDF"):
                st.text(self.full_text)
            
            self._extract_cliente()
            self._extract_venda()
            self._extract_parcelas_smart() # Nova função inteligente
            self._calculate_totais()
            return True
        except Exception as e:
            st.error(f"Erro no processamento: {str(e)}")
            return False

    def _extract_cliente(self):
        # Tenta achar padrão "Cliente: 001 - Nome" ou só "Cliente: Nome"
        match = re.search(r'Cliente\s*[:\.]?\s*(\d+)?\s*[-–]?\s*([^\n\r]+)', self.full_text, re.IGNORECASE)
        if match:
            cod = match.group(1) if match.group(1) else "N/A"
            nom = match.group(2).strip()
            self.cliente = Cliente(cod, nom)

    def _extract_venda(self):
        match = re.search(r'Venda\s*[:\.]?\s*(\d+)', self.full_text, re.IGNORECASE)
        if match:
            self.venda.numero = match.group(1)

    def _extract_parcelas_smart(self):
        """
        Scanner linha por linha que ignora lixo.
        Regra: Linha deve ter uma DATA e um VALOR.
        Ignora linhas com 'Total', 'Saldo', 'Juros'.
        """
        lines = self.full_text.split('\n')
        self.parcelas = []
        
        re_date = r'(\d{2}/\d{2}/\d{4})'
        re_money = r'(\d{1,3}(?:\.\d{3})*,\d{2})'
        re_index = r'(\d{1,3}/\d{1,3})' # ex: 01/12

        for line in lines:
            line_clean = line.strip()
            
            # Filtro de exclusão: Palavras que indicam cabeçalho ou rodapé
            blacklist = ['total', 'soma', 'saldo', 'resumo', 'página', 'relatório', 'venda:', 'cliente:', 'aberto']
            if any(word in line_clean.lower() for word in blacklist):
                continue

            dates = re.findall(re_date, line_clean)
            values = re.findall(re_money, line_clean)
            index = re.search(re_index, line_clean)

            # Critério rigoroso: Precisa ter pelo menos 1 Data e 1 Valor
            if dates and values:
                # Tenta definir o código da parcela (ex: 1/12) ou usa a data
                cod_parcela = index.group(1) if index else f"Parc {len(self.parcelas)+1}"
                
                dt_venc = dates[0] # Assume a primeira data como vencimento
                val_orig = parse_monetary(values[0])
                
                # Se tiver dois valores, o segundo pode ser o pago. 
                # Se tiver data de pagamento, tenta achar
                val_pago = 0.0
                dt_pag = None
                
                if len(values) > 1:
                    # Lógica comum: Valor Original ... Valor Pago
                    val_pago = parse_monetary(values[-1])
                
                if len(dates) > 1:
                    dt_pag = dates[1] # Assume segunda data como pagamento
                
                p = Parcela(cod_parcela, dt_venc, val_orig, val_pago)
                # Salva data recebimento se tiver
                if dt_pag: p.data_recebimento = dt_pag
                
                self.parcelas.append(p)

        if not self.parcelas:
            st.warning("⚠️ Nenhuma parcela identificada automaticamente.")

    def _calculate_totais(self):
        self.total_original = sum(p.valor_original for p in self.parcelas)
        self.total_recebido = sum(p.valor_pago for p in self.parcelas)

# ===== UI =====
def render_sidebar():
    st.sidebar.header("Configurações")
    if st.sidebar.button("🧹 Limpar Cache de Índices"):
        limpar_cache()
        st.rerun()

    indices = get_indices_disponiveis()
    
    modo = st.sidebar.radio("Modo", ["PDF", "Manual"])
    metodo = st.sidebar.radio("Cálculo", ["Índice Único", "Média"])
    
    opcoes = list(indices.keys())
    selecionados = []
    
    if metodo == "Índice Único":
        idx = st.sidebar.selectbox("Índice", options=opcoes)
        if idx: selecionados = [idx]
    else:
        selecionados = st.sidebar.multiselect("Índices", options=opcoes, default=opcoes)
        
    dt_ref = st.sidebar.date_input("Data Referência", value=date.today())
    
    return {"modo": modo, "metodo": metodo, "indices": selecionados, "data_ref": dt_ref}

def render_manual(config):
    st.subheader("Entrada Manual")
    if "manuais" not in st.session_state: st.session_state.manuais = []
    
    c1, c2, c3 = st.columns([2,2,1])
    val = c1.number_input("Valor", value=1000.0)
    dt = c2.date_input("Data", value=date(2023,1,1))
    if c3.button("Adicionar"):
        st.session_state.manuais.append({'valor': val, 'data': dt})
        st.rerun()
        
    if st.session_state.manuais:
        df = pd.DataFrame(st.session_state.manuais)
        st.dataframe(df)
        if st.button("Calcular"):
            res = []
            for item in st.session_state.manuais:
                if config['metodo'] == 'Índice Único':
                    r = calcular_correcao_individual(item['valor'], item['data'], config['data_ref'], config['indices'][0])
                else:
                    r = calcular_correcao_media(item['valor'], item['data'], config['data_ref'], config['indices'])
                
                if r['sucesso']:
                    res.append({
                        'Original': item['valor'],
                        'Data': item['data'],
                        'Corrigido': r['valor_corrigido'],
                        'Fator': r['fator_correcao']
                    })
            st.dataframe(pd.DataFrame(res))

def render_pdf_results(proc, config):
    st.divider()
    st.subheader(f"Cliente: {proc.cliente.nome}")
    
    # Exibe tabela limpa das parcelas encontradas ANTES de calcular
    df_raw = pd.DataFrame([{
        'Parcela': p.codigo, 
        'Vencimento': p.data_vencimento, 
        'Valor': p.valor_original
    } for p in proc.parcelas])
    
    st.caption(f"Foram identificadas {len(proc.parcelas)} parcelas válidas.")
    with st.expander("Ver dados extraídos (Conferência)"):
        st.dataframe(df_raw)

    if st.button("🚀 Calcular Correção", type="primary"):
        res_list = []
        bar = st.progress(0)
        
        for i, p in enumerate(proc.parcelas):
            bar.progress((i+1)/len(proc.parcelas))
            dt_venc = parse_date(p.data_vencimento)
            if not dt_venc: continue
            
            if config['metodo'] == 'Índice Único':
                r = calcular_correcao_individual(p.valor_original, dt_venc, config['data_ref'], config['indices'][0])
            else:
                r = calcular_correcao_media(p.valor_original, dt_venc, config['data_ref'], config['indices'])
            
            res_list.append({
                'Parcela': p.codigo,
                'Vencimento': p.data_vencimento,
                'Valor Original': formatar_moeda(p.valor_original),
                'Valor Corrigido': formatar_moeda(r.get('valor_corrigido', 0)),
                'Fator': r.get('fator_correcao', 1)
            })
            
        st.success("Cálculo Finalizado")
        df_final = pd.DataFrame(res_list)
        st.dataframe(df_final)
        
        # Export
        csv = df_final.to_csv(index=False).encode('utf-8')
        st.download_button("Download CSV", csv, "calculo.csv", "text/csv")

def main():
    config = render_sidebar()
    if not config: return

    if config['modo'] == 'Manual':
        render_manual(config)
    else:
        f = st.file_uploader("Upload PDF", type="pdf")
        if f:
            proc = PDFProcessor()
            if proc.process_pdf(f):
                render_pdf_results(proc, config)

if __name__ == "__main__":
    main()
