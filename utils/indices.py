import requests
import pandas as pd
from datetime import datetime, date
import streamlit as st
import concurrent.futures
import time
from typing import Dict, List
import json
import os
import sqlite3
import urllib3

# Desabilita avisos de SSL (Crucial para APIs gov.br)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==============================================================================
# CONFIGURAÇÕES DE API (A Mágica acontece aqui)
# ==============================================================================
# O IPEA não bloqueia o Streamlit Cloud. O BCB bloqueia.
# Mapeamos os códigos do IPEA que correspondem aos do BCB.

MAPA_CODIGOS = {
    "IPCA": {
        "ipea": "PRECOS12_IPCA12",  # Código IPEA
        "bcb": "433"                # Código BCB
    },
    "INPC": {
        "ipea": "PRECOS12_INPC12",
        "bcb": "188"
    },
    "IGPM": {
        "ipea": "IGP12_IGPM12",
        "bcb": "189"
    },
    "INCC": {
        "ipea": "IGP12_INCC12",
        "bcb": "192"
    },
    "SELIC": {
        "ipea": "BM12_TJOVER12",    # Selic acumulada mensal
        "bcb": "4390"
    }
}

# ==============================================================================
# SISTEMA DE CACHE (SQLite Simples)
# ==============================================================================
class CacheManager:
    def __init__(self, db_path="indices_cache_v3.db"):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS cache_indices (
                    chave TEXT PRIMARY KEY,
                    dados TEXT,
                    timestamp REAL
                )
            ''')
            conn.commit()
            conn.close()
        except: pass
    
    def get(self, chave):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            # Cache válido por 24 horas (86400 segundos)
            cursor.execute("SELECT dados, timestamp FROM cache_indices WHERE chave = ?", (chave,))
            row = cursor.fetchone()
            conn.close()
            if row:
                dados, ts = row
                if time.time() - ts < 86400: # 24h
                    return json.loads(dados)
            return None
        except: return None
    
    def set(self, chave, dados):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO cache_indices (chave, dados, timestamp) VALUES (?, ?, ?)",
                (chave, json.dumps(dados), time.time())
            )
            conn.commit()
            conn.close()
        except: pass

cache = CacheManager()

# ==============================================================================
# FUNÇÕES DE BUSCA (A Lógica Robusta)
# ==============================================================================

def buscar_ipea(codigo_ipea: str) -> pd.DataFrame:
    """
    Busca dados no IPEA (API OData). 
    Esta API NÃO BLOQUEIA o Streamlit Cloud.
    """
    url = f"http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{codigo_ipea}')"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if 'value' in data:
                df = pd.DataFrame(data['value'])
                # O IPEA retorna colunas: VALDATA (Data) e VALVALOR (Valor)
                df = df.rename(columns={'VALDATA': 'data', 'VALVALOR': 'valor'})
                
                # Tratamento de Data
                df['data'] = pd.to_datetime(df['data']).dt.date
                
                # Tratamento de Valor (IPEA manda percentual ex: 0.53)
                df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
                df['valor'] = df['valor'] / 100 # Converte para decimal
                
                # Filtro (Pós Real)
                df = df[df['data'] >= date(1995, 1, 1)]
                return df[['data', 'valor']].sort_values('data')
    except Exception as e:
        print(f"Erro IPEA: {e}")
    
    return pd.DataFrame()

def buscar_bcb_csv(codigo_bcb: str) -> pd.DataFrame:
    """
    Backup: Tenta baixar o CSV do BCB em vez de usar a API JSON.
    O endpoint de arquivo às vezes tem regras de firewall mais brandas.
    """
    url = f"http://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_bcb}/dados?formato=csv"
    
    try:
        # Pandas read_csv lida melhor com conexões diretas de arquivo
        df = pd.read_csv(
            url, 
            sep=';', 
            decimal=',', 
            encoding='utf-8',
            names=['data', 'valor'],
            header=0
        )
        
        df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y', errors='coerce').dt.date
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        
        # Tratamento de escala (BCB manda 0.53 para 0.53%)
        # Se média > 0.1, divide por 100
        if df['valor'].mean() > 0.1:
            df['valor'] = df['valor'] / 100
            
        df = df[df['data'] >= date(1995, 1, 1)]
        return df.dropna().sort_values('data')
        
    except Exception as e:
        print(f"Erro BCB CSV: {e}")
        
    return pd.DataFrame()

def buscar_dados_indice(indice_nome: str, data_inicio: date, data_final: date) -> pd.DataFrame:
    """Orquestrador: Tenta Cache -> IPEA -> BCB CSV"""
    
    # 1. Tenta Cache
    chave = f"{indice_nome}_{data_inicio}_{data_final}"
    dados_cache = cache.get(chave)
    if dados_cache:
        df = pd.DataFrame(dados_cache)
        df['data'] = pd.to_datetime(df['data']).dt.date # Recupera objeto date
        return df

    codigos = MAPA_CODIGOS.get(indice_nome)
    if not codigos: return pd.DataFrame()

    df_final = pd.DataFrame()

    # 2. Tenta IPEA (Prioridade Alta - Funciona na Nuvem)
    df_ipea = buscar_ipea(codigos['ipea'])
    if not df_ipea.empty:
        df_final = df_ipea

    # 3. Se IPEA falhar, Tenta BCB CSV (Backup)
    if df_final.empty:
        df_bcb = buscar_bcb_csv(codigos['bcb'])
        if not df_bcb.empty:
            df_final = df_bcb

    # Filtra pelo período solicitado e Salva Cache
    if not df_final.empty:
        mask = (df_final['data'] >= data_inicio) & (df_final['data'] <= data_final)
        df_filtrado = df_final.loc[mask].copy()
        
        # Converte datas para string para salvar no JSON do cache
        df_cache = df_filtrado.copy()
        df_cache['data'] = df_cache['data'].astype(str)
        cache.set(chave, df_cache.to_dict('records'))
        
        return df_filtrado

    return pd.DataFrame()

# ==============================================================================
# FUNÇÕES EXPOSTAS AO APP
# ==============================================================================

@st.cache_data(ttl=3600)
def get_indices_disponiveis() -> Dict[str, dict]:
    hoje = date.today()
    data_teste = date(hoje.year - 1, hoje.month, 1)
    
    st.sidebar.caption("Sincronizando IPEA/BCB...")
    progress = st.sidebar.progress(0)
    
    indices_status = {}
    total = len(MAPA_CODIGOS)
    
    # Execução em Paralelo para ser rápido
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(buscar_dados_indice, nome, data_teste, hoje): nome
            for nome in MAPA_CODIGOS.keys()
        }
        
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            nome = futures[future]
            try:
                df = future.result()
                disponivel = not df.empty
                
                # Feedback Visual da Taxa
                txt_taxa = ""
                if disponivel:
                    ult_val = df.iloc[-1]['valor'] * 100
                    txt_taxa = f"({ult_val:.2f}%)"
                
                indices_status[nome] = {
                    'nome': f"{nome} {txt_taxa}",
                    'disponivel': disponivel,
                    'ultima_data': df['data'].max().strftime("%m/%Y") if disponivel else "-"
                }
            except:
                indices_status[nome] = {'nome': nome, 'disponivel': False}
            
            progress.progress((i + 1) / total)
            
    progress.empty()
    return indices_status

def calcular_correcao_individual(valor: float, data_original: date, data_referencia: date, indice: str) -> dict:
    if data_original >= data_referencia:
        return {'sucesso': True, 'valor_corrigido': valor, 'fator_correcao': 1.0, 'variacao_percentual': 0.0, 'indices': [indice]}
    
    # Busca dados (Já filtra o período básico na busca)
    # Pegamos uma margem de segurança nas datas para garantir que temos o mês inicial
    dt_busca_ini = date(data_original.year, data_original.month, 1)
    df = buscar_dados_indice(indice, dt_busca_ini, data_referencia)
    
    if df.empty:
        return {'sucesso': False, 'mensagem': f'Indisponível: {indice}', 'valor_corrigido': valor}
    
    # Lógica de Correção: Mês Inicial INCLUSO, Mês Final EXCLUSO (Padrão TJ)
    dt_fim_corte = date(data_referencia.year, data_referencia.month, 1)
    
    mask = (df['data'] >= dt_busca_ini) & (df['data'] < dt_fim_corte)
    subset = df.loc[mask]
    
    if subset.empty:
        # Se datas iguais (mesmo mês), correção é zero
        if dt_busca_ini == dt_fim_corte:
             return {'sucesso': True, 'valor_corrigido': valor, 'fator_correcao': 1.0, 'variacao_percentual': 0.0}
        return {'sucesso': False, 'mensagem': 'Período sem dados', 'valor_corrigido': valor}

    # Cálculo Acumulado
    fator = (1 + subset['valor']).prod()
    
    # Trava de Sanidade (Se o fator for > 1000x, tem erro de dados)
    if fator > 1000:
        return {'sucesso': False, 'mensagem': 'Erro: Taxa explosiva detectada', 'valor_corrigido': valor}
        
    valor_corrigido = valor * fator
    
    return {
        'sucesso': True,
        'valor_corrigido': valor_corrigido,
        'fator_correcao': fator,
        'variacao_percentual': (fator - 1) * 100,
        'indice': indice
    }

def calcular_correcao_media(valor: float, data_original: date, data_referencia: date, indices: List[str]) -> dict:
    import math
    if not indices: return {'sucesso': False, 'mensagem': 'Nenhum índice'}
    
    fatores = []
    sucessos = []
    
    for ind in indices:
        res = calcular_correcao_individual(valor, data_original, data_referencia, ind)
        if res['sucesso']:
            fatores.append(res['fator_correcao'])
            sucessos.append(ind)
            
    if not fatores: return {'sucesso': False, 'mensagem': 'Falha nos índices'}
    
    # Média Geométrica
    prod = math.prod(fatores)
    fator_medio = prod ** (1/len(fatores))
    
    return {
        'sucesso': True,
        'valor_corrigido': valor * fator_medio,
        'fator_correcao': fator_medio,
        'variacao_percentual': (fator_medio - 1) * 100,
        'indices': sucessos
    }

def formatar_moeda(valor: float) -> str:
    if not valor: return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def limpar_cache():
    try:
        if os.path.exists("indices_cache_v3.db"):
            os.remove("indices_cache_v3.db")
        st.success("Cache limpo!")
    except: pass
