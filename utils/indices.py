import requests
import pandas as pd
from datetime import datetime, date
import streamlit as st
import concurrent.futures
import time
import sidrapy
import urllib3

# === CONFIGURAÇÃO CRÍTICA ===
# Desabilita avisos de segurança para conexões não verificadas (necessário para api.bcb.gov.br)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Headers para simular navegador
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Connection': 'keep-alive'
}

# Mapeamento de Séries
# PRIORIDADE: Tentar códigos do BCB primeiro, pois é mais estável.
CODIGOS_INDICES = {
    "IPCA": {
        "bcb_code": 433,      # Oficial BCB
        "ibge_table": "1737", # Oficial IBGE
        "ibge_var": "63"
    },
    "INPC": {
        "bcb_code": 188,      # Oficial BCB
        "ibge_table": "1736", # Oficial IBGE
        "ibge_var": "63"
    },
    "IGPM": {
        "bcb_code": 189,
        "ibge_table": None,
        "ibge_var": None
    },
    "INCC": {
        "bcb_code": 192,
        "ibge_table": None,
        "ibge_var": None
    }
}

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_bcb_data(series_code: int, start_date: date = None, end_date: date = None) -> pd.DataFrame:
    """Busca dados do BCB ignorando SSL e com Headers"""
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados"
    
    try:
        print(f"Tentando BCB (Série {series_code})...")
        params = {"formato": "json"}
        if start_date and end_date:
            params["dataInicial"] = start_date.strftime("%d/%m/%Y")
            params["dataFinal"] = end_date.strftime("%d/%m/%Y")

        # VERIFY=FALSE é crucial aqui
        response = requests.get(url, params=params, headers=HEADERS, timeout=10, verify=False)
        response.raise_for_status()
        
        data = response.json()
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df['data'] = pd.to_datetime(df['data'], dayfirst=True).dt.date
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce') / 100
        df = df.dropna()
        
        if start_date and end_date:
            mask = (df['data'] >= start_date) & (df['data'] <= end_date)
            df = df[mask].copy()
            
        print(f"BCB (Série {series_code}) SUCESSO: {len(df)} registros.")
        return df.sort_values('data')

    except Exception as e:
        print(f"ERRO BCB {series_code}: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_ibge_data(table_code: str, variable: str, start_date: date, end_date: date) -> pd.DataFrame:
    """Busca dados do IBGE via Sidrapy"""
    if not table_code: return pd.DataFrame()
    
    try:
        print(f"Tentando IBGE (Tabela {table_code})...")
        # Sidrapy não aceita verify=False nativamente fácil, então se falhar, falhará.
        # Mas como movemos a prioridade para o BCB, isso será usado apenas como backup real.
        data = sidrapy.get_table(
            table_code=table_code,
            territorial_level="1",
            ibge_territorial_code="all",
            variable=variable,
            period="all" # Pega tudo para garantir
        )
        
        if data is None or data.empty or 'D1C' not in data.columns:
            return pd.DataFrame()

        df = data.iloc[1:].copy()
        df['data'] = pd.to_datetime(df['D1C'], format='%Y%m', errors='coerce').dt.date
        df['valor'] = pd.to_numeric(df['V'], errors='coerce') / 100
        df = df.dropna(subset=['data', 'valor'])
        
        if start_date and end_date:
            mask = (df['data'] >= start_date) & (df['data'] <= end_date)
            df = df[mask].copy()

        print(f"IBGE (Tabela {table_code}) SUCESSO: {len(df)} registros.")
        return df.sort_values('data')

    except Exception as e:
        print(f"ERRO IBGE {table_code}: {str(e)}")
        return pd.DataFrame()

def get_indice_data(indice: str, start_date: date, end_date: date) -> pd.DataFrame:
    """Orquestrador: Tenta BCB PRIMEIRO (mais rápido/estável), depois IBGE"""
    config = CODIGOS_INDICES.get(indice)
    if not config: return pd.DataFrame()

    # 1. Tenta BCB (Séries espelhadas do IBGE existem no BCB e são JSON simples)
    if config.get("bcb_code"):
        df = fetch_bcb_data(config["bcb_code"], start_date, end_date)
        if not df.empty:
            return df
    
    # 2. Se falhar e tiver configuração IBGE, tenta Sidra
    if config.get("ibge_table"):
        df = fetch_ibge_data(config["ibge_table"], config["ibge_var"], start_date, end_date)
        if not df.empty:
            return df

    return pd.DataFrame()

def get_indices_disponiveis():
    """
    Verifica disponibilidade de forma simplificada.
    Se falhar na verificação profunda, retorna a lista padrão para não travar a UI.
    """
    nomes = {
        "IPCA": "IPCA (IBGE/BCB)",
        "IGPM": "IGP-M (FGV/BCB)",
        "INPC": "INPC (IBGE/BCB)",
        "INCC": "INCC (FGV/BCB)"
    }
    
    # Tentativa Rápida: Verificar apenas conexão com Google
    try:
        requests.get("https://www.google.com", timeout=3)
    except:
        st.error("Sem conexão com a internet detectada.")
        return {}

    # Em vez de testar todos e bloquear, vamos assumir que estão disponíveis
    # e deixar o erro explodir apenas na hora do cálculo se necessário.
    # Isso evita que o sidebar fique vazio.
    
    # Faz um teste leve apenas no IGPM (série 189) para ver se a API do BCB responde
    try:
        teste = fetch_bcb_data(189)
        if teste.empty:
            st.warning("Atenção: A API do Banco Central parece instável, mas tentaremos calcular.")
    except:
        pass

    return nomes

# --- Funções de Cálculo (Mantidas iguais para garantir compatibilidade) ---

def calcular_correcao_individual(valor_original, data_inicio: date, data_fim: date, indice: str):
    if data_inicio >= data_fim:
        return {'sucesso': True, 'valor_corrigido': valor_original, 'fator_correcao': 1.0, 'variacao_percentual': 0.0, 'mensagem': 'Data inicial >= Data final'}

    df = get_indice_data(indice, data_inicio, data_fim)
    
    if df.empty:
        return {'sucesso': False, 'mensagem': f'Sem dados para {indice}. A API pode estar fora do ar.', 'valor_corrigido': valor_original}

    fator = (1 + df['valor']).prod()
    valor_corrigido = valor_original * fator
    
    return {
        'sucesso': True,
        'valor_corrigido': valor_corrigido,
        'fator_correcao': fator,
        'variacao_percentual': (fator - 1) * 100,
        'detalhes': df,
        'indices': [indice]
    }

def calcular_correcao_media(valor_original, data_inicio: date, data_fim: date, indices: list):
    resultados_individuais = []
    
    for idx in indices:
        res = calcular_correcao_individual(valor_original, data_inicio, data_fim, idx)
        if res['sucesso']:
            resultados_individuais.append(res['fator_correcao'])
    
    if not resultados_individuais:
        return {'sucesso': False, 'mensagem': 'Nenhum índice retornou dados.', 'valor_corrigido': valor_original}
        
    fator_medio = sum(resultados_individuais) / len(resultados_individuais)
    valor_corrigido = valor_original * fator_medio
    
    return {
        'sucesso': True,
        'valor_corrigido': valor_corrigido,
        'fator_correcao': fator_medio,
        'variacao_percentual': (fator_medio - 1) * 100,
        'indices': indices
    }

def formatar_moeda(valor):
    if valor is None: return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
