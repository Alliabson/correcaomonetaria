import pandas as pd
from datetime import datetime, date
import streamlit as st
import sidrapy
import requests
import logging
import sqlite3
import json
import os
import time

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Indices")

# ==============================================================================
# 1. CONFIGURAÇÃO DAS FONTES
# ==============================================================================
CONFIG_FONTES = {
    "IPCA": {
        "metodo": "IBGE",
        "params": {"tabela": "1737", "variavel": "63", "geral": "2265"}
    },
    "INPC": {
        "metodo": "IBGE",
        "params": {"tabela": "1736", "variavel": "44", "geral": "2289"}
    },
    "IGPM": {
        "metodo": "IPEA",
        "codigo": "IGP12_IGPM12"
    },
    "INCC": {
        "metodo": "IPEA",
        "codigo": "IGP12_INCC12"
    },
    "SELIC": {
        "metodo": "IPEA",
        "codigo": "BM12_TJOVER12"
    }
}

# ==============================================================================
# 2. FUNÇÕES DE COLETA PADRONIZADAS
# ==============================================================================

def _padronizar_dataframe(df: pd.DataFrame, origem: str) -> pd.DataFrame:
    if df.empty: return df

    try:
        df['data'] = pd.to_datetime(df['data']).dt.date
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        df = df.dropna()
        
        # Regra de Escala: Se média > 0.5 (50%), divide por 100
        media_valores = df['valor'].abs().mean()
        if media_valores > 0.5:
            df['valor'] = df['valor'] / 100
            
        return df[['data', 'valor']].sort_values('data')
    except Exception as e:
        logger.error(f"Erro ao padronizar ({origem}): {e}")
        return pd.DataFrame()

def buscar_ibge_sidra(params: dict) -> pd.DataFrame:
    try:
        data = sidrapy.get_table(
            table_code=params['tabela'],
            territorial_level="1",
            ibge_territorial_code="all",
            variable=params['variavel'],
            period="last 120",
            classifications={"315": params['geral']}
        )
        
        if data.empty or 'V' not in data.columns: return pd.DataFrame()
            
        df = data.iloc[1:].copy()
        df_final = pd.DataFrame()
        df_final['valor'] = df['V']
        df_final['data'] = pd.to_datetime(df['D2C'], format="%Y%m", errors='coerce')
        
        return _padronizar_dataframe(df_final, "IBGE")
    except Exception as e:
        logger.error(f"Erro IBGE: {e}")
        return pd.DataFrame()

def buscar_ipea_api(codigo: str) -> pd.DataFrame:
    url = f"http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{codigo}')"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            dados = response.json()
            if 'value' in dados and len(dados['value']) > 0:
                df = pd.DataFrame(dados['value'])
                df_final = pd.DataFrame()
                df_final['data'] = df['VALDATA']
                df_final['valor'] = df['VALVALOR']
                return _padronizar_dataframe(df_final, "IPEA")
    except Exception as e:
        logger.error(f"Erro IPEA ({codigo}): {e}")
    return pd.DataFrame()

# ==============================================================================
# 3. GERENCIAMENTO DE CACHE (COM CORREÇÃO DE INICIALIZAÇÃO)
# ==============================================================================

def _inicializar_sessao():
    """Garante que a variável de sessão exista antes de usar"""
    if 'cache_indices_ram' not in st.session_state:
        st.session_state['cache_indices_ram'] = {}

def get_dados_indice(nome_indice: str) -> pd.DataFrame:
    # 1. Garante inicialização
    _inicializar_sessao()
    
    # 2. Verifica memória RAM (usando sintaxe de dicionário que é mais segura)
    if nome_indice in st.session_state['cache_indices_ram']:
        return st.session_state['cache_indices_ram'][nome_indice]
    
    # 3. Se não tem, busca na fonte
    config = CONFIG_FONTES.get(nome_indice)
    if not config: return pd.DataFrame()
    
    df = pd.DataFrame()
    if config['metodo'] == "IBGE":
        df = buscar_ibge_sidra(config['params'])
    elif config['metodo'] == "IPEA":
        df = buscar_ipea_api(config['codigo'])
        
    # 4. Salva na memória e retorna
    if not df.empty:
        st.session_state['cache_indices_ram'][nome_indice] = df
        
    return df

# ==============================================================================
# 4. FUNÇÕES PARA O APP
# ==============================================================================

def get_indices_disponiveis() -> dict:
    status = {}
    prog = st.sidebar.progress(0)
    total = len(CONFIG_FONTES)
    
    for i, nome in enumerate(CONFIG_FONTES.keys()):
        df = get_dados_indice(nome)
        
        disponivel = not df.empty
        ultima_data = "-"
        if disponivel:
            ultima_data = df['data'].max().strftime("%m/%Y")
            
        status[nome] = {
            "nome": nome,
            "disponivel": disponivel,
            "ultima_data": ultima_data
        }
        prog.progress((i + 1) / total)
        
    prog.empty()
    return status

def formatar_moeda(valor):
    if not valor: return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def limpar_cache():
    # Reinicializa forçadamente
    st.session_state['cache_indices_ram'] = {}
    st.success("Cache limpo!")

# ==============================================================================
# 5. CÁLCULO
# ==============================================================================

def calcular_correcao_individual(valor: float, data_original: date, data_referencia: date, indice: str) -> dict:
    if data_original >= data_referencia:
        return {'sucesso': True, 'valor_corrigido': valor, 'fator_correcao': 1.0, 'variacao_percentual': 0.0, 'indices': [indice]}
    
    df = get_dados_indice(indice)
    
    if df.empty:
        return {'sucesso': False, 'mensagem': f'Índice {indice} sem dados', 'valor_corrigido': valor}
    
    dt_inicio = date(data_original.year, data_original.month, 1)
    dt_fim = date(data_referencia.year, data_referencia.month, 1)
    
    mask = (df['data'] >= dt_inicio) & (df['data'] < dt_fim)
    subset = df.loc[mask]
    
    if subset.empty:
        if dt_inicio == dt_fim:
            return {'sucesso': True, 'valor_corrigido': valor, 'fator_correcao': 1.0, 'variacao_percentual': 0.0, 'indices': [indice]}
        return {'sucesso': False, 'mensagem': 'Período sem cobertura', 'valor_corrigido': valor}
    
    fator = (1 + subset['valor']).prod()
    
    # Trava de Sanidade
    if fator > 100:
        return {'sucesso': False, 'mensagem': 'Erro: Taxa explosiva detectada', 'valor_corrigido': valor}
        
    valor_corrigido = valor * fator
    
    return {
        'sucesso': True,
        'valor_corrigido': valor_corrigido,
        'fator_correcao': fator,
        'variacao_percentual': (fator - 1) * 100,
        'indices': [indice],
        'detalhes': f"{len(subset)} meses"
    }

def calcular_correcao_media(valor: float, data_original: date, data_referencia: date, indices: list) -> dict:
    if not indices: return {'sucesso': False, 'mensagem': 'Selecione índices'}
    
    fatores = []
    indices_ok = []
    
    for ind in indices:
        res = calcular_correcao_individual(valor, data_original, data_referencia, ind)
        if res['sucesso']:
            fatores.append(res['fator_correcao'])
            indices_ok.append(ind)
        else:
            return {'sucesso': False, 'mensagem': f'Falha no índice {ind}', 'valor_corrigido': valor}
            
    if not fatores:
        return {'sucesso': False, 'mensagem': 'Erro geral', 'valor_corrigido': valor}
        
    import math
    fator_medio = math.prod(fatores) ** (1 / len(fatores))
    
    return {
        'sucesso': True,
        'valor_corrigido': valor * fator_medio,
        'fator_correcao': fator_medio,
        'variacao_percentual': (fator_medio - 1) * 100,
        'indices': indices_ok
    }
