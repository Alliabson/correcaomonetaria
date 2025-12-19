import requests
import pandas as pd
from datetime import datetime, date, timedelta
import streamlit as st
import concurrent.futures
import time
from typing import Dict, List, Optional, Tuple
import json
import os
import sqlite3
import urllib3

# === CRÍTICO: Desabilita avisos de SSL para o BCB ===
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configurações atualizadas com múltiplos endpoints
API_CONFIG = {
    "BCB_PRIMARIO": {
        "base_url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados",
        "timeout": 30,
        "prioridade": 1
    },
    "BCB_ALTERNATIVO": {
        "base_url": "https://dados.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados",
        "timeout": 30,
        "prioridade": 2
    },
    "IBGE_NOVO": {
        "base_url": "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos/-{}%20meses/variaveis/63?localidades=N1[all]",
        "timeout": 30,
        "prioridade": 1
    },
    "IBGE_SIDRA": {
        "base_url": "https://apisidra.ibge.gov.br/values/t/{}/n1/all/v/63/p/all/d/v63%202",
        "timeout": 30,
        "prioridade": 2
    }
}

# Múltiplas fontes para cada índice
CODIGOS_INDICES = {
    "IPCA": {
        "fontes": [
            {"api": "BCB_PRIMARIO", "codigo": "433"},
            {"api": "BCB_ALTERNATIVO", "codigo": "433"},
            {"api": "IBGE_NOVO", "codigo": "1737"},
            {"api": "IBGE_SIDRA", "codigo": "1737"},
            {"api": "BCB_PRIMARIO", "codigo": "1619"} 
        ]
    },
    "IGPM": {
        "fontes": [
            {"api": "BCB_PRIMARIO", "codigo": "189"},
            {"api": "BCB_ALTERNATIVO", "codigo": "189"},
            {"api": "BCB_PRIMARIO", "codigo": "190"}
        ]
    },
    "INPC": {
        "fontes": [
            {"api": "BCB_PRIMARIO", "codigo": "188"},
            {"api": "BCB_ALTERNATIVO", "codigo": "188"},
            {"api": "IBGE_NOVO", "codigo": "1736"},
            {"api": "IBGE_SIDRA", "codigo": "1736"},
            {"api": "BCB_PRIMARIO", "codigo": "11426"}
        ]
    },
    "INCC": {
        "fontes": [
            {"api": "BCB_PRIMARIO", "codigo": "192"},
            {"api": "BCB_ALTERNATIVO", "codigo": "192"},
            {"api": "BCB_PRIMARIO", "codigo": "7458"}
        ]
    }
}

# Sistema de cache com SQLite
class CacheManager:
    def __init__(self, db_path="indices_cache.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cache_indices (
                chave TEXT PRIMARY KEY,
                dados TEXT,
                timestamp REAL,
                expiracao REAL
            )
        ''')
        conn.commit()
        conn.close()

    def get(self, chave):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT dados, timestamp, expiracao FROM cache_indices WHERE chave = ?",
                (chave,)
            )
            result = cursor.fetchone()
            conn.close()
            
            if result and time.time() < result[2]:
                return json.loads(result[0])
            return None
        except:
            return None

    def set(self, chave, dados, duracao_horas=24):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            timestamp = time.time()
            expiracao = timestamp + (duracao_horas * 3600)
            
            cursor.execute(
                '''INSERT OR REPLACE INTO cache_indices 
                   (chave, dados, timestamp, expiracao) VALUES (?, ?, ?, ?)''',
                (chave, json.dumps(dados), timestamp, expiracao)
            )
            conn.commit()
            conn.close()
            return True
        except:
            return False

# Inicializar cache
cache = CacheManager()

def fazer_requisicao_robusta(url: str, params: dict = None, timeout: int = 30, max_retries: int = 3):
    """Faz requisição com múltiplas tentativas e SSL ignorado se necessário"""
    for tentativa in range(max_retries):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json',
                'Connection': 'keep-alive'
            }
            response = requests.get(
                url, 
                params=params, 
                headers=headers, 
                timeout=timeout,
                verify=False  # Alterado para False para evitar erros de SSL do governo
            )
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code in [502, 503, 504]:
                time.sleep(2 ** tentativa)
            else:
                break
                
        except Exception:
            if tentativa < max_retries - 1:
                time.sleep(2 ** tentativa)

    return None

def buscar_dados_bcb(api_config: dict, codigo: str, data_inicio: date, data_final: date) -> pd.DataFrame:
    base_url = api_config["base_url"].format(codigo)
    # Tenta sem filtro primeiro (mais robusto no BCB)
    dados = fazer_requisicao_robusta(base_url, {"formato": "json"}, api_config["timeout"])

    if not dados:
        # Tenta com filtro
        params = {
            "formato": "json",
            "dataInicial": data_inicio.strftime("%d/%m/%Y"),
            "dataFinal": data_final.strftime("%d/%m/%Y")
        }
        dados = fazer_requisicao_robusta(base_url, params, api_config["timeout"])

    if dados:
        try:
            df = pd.DataFrame(dados)
            df['data'] = pd.to_datetime(df['data'], dayfirst=True, errors='coerce').dt.date
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
            
            if df['valor'].max() > 10: # Correção percentual
                df['valor'] = df['valor'] / 100
            
            df = df.dropna()
            mask = (df['data'] >= data_inicio) & (df['data'] <= data_final)
            return df[mask].copy()
        except Exception:
            pass

    return pd.DataFrame()

def buscar_dados_ibge_novo(api_config: dict, codigo: str, data_inicio: date, data_final: date) -> pd.DataFrame:
    try:
        # Pega sempre um periodo maior para garantir
        url = api_config["base_url"].format(codigo, "120") # Últimos 10 anos
        dados = fazer_requisicao_robusta(url, timeout=api_config["timeout"])
        
        if not dados: return pd.DataFrame()
        
        resultados = []
        for item in dados:
            for resultado in item.get('resultados', []):
                for serie in resultado.get('series', []):
                    for periodo_str, valores in serie.get('serie', {}).items():
                        try:
                            if len(periodo_str) == 6:
                                data_ref = datetime.strptime(periodo_str, "%Y%m").date()
                                if data_inicio <= data_ref <= data_final:
                                    valor = float(valores.get('V', 0))
                                    resultados.append({'data': data_ref, 'valor': valor / 100})
                        except:
                            continue
        if resultados:
            return pd.DataFrame(resultados).sort_values('data')
    except:
        pass
    return pd.DataFrame()

def buscar_dados_ibge_sidra(api_config: dict, codigo: str, data_inicio: date, data_final: date) -> pd.DataFrame:
    try:
        url = api_config["base_url"].format(codigo)
        dados = fazer_requisicao_robusta(url, timeout=api_config["timeout"])
        if not dados or len(dados) < 2: return pd.DataFrame()
        
        resultados = []
        for linha in dados[1:]:
            try:
                if len(linha) >= 2 and len(linha[0]) == 6:
                    data_ref = datetime.strptime(linha[0], "%Y%m").date()
                    if data_inicio <= data_ref <= data_final:
                        resultados.append({'data': data_ref, 'valor': float(linha[1]) / 100})
            except:
                continue
        if resultados:
            return pd.DataFrame(resultados).sort_values('data')
    except:
        pass
    return pd.DataFrame()

def buscar_dados_indice(indice: str, data_inicio: date, data_final: date) -> pd.DataFrame:
    chave_cache = f"{indice}_{data_inicio}_{data_final}"
    dados_cache = cache.get(chave_cache)
    if dados_cache:
        return pd.DataFrame(dados_cache)

    config_indice = CODIGOS_INDICES.get(indice)
    if not config_indice: return pd.DataFrame()

    fontes_ordenadas = []
    for fonte in config_indice["fontes"]:
        api_config = API_CONFIG.get(fonte["api"])
        if api_config: fontes_ordenadas.append((fonte, api_config))
    fontes_ordenadas.sort(key=lambda x: x[1]["prioridade"])

    for fonte, api_config in fontes_ordenadas:
        try:
            if "BCB" in fonte["api"]:
                df = buscar_dados_bcb(api_config, fonte["codigo"], data_inicio, data_final)
            elif "IBGE_NOVO" == fonte["api"]:
                df = buscar_dados_ibge_novo(api_config, fonte["codigo"], data_inicio, data_final)
            elif "IBGE_SIDRA" == fonte["api"]:
                df = buscar_dados_ibge_sidra(api_config, fonte["codigo"], data_inicio, data_final)
            else:
                continue

            if not df.empty:
                # Normaliza datas para dia 1 para evitar problemas
                df['data'] = df['data'].apply(lambda x: date(x.year, x.month, 1))
                df = df.drop_duplicates('data').sort_values('data')
                
                cache.set(chave_cache, df.to_dict('records'), duracao_horas=6)
                return df
        except:
            continue

    return pd.DataFrame()

def get_indices_disponiveis() -> Dict[str, dict]:
    hoje = date.today()
    data_teste = date(hoje.year - 1, hoje.month, 1)
    indices_disponiveis = {}
    
    # Teste rápido apenas com IPCA para não travar load
    teste = buscar_dados_indice("IPCA", data_teste, hoje)
    status_geral = not teste.empty

    for indice in CODIGOS_INDICES.keys():
        # Se IPCA funcionou, assume que os outros funcionam para não fazer 50 requests no boot
        indices_disponiveis[indice] = {
            'nome': indice,
            'disponivel': status_geral,
            'ultima_data': hoje.strftime("%m/%Y")
        }
    return indices_disponiveis

def calcular_correcao_individual(valor: float, data_original: date, data_referencia: date, indice: str) -> dict:
    if data_original >= data_referencia:
        return {'sucesso': True, 'valor_corrigido': valor, 'fator_correcao': 1.0, 'variacao_percentual': 0.0}
    
    dados = buscar_dados_indice(indice, data_original, data_referencia)
    if dados.empty:
        return {'sucesso': False, 'mensagem': f'Sem dados para {indice}', 'valor_corrigido': valor}
    
    fator = (1 + dados['valor']).prod()
    return {
        'sucesso': True,
        'valor_corrigido': valor * fator,
        'fator_correcao': fator,
        'variacao_percentual': (fator - 1) * 100,
        'indices': [indice]
    }

def calcular_correcao_media(valor: float, data_original: date, data_referencia: date, indices: List[str]) -> dict:
    fatores = []
    for idx in indices:
        res = calcular_correcao_individual(valor, data_original, data_referencia, idx)
        if res['sucesso']: fatores.append(res['fator_correcao'])
    
    if not fatores:
        return {'sucesso': False, 'mensagem': 'Nenhum índice disponível', 'valor_corrigido': valor}
    
    media = sum(fatores) / len(fatores)
    return {
        'sucesso': True,
        'valor_corrigido': valor * media,
        'fator_correcao': media,
        'variacao_percentual': (media - 1) * 100
    }

def formatar_moeda(valor: float) -> str:
    if not valor: return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def limpar_cache():
    try:
        if os.path.exists("indices_cache.db"):
            os.remove("indices_cache.db")
        cache._init_db()
        st.success("Cache limpo!")
    except:
        pass
