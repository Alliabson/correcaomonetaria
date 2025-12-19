import requests
import pandas as pd
from datetime import datetime, date
import streamlit as st
import concurrent.futures
import time
from typing import Dict, List, Optional, Tuple
import json
import os
import sqlite3
import urllib3

# Desabilita avisos de certificado SSL (Necessário para APIs do governo)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configurações atualizadas com múltiplos endpoints
API_CONFIG = {
    "BCB_PRIMARIO": {
        "base_url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados",
        "timeout": 15,
        "prioridade": 1
    },
    "BCB_ALTERNATIVO": {
        "base_url": "https://dados.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados",
        "timeout": 15,
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
        ]
    },
    "IGPM": {
        "fontes": [
            {"api": "BCB_PRIMARIO", "codigo": "189"},
            {"api": "BCB_ALTERNATIVO", "codigo": "189"},
        ]
    },
    "INPC": {
        "fontes": [
            {"api": "BCB_PRIMARIO", "codigo": "188"},
            {"api": "BCB_ALTERNATIVO", "codigo": "188"},
        ]
    },
    "INCC": {
        "fontes": [
            {"api": "BCB_PRIMARIO", "codigo": "192"},
            {"api": "BCB_ALTERNATIVO", "codigo": "192"},
        ]
    },
    "SELIC": {
        "fontes": [
            {"api": "BCB_PRIMARIO", "codigo": "11"},
            {"api": "BCB_ALTERNATIVO", "codigo": "11"},
        ]
    }
}

# Sistema de cache com SQLite
class CacheManager:
    def __init__(self, db_path="indices_cache_v2.db"): # Mudei o nome para limpar o cache antigo sujo
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
                    timestamp REAL,
                    expiracao REAL
                )
            ''')
            conn.commit()
            conn.close()
        except:
            pass
    
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

# ==============================================================================
# AQUI ESTÁ A CORREÇÃO DO BLOQUEIO (Requisição Robusta)
# ==============================================================================
def fazer_requisicao_robusta(url: str, params: dict = None, timeout: int = 30, max_retries: int = 3):
    """
    Faz requisição fingindo ser um navegador real para evitar bloqueio 403 do BCB.
    """
    # Headers completos de um navegador Chrome no Windows
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1'
    }

    session = requests.Session()

    for tentativa in range(max_retries):
        try:
            # verify=False IGNORA o erro de SSL do governo
            response = session.get(
                url, 
                params=params, 
                headers=headers, 
                timeout=timeout, 
                verify=False 
            )
            
            if response.status_code == 200:
                try:
                    return response.json()
                except:
                    # Se não for JSON válido, tenta limpar caracteres estranhos
                    return json.loads(response.text)

            elif response.status_code in [403, 502, 503, 504]:
                time.sleep(1 + tentativa) # Espera progressiva
            else:
                break
                
        except Exception:
            time.sleep(1)
    
    return None

def buscar_dados_bcb(api_config: dict, codigo: str, data_inicio: date, data_final: date) -> pd.DataFrame:
    base_url = api_config["base_url"].format(codigo)
    
    # Tenta pegar tudo primeiro (mais rápido e estável no BCB)
    dados = fazer_requisicao_robusta(base_url, {"formato": "json"}, api_config["timeout"])
    
    if dados:
        try:
            df = pd.DataFrame(dados)
            df['data'] = pd.to_datetime(df['data'], dayfirst=True, errors='coerce').dt.date
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
            
            # ==================================================================
            # AQUI ESTÁ A CORREÇÃO DO CÁLCULO (R$ 42 MIL)
            # ==================================================================
            # O BCB envia 0.53 para significar 0.53%.
            # O código antigo só dividia se fosse > 10. 0.53 < 10, então não dividia.
            # Resultado: 53% de juros ao mês.
            # CORREÇÃO: Dividimos por 100 sempre que o valor parecer percentual.
            
            # Se a média for maior que 0.1 (10%), assume que está em escala 0-100 e divide.
            if df['valor'].abs().mean() > 0.1:
                df['valor'] = df['valor'] / 100
            
            # Filtro adicional: Se algum valor individual for > 2.0 (200%), divide também
            mask_erro = df['valor'].abs() > 2.0
            if mask_erro.any():
                df.loc[mask_erro, 'valor'] = df.loc[mask_erro, 'valor'] / 100
            
            df = df.dropna()
            mask = (df['data'] >= data_inicio) & (df['data'] <= data_final)
            return df[mask].copy()

        except Exception:
            pass
            
    return pd.DataFrame()

def buscar_dados_ibge_novo(api_config: dict, codigo: str, data_inicio: date, data_final: date) -> pd.DataFrame:
    try:
        meses_diff = (data_final.year - data_inicio.year) * 12 + data_final.month - data_inicio.month + 1
        periodo = f"{meses_diff}" if meses_diff > 1 else "1"
        url = api_config["base_url"].format(codigo, periodo)
        dados = fazer_requisicao_robusta(url, timeout=api_config["timeout"])
        
        if not dados: return pd.DataFrame()
        
        resultados = []
        for item in dados:
            for resultado in item.get('resultados', []):
                for serie in resultado.get('series', []):
                    for periodo_str, valores in serie.get('serie', {}).items():
                        if len(periodo_str) == 6:
                            data_ref = datetime.strptime(periodo_str, "%Y%m").date()
                            if data_inicio <= data_ref <= data_final:
                                # IBGE manda string, ex: "0.53". Dividimos por 100.
                                valor = float(valores.get('V', 0)) / 100 
                                resultados.append({'data': data_ref, 'valor': valor})
        
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
            if len(linha) >= 2 and len(linha[0]) == 6:
                data_ref = datetime.strptime(linha[0], "%Y%m").date()
                if data_inicio <= data_ref <= data_final:
                    # Sidra manda string "0.53". Dividimos por 100.
                    resultados.append({'data': data_ref, 'valor': float(linha[1]) / 100})
        
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
    
    for fonte in config_indice["fontes"]:
        api_config = API_CONFIG.get(fonte["api"])
        if not api_config: continue
        
        try:
            if fonte["api"].startswith("BCB"):
                df = buscar_dados_bcb(api_config, fonte["codigo"], data_inicio, data_final)
            elif fonte["api"] == "IBGE_NOVO":
                df = buscar_dados_ibge_novo(api_config, fonte["codigo"], data_inicio, data_final)
            elif fonte["api"] == "IBGE_SIDRA":
                df = buscar_dados_ibge_sidra(api_config, fonte["codigo"], data_inicio, data_final)
            else:
                continue
            
            if not df.empty:
                cache.set(chave_cache, df.to_dict('records'), duracao_horas=6)
                return df
        except:
            continue
    
    return pd.DataFrame()

# ===== INÍCIO DA CORREÇÃO =====
@st.cache_data(ttl=3600)
def get_indices_disponiveis() -> Dict[str, dict]:
    hoje = date.today()
    data_teste = date(hoje.year - 1, hoje.month, 1)
    
    # Feedback visual mais limpo
    progress = st.sidebar.progress(0)
    st.sidebar.caption("Sincronizando índices...")
    
    indices_disponiveis = {}
    total = len(CODIGOS_INDICES)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(buscar_dados_indice, indice, data_teste, hoje): indice 
            for indice in CODIGOS_INDICES.keys()
        }
        
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            indice = futures[future]
            try:
                df = future.result(timeout=20)
                if not df.empty:
                    # Verificação visual da última taxa
                    ultima_taxa = df.iloc[-1]['valor'] * 100
                    indices_disponiveis[indice] = {
                        'nome': f"{indice} ({ultima_taxa:.2f}%)",
                        'disponivel': True,
                        'ultima_data': df['data'].max().strftime("%m/%Y")
                    }
                else:
                    indices_disponiveis[indice] = {'nome': f"{indice} (Erro)", 'disponivel': False}
            except:
                indices_disponiveis[indice] = {'nome': f"{indice} (Erro)", 'disponivel': False}
            
            progress.progress((i + 1) / total)
    
    progress.empty()
    return indices_disponiveis

def calcular_correcao_individual(valor: float, data_original: date, data_referencia: date, indice: str) -> dict:
    if data_original >= data_referencia:
        return {'sucesso': True, 'valor_corrigido': valor, 'fator_correcao': 1.0, 'variacao_percentual': 0.0, 'indices': [indice]}
    
    dados = buscar_dados_indice(indice, data_original, data_referencia)
    
    if dados.empty:
        return {'sucesso': False, 'mensagem': f'Sem dados para {indice}', 'valor_corrigido': valor}
    
    # Filtro de data exato (Mês Inicial -> Mês Anterior ao Final)
    dt_inicio = date(data_original.year, data_original.month, 1)
    dt_fim = date(data_referencia.year, data_referencia.month, 1)
    mask = (dados['data'] >= dt_inicio) & (dados['data'] < dt_fim)
    subset = dados.loc[mask]
    
    if subset.empty:
        if dt_inicio == dt_fim:
             return {'sucesso': True, 'valor_corrigido': valor, 'fator_correcao': 1.0, 'variacao_percentual': 0.0}
        return {'sucesso': False, 'mensagem': f'Período inválido', 'valor_corrigido': valor}

    fator_correcao = (1 + subset['valor']).prod()
    
    # Trava de segurança anti-explosão
    if fator_correcao > 100:
        return {'sucesso': False, 'mensagem': f'Erro de cálculo (Fator {fator_correcao:.2f})', 'valor_corrigido': valor}

    valor_corrigido = valor * fator_correcao
    
    return {
        'sucesso': True,
        'valor_original': valor,
        'valor_corrigido': valor_corrigido,
        'fator_correcao': fator_correcao,
        'variacao_percentual': (fator_correcao - 1) * 100,
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
        if os.path.exists("indices_cache_v2.db"):
            os.remove("indices_cache_v2.db")
        cache._init_db()
        st.success("Cache limpo!")
    except:
        pass
