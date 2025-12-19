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
from pathlib import Path
import random
import numpy as np

# ====== CONFIGURAÇÕES OTIMIZADAS ======
API_CONFIG = {
    "IBGE_NOVO": {
        "base_url": "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos/-{}%20meses/variaveis/63?localidades=N1[all]",
        "timeout": 30,
        "prioridade": 1,
        "delay_min": 0.1,
        "delay_max": 0.5
    },
    "IBGE_SIDRA": {
        "base_url": "https://apisidra.ibge.gov.br/values/t/{}/n1/all/v/63/p/-{}/d/v63%202",
        "timeout": 30,
        "prioridade": 2,
        "delay_min": 0.1,
        "delay_max": 0.5
    },
    "BCB_RAPIDO": {
        "base_url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados/ultimos/{}",
        "timeout": 20,
        "prioridade": 3,
        "delay_min": 0.5,
        "delay_max": 1
    }
}

# ====== FONTES SIMPLIFICADAS E OTIMIZADAS ======
CODIGOS_INDICES = {
    "IPCA": {
        "fontes": [
            {"api": "IBGE_NOVO", "codigo": "1737", "nome": "IPCA - IBGE", "cache_horas": 24},
            {"api": "IBGE_SIDRA", "codigo": "1737", "nome": "IPCA - SIDRA", "cache_horas": 24},
            {"api": "BCB_RAPIDO", "codigo": "433", "nome": "IPCA - BCB", "cache_horas": 6}
        ],
        "fallback_rate": 0.005  # 0.5% ao mês como fallback
    },
    "IGPM": {
        "fontes": [
            {"api": "BCB_RAPIDO", "codigo": "189", "nome": "IGP-M - BCB", "cache_horas": 6}
        ],
        "fallback_rate": 0.006
    },
    "INPC": {
        "fontes": [
            {"api": "IBGE_NOVO", "codigo": "1736", "nome": "INPC - IBGE", "cache_horas": 24},
            {"api": "BCB_RAPIDO", "codigo": "188", "nome": "INPC - BCB", "cache_horas": 6}
        ],
        "fallback_rate": 0.004
    },
    "INCC": {
        "fontes": [
            {"api": "BCB_RAPIDO", "codigo": "192", "nome": "INCC - BCB", "cache_horas": 6}
        ],
        "fallback_rate": 0.0055
    },
    "SELIC": {
        "fontes": [
            {"api": "BCB_RAPIDO", "codigo": "11", "nome": "SELIC - BCB", "cache_horas": 6}
        ],
        "fallback_rate": 0.01
    }
}

# ====== CACHE MELHORADO ======
class CacheManager:
    def __init__(self, db_path="indices_cache.db"):
        self.db_path = db_path
        self._init_db()
        self.mem_cache = {}  # Cache em memória para acesso rápido
        self.mem_cache_ttl = 300  # 5 minutos em memória

    def _init_db(self):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS cache_indices (
                    chave TEXT PRIMARY KEY,
                    dados TEXT,
                    timestamp REAL,
                    expiracao REAL,
                    indice TEXT,
                    data_inicio TEXT,
                    data_fim TEXT
                )
            ''')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_indice ON cache_indices(indice)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_expiracao ON cache_indices(expiracao)')
            conn.commit()
            conn.close()
        except:
            pass

    def get(self, chave):
        # Primeiro verifica cache em memória
        if chave in self.mem_cache:
            cached_data, timestamp = self.mem_cache[chave]
            if time.time() - timestamp < self.mem_cache_ttl:
                return cached_data
        
        # Depois verifica SQLite
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT dados FROM cache_indices WHERE chave = ? AND expiracao > ?",
                (chave, time.time())
            )
            result = cursor.fetchone()
            conn.close()
            
            if result:
                data = json.loads(result[0])
                # Armazena em memória para acesso rápido
                self.mem_cache[chave] = (data, time.time())
                return data
        except:
            pass
        return None

    def set(self, chave, dados, duracao_horas=24, indice="", data_inicio="", data_fim=""):
        try:
            # Armazena em memória
            self.mem_cache[chave] = (dados, time.time())
            
            # Armazena em SQLite
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            timestamp = time.time()
            expiracao = timestamp + (duracao_horas * 3600)
            
            cursor.execute('''
                INSERT OR REPLACE INTO cache_indices 
                (chave, dados, timestamp, expiracao, indice, data_inicio, data_fim) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (chave, json.dumps(dados), timestamp, expiracao, indice, data_inicio, data_fim))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Erro no cache: {str(e)}")
            return False

# Inicializar cache
cache = CacheManager()

# ====== REQUISIÇÃO OTIMIZADA ======
def fazer_requisicao_rapida(url: str, params: dict = None, timeout: int = 20, source_name: str = ""):
    """Faz requisição otimizada com timeout curto"""
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip'
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=timeout)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            st.warning(f"Endpoint não encontrado: {source_name}")
        elif response.status_code == 403:
            st.warning(f"Acesso bloqueado: {source_name}")
        
        return None
    except requests.exceptions.Timeout:
        st.warning(f"Timeout: {source_name}")
        return None
    except Exception as e:
        st.warning(f"Erro {source_name}: {str(e)[:50]}")
        return None

# ====== BUSCA OTIMIZADA POR ÍNDICE ======
def buscar_dados_ibge_novo_rapido(codigo: str, meses: int) -> pd.DataFrame:
    """Busca rápida da API nova do IBGE"""
    try:
        url = f"https://servicodados.ibge.gov.br/api/v3/agregados/{codigo}/periodos/-{meses}%20meses/variaveis/63?localidades=N1[all]"
        dados = fazer_requisicao_rapida(url, source_name="IBGE")
        
        if not dados:
            return pd.DataFrame()
        
        resultados = []
        for item in dados:
            for serie in item.get('resultados', [{}])[0].get('series', []):
                for periodo, valor in serie.get('serie', {}).items():
                    if len(periodo) == 6:
                        data_ref = datetime.strptime(periodo, "%Y%m").date()
                        val = float(valor.get('V', 0)) / 100
                        resultados.append({'data': data_ref, 'valor': val})
        
        return pd.DataFrame(resultados).sort_values('data')
    except:
        return pd.DataFrame()

def buscar_dados_ibge_sidra_rapido(codigo: str, meses: int) -> pd.DataFrame:
    """Busca rápida do SIDRA"""
    try:
        url = f"https://apisidra.ibge.gov.br/values/t/{codigo}/n1/all/v/63/p/-{meses}/d/v63%202"
        dados = fazer_requisicao_rapida(url, source_name="SIDRA")
        
        if not dados or len(dados) < 2:
            return pd.DataFrame()
        
        resultados = []
        for linha in dados[1:]:
            if len(linha) >= 2:
                periodo = linha[0]
                valor_str = linha[1]
                if periodo and len(periodo) == 6:
                    data_ref = datetime.strptime(periodo, "%Y%m").date()
                    try:
                        valor = float(valor_str) / 100
                        resultados.append({'data': data_ref, 'valor': valor})
                    except:
                        continue
        
        return pd.DataFrame(resultados).sort_values('data')
    except:
        return pd.DataFrame()

def buscar_dados_bcb_rapido(codigo: str, meses: int) -> pd.DataFrame:
    """Busca rápida do BCB (apenas últimos N meses)"""
    try:
        # Busca apenas os últimos X meses para ser rápido
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados/ultimos/{meses * 2}"
        dados = fazer_requisicao_rapida(url, source_name="BCB")
        
        if not dados:
            return pd.DataFrame()
        
        resultados = []
        for item in dados:
            try:
                data_ref = datetime.strptime(item['data'], '%d/%m/%Y').date()
                valor = float(item['valor'].replace(',', '.'))
                if valor > 10:  # É porcentagem
                    valor = valor / 100
                resultados.append({'data': data_ref, 'valor': valor})
            except:
                continue
        
        return pd.DataFrame(resultados).sort_values('data')
    except:
        return pd.DataFrame()

# ====== BUSCA INTELIGENTE COM PARALELISMO ======
def buscar_dados_indice_rapido(indice: str, data_inicio: date, data_final: date) -> pd.DataFrame:
    """Busca otimizada com paralelismo e cache inteligente"""
    
    # Verificar cache primeiro (chave mais específica)
    meses_diff = (data_final.year - data_inicio.year) * 12 + data_final.month - data_inicio.month + 1
    chave_cache = f"{indice}_{data_inicio}_{data_final}_{meses_diff}"
    
    dados_cache = cache.get(chave_cache)
    if dados_cache:
        st.info(f"📦 Cache: {indice} ({len(dados_cache)} períodos)")
        return pd.DataFrame(dados_cache)
    
    config = CODIGOS_INDICES.get(indice)
    if not config:
        return pd.DataFrame()
    
    # Calcular quantos meses precisamos
    meses_necessarios = max(12, meses_diff + 3)  # Pega um pouco a mais para cache
    
    # Buscar de todas as fontes em paralelo
    resultados = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        
        for fonte in config["fontes"]:
            if fonte["api"] == "IBGE_NOVO":
                futures.append(executor.submit(buscar_dados_ibge_novo_rapido, fonte["codigo"], meses_necessarios))
            elif fonte["api"] == "IBGE_SIDRA":
                futures.append(executor.submit(buscar_dados_ibge_sidra_rapido, fonte["codigo"], meses_necessarios))
            elif fonte["api"] == "BCB_RAPIDO":
                futures.append(executor.submit(buscar_dados_bcb_rapido, fonte["codigo"], meses_necessarios))
        
        # Aguardar primeira resposta válida
        for future in concurrent.futures.as_completed(futures, timeout=15):
            try:
                df = future.result(timeout=10)
                if not df.empty and len(df) >= meses_diff:
                    # Filtrar pelo período exato
                    mask = (df['data'] >= data_inicio) & (df['data'] <= data_final)
                    df_filtrado = df[mask].copy()
                    
                    if not df_filtrado.empty:
                        resultados.append(df_filtrado)
                        break  # Primeira fonte válida é suficiente
            except:
                continue
    
    # Se encontrou dados, usar e cachear
    if resultados:
        df_final = resultados[0]
        
        # Cache por mais tempo se dados são históricos
        cache_horas = 24 if data_final.year < datetime.now().year else 6
        cache.set(
            chave_cache, 
            df_final.to_dict('records'),
            duracao_horas=cache_horas,
            indice=indice,
            data_inicio=data_inicio.isoformat(),
            data_fim=data_final.isoformat()
        )
        
        return df_final
    
    # Fallback: dados simulados (RÁPIDO)
    st.warning(f"⚠️ Usando dados estimados para {indice}")
    return gerar_dados_fallback_rapido(indice, data_inicio, data_final)

def gerar_dados_fallback_rapido(indice: str, data_inicio: date, data_final: date) -> pd.DataFrame:
    """Gera dados de fallback rapidamente"""
    taxas = {
        "IPCA": 0.0045, "IGPM": 0.0050, "INPC": 0.0040,
        "INCC": 0.0055, "SELIC": 0.0100
    }
    
    taxa_base = taxas.get(indice, 0.005)
    
    # Gerar série mensal
    datas = pd.date_range(start=data_inicio, end=data_final, freq='MS').date
    np.random.seed(hash(indice) % 10000)  # Seed consistente por índice
    
    # Variação mais realista
    variacoes = np.random.normal(taxa_base, taxa_base * 0.3, len(datas))
    variacoes = np.clip(variacoes, taxa_base * 0.5, taxa_base * 1.5)
    
    df = pd.DataFrame({
        'data': datas,
        'valor': variacoes
    })
    
    return df

# ====== CÁLCULOS OTIMIZADOS ======
@st.cache_data(ttl=600, show_spinner=False)  # 10 minutos cache
def get_indices_disponiveis_rapido() -> Dict[str, dict]:
    """Verificação rápida de disponibilidade"""
    
    hoje = date.today()
    data_teste = date(hoje.year - 1, hoje.month, 1)
    
    indices_status = {}
    
    # Verificar apenas 6 meses para ser rápido
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        
        for indice in CODIGOS_INDICES.keys():
            futures[executor.submit(
                buscar_dados_indice_rapido, 
                indice, 
                date(hoje.year, hoje.month - 6, 1) if hoje.month > 6 else date(hoje.year - 1, hoje.month + 6, 1),
                hoje
            )] = indice
        
        for future in concurrent.futures.as_completed(futures, timeout=10):
            indice = futures[future]
            try:
                df = future.result(timeout=5)
                indices_status[indice] = {
                    'disponivel': not df.empty,
                    'ultima_data': df['data'].max().strftime("%m/%Y") if not df.empty else "N/A",
                    'qtd_dados': len(df)
                }
            except:
                indices_status[indice] = {
                    'disponivel': False,
                    'ultima_data': "N/A",
                    'qtd_dados': 0
                }
    
    return indices_status

def calcular_correcao_rapida(valor: float, data_original: date, data_referencia: date, indice: str) -> dict:
    """Cálculo otimizado de correção"""
    
    if data_original >= data_referencia:
        return {
            'sucesso': False,
            'valor_corrigido': valor,
            'fator_correcao': 1.0,
            'variacao_percentual': 0.0,
            'indice': indice
        }
    
    try:
        # Buscar dados (já otimizado com cache)
        dados = buscar_dados_indice_rapido(indice, data_original, data_referencia)
        
        if dados.empty:
            return {
                'sucesso': False,
                'valor_corrigido': valor,
                'fator_correcao': 1.0,
                'variacao_percentual': 0.0,
                'indice': indice
            }
        
        # Cálculo vetorizado (RÁPIDO)
        fator_correcao = np.prod(1 + dados['valor'].values)
        valor_corrigido = valor * fator_correcao
        
        return {
            'sucesso': True,
            'valor_original': valor,
            'valor_corrigido': valor_corrigido,
            'fator_correcao': fator_correcao,
            'variacao_percentual': (fator_correcao - 1) * 100,
            'indice': indice,
            'periodos': len(dados),
            'fonte': 'Cache' if 'estimados' not in str(dados) else 'Estimado'
        }
        
    except Exception as e:
        return {
            'sucesso': False,
            'valor_corrigido': valor,
            'fator_correcao': 1.0,
            'variacao_percentual': 0.0,
            'indice': indice
        }

def calcular_correcao_multipla_rapida(valores_datas: List[Tuple[float, date]], data_referencia: date, indice: str) -> List[dict]:
    """Calcula correção para múltiplos valores de uma vez (OTIMIZADO)"""
    
    resultados = []
    
    # Agrupar por mês para otimizar buscas
    meses_unicos = set((d.year, d.month) for _, d in valores_datas)
    
    # Buscar dados para todos os meses necessários de uma vez
    if meses_unicos:
        datas_unicas = [date(y, m, 1) for y, m in meses_unicos]
        data_min = min(datas_unicas)
        data_max = max(datas_unicas)
        
        # Buscar dados para todo o período de uma vez
        dados_completos = buscar_dados_indice_rapido(indice, data_min, max(data_referencia, data_max))
        
        if not dados_completos.empty:
            # Criar dicionário rápido para acesso O(1)
            dados_dict = {d.date(): v for d, v in zip(pd.to_datetime(dados_completos['data']), dados_completos['valor'])}
            
            for valor, data_orig in valores_datas:
                if data_orig >= data_referencia:
                    resultados.append({
                        'sucesso': False,
                        'valor': valor,
                        'data': data_orig,
                        'corrigido': valor,
                        'fator': 1.0
                    })
                    continue
                
                # Calcular fator acumulado
                fator = 1.0
                data_atual = date(data_orig.year, data_orig.month, 1)
                
                while data_atual <= data_referencia:
                    if data_atual in dados_dict:
                        fator *= (1 + dados_dict[data_atual])
                    
                    # Próximo mês
                    if data_atual.month == 12:
                        data_atual = date(data_atual.year + 1, 1, 1)
                    else:
                        data_atual = date(data_atual.year, data_atual.month + 1, 1)
                
                resultados.append({
                    'sucesso': True,
                    'valor': valor,
                    'data': data_orig,
                    'corrigido': valor * fator,
                    'fator': fator
                })
    
    return resultados

# Funções de interface (mantidas)
def formatar_moeda(valor: float) -> str:
    if valor == 0:
        return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def limpar_cache():
    try:
        cache.mem_cache.clear()
        if os.path.exists("indices_cache.db"):
            os.remove("indices_cache.db")
        cache._init_db()
        st.success("✅ Cache limpo!")
    except:
        st.error("❌ Erro ao limpar cache")
