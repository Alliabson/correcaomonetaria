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
            {"api": "BCB_PRIMARIO", "codigo": "1619"}  # IPCA-15 como fallback
        ]
    },
    "IGPM": {
        "fontes": [
            {"api": "BCB_PRIMARIO", "codigo": "189"},
            {"api": "BCB_ALTERNATIVO", "codigo": "189"},
            {"api": "BCB_PRIMARIO", "codigo": "190"}  # IGP-DI como fallback
        ]
    },
    "INPC": {
        "fontes": [
            {"api": "BCB_PRIMARIO", "codigo": "188"},
            {"api": "BCB_ALTERNATIVO", "codigo": "188"},
            {"api": "IBGE_NOVO", "codigo": "1736"},
            {"api": "IBGE_SIDRA", "codigo": "1736"},
            {"api": "BCB_PRIMARIO", "codigo": "11426"}  # INPC série nova
        ]
    },
    "INCC": {
        "fontes": [
            {"api": "BCB_PRIMARIO", "codigo": "192"},
            {"api": "BCB_ALTERNATIVO", "codigo": "192"},
            {"api": "BCB_PRIMARIO", "codigo": "7458"}  # INCC-DI
        ]
    },
    "SELIC": {
        "fontes": [
            {"api": "BCB_PRIMARIO", "codigo": "11"},
            {"api": "BCB_ALTERNATIVO", "codigo": "11"},
            {"api": "BCB_PRIMARIO", "codigo": "1178"}  # Meta Selic
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
    """Faz requisição com múltiplas tentativas e tratamento de erro"""
    for tentativa in range(max_retries):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
            
            response = requests.get(
                url, 
                params=params, 
                headers=headers, 
                timeout=timeout,
                verify=True  # Importante para SSL
            )
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code in [502, 503, 504]:
                st.warning(f"Tentativa {tentativa + 1}: Servidor indisponível, aguardando...")
                time.sleep(2 ** tentativa)  # Backoff exponencial
            else:
                st.warning(f"Tentativa {tentativa + 1}: HTTP {response.status_code}")
                break
                
        except requests.exceptions.Timeout:
            st.warning(f"Tentativa {tentativa + 1}: Timeout")
            if tentativa < max_retries - 1:
                time.sleep(2 ** tentativa)
        except requests.exceptions.ConnectionError:
            st.warning(f"Tentativa {tentativa + 1}: Erro de conexão")
            if tentativa < max_retries - 1:
                time.sleep(2 ** tentativa)
        except Exception as e:
            st.warning(f"Tentativa {tentativa + 1}: {str(e)}")
            break
    
    return None

def buscar_dados_bcb(api_config: dict, codigo: str, data_inicio: date, data_final: date) -> pd.DataFrame:
    """Busca dados do BCB com estratégias alternativas"""
    base_url = api_config["base_url"].format(codigo)
    
    # Estratégia 1: Sem filtro de data (mais estável)
    dados = fazer_requisicao_robusta(base_url, {"formato": "json"}, api_config["timeout"])
    
    if dados:
        try:
            df = pd.DataFrame(dados)
            df['data'] = pd.to_datetime(df['data'], dayfirst=True, errors='coerce').dt.date
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
            
            # Converter para decimal se for porcentagem
            if df['valor'].max() > 10:  # Assume que é porcentagem se valores > 10
                df['valor'] = df['valor'] / 100
            
            df = df.dropna()
            
            # Filtrar por período
            mask = (df['data'] >= data_inicio) & (df['data'] <= data_final)
            df_filtrado = df[mask].copy()
            
            if not df_filtrado.empty:
                return df_filtrado
        except Exception as e:
            st.warning(f"Erro ao processar dados BCB: {str(e)}")
    
    # Estratégia 2: Com filtro de data
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
            
            if df['valor'].max() > 10:
                df['valor'] = df['valor'] / 100
            
            return df.dropna()
        except Exception as e:
            st.warning(f"Erro ao processar dados BCB filtrados: {str(e)}")
    
    return pd.DataFrame()

def buscar_dados_ibge_novo(api_config: dict, codigo: str, data_inicio: date, data_final: date) -> pd.DataFrame:
    """Busca dados da nova API do IBGE"""
    try:
        # Calcular número de meses
        meses_diff = (data_final.year - data_inicio.year) * 12 + data_final.month - data_inicio.month + 1
        periodo = f"{meses_diff}" if meses_diff > 1 else "1"
        
        url = api_config["base_url"].format(codigo, periodo)
        dados = fazer_requisicao_robusta(url, timeout=api_config["timeout"])
        
        if not dados or len(dados) == 0:
            return pd.DataFrame()
        
        # Processar estrutura complexa do IBGE
        resultados = []
        for item in dados:
            for resultado in item.get('resultados', []):
                for serie in resultado.get('series', []):
                    for periodo_str, valores in serie.get('serie', {}).items():
                        try:
                            if len(periodo_str) == 6:  # Formato YYYYMM
                                data_ref = datetime.strptime(periodo_str, "%Y%m").date()
                                if data_inicio <= data_ref <= data_final:
                                    valor = float(valores.get('V', 0))
                                    resultados.append({
                                        'data': data_ref,
                                        'valor': valor / 100  # Converter para decimal
                                    })
                        except:
                            continue
        
        if resultados:
            return pd.DataFrame(resultados).sort_values('data')
            
    except Exception as e:
        st.warning(f"Erro API IBGE nova: {str(e)}")
    
    return pd.DataFrame()

def buscar_dados_ibge_sidra(api_config: dict, codigo: str, data_inicio: date, data_final: date) -> pd.DataFrame:
    """Busca dados do SIDRA tradicional"""
    try:
        url = api_config["base_url"].format(codigo)
        dados = fazer_requisicao_robusta(url, timeout=api_config["timeout"])
        
        if not dados or len(dados) < 2:
            return pd.DataFrame()
        
        # Processar dados do SIDRA
        resultados = []
        for linha in dados[1:]:
            try:
                if len(linha) >= 2:
                    periodo_str = linha[0]  # Primeira coluna é o período
                    valor_str = linha[1]    # Segunda coluna é o valor
                    
                    if periodo_str and valor_str and len(periodo_str) == 6:
                        data_ref = datetime.strptime(periodo_str, "%Y%m").date()
                        if data_inicio <= data_ref <= data_final:
                            valor = float(valor_str)
                            resultados.append({
                                'data': data_ref,
                                'valor': valor / 100
                            })
            except:
                continue
        
        if resultados:
            return pd.DataFrame(resultados).sort_values('data')
            
    except Exception as e:
        st.warning(f"Erro API IBGE SIDRA: {str(e)}")
    
    return pd.DataFrame()

def buscar_dados_indice(indice: str, data_inicio: date, data_final: date) -> pd.DataFrame:
    """Busca dados de um índice usando múltiplas fontes em ordem de prioridade"""
    # Verificar cache primeiro
    chave_cache = f"{indice}_{data_inicio}_{data_final}"
    dados_cache = cache.get(chave_cache)
    
    if dados_cache:
        st.info(f"📦 Usando dados em cache para {indice}")
        return pd.DataFrame(dados_cache)
    
    config_indice = CODIGOS_INDICES.get(indice)
    if not config_indice:
        return pd.DataFrame()
    
    # Ordenar fontes por prioridade
    fontes_ordenadas = []
    for fonte in config_indice["fontes"]:
        api_config = API_CONFIG.get(fonte["api"])
        if api_config:
            fontes_ordenadas.append((fonte, api_config))
    
    # Ordenar por prioridade da API
    fontes_ordenadas.sort(key=lambda x: x[1]["prioridade"])
    
    for fonte, api_config in fontes_ordenadas:
        st.info(f"🔍 Tentando {fonte['api']} para {indice}...")
        
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
                st.success(f"✅ Dados obtidos de {fonte['api']} para {indice}")
                
                # Salvar no cache
                cache.set(chave_cache, df.to_dict('records'), duracao_horas=6)
                
                return df
                
        except Exception as e:
            st.warning(f"❌ Falha em {fonte['api']}: {str(e)}")
            continue
    
    st.error(f"❌ Todas as fontes falharam para {indice}")
    return pd.DataFrame()

# ===== INÍCIO DA CORREÇÃO =====
@st.cache_data(ttl=3600) # Cacheia o resultado por 1 hora (3600 segundos)
# ===== FIM DA CORREÇÃO =====
def get_indices_disponiveis() -> Dict[str, dict]:
    """Verifica disponibilidade dos índices de forma inteligente"""
    hoje = date.today()
    data_teste = date(hoje.year - 1, hoje.month, 1)
    
    st.sidebar.info("🔍 Verificando disponibilidade dos índices...")
    
    indices_disponiveis = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(buscar_dados_indice, indice, data_teste, hoje): indice 
            for indice in CODIGOS_INDICES.keys()
        }
        
        for future in concurrent.futures.as_completed(futures):
            indice = futures[future]
            try:
                df = future.result(timeout=30)
                if not df.empty:
                    indices_disponiveis[indice] = {
                        'nome': f"{indice} - Disponível",
                        'disponivel': True,
                        'ultima_data': df['data'].max().strftime("%m/%Y"),
                        'qtd_dados': len(df)
                    }
                    st.sidebar.success(f"✅ {indice}: {len(df)} períodos")
                else:
                    st.sidebar.error(f"❌ {indice}: Sem dados")
            except concurrent.futures.TimeoutError:
                st.sidebar.warning(f"⏰ {indice}: Timeout")
            except Exception as e:
                st.sidebar.error(f"❌ {indice}: {str(e)}")
    
    if not indices_disponiveis:
        st.sidebar.error("""
        ⚠️ **Sistema usando modo de contingência**
        
        Todas as APIs estão indisponíveis no momento.
        Recomendações:
        - Verifique sua conexão com a internet
        - Tente novamente em alguns minutos
        - Use dados históricos locais se disponível
        """)
    
    return indices_disponiveis

def calcular_correcao_individual(valor: float, data_original: date, data_referencia: date, indice: str) -> dict:
    """Calcula correção monetária individual com tratamento robusto"""
    if data_original > data_referencia:
        return {
            'sucesso': False,
            'mensagem': 'Data de referência deve ser posterior à data original',
            'valor_corrigido': valor,
            'fator_correcao': 1.0,
            'variacao_percentual': 0.0,
            'indice': indice
        }
    
    try:
        dados = buscar_dados_indice(indice, data_original, data_referencia)
        
        if dados.empty:
            return {
                'sucesso': False,
                'mensagem': f'Não foram encontrados dados para {indice} no período',
                'valor_corrigido': valor,
                'fator_correcao': 1.0,
                'variacao_percentual': 0.0,
                'indice': indice
            }
        
        # Calcular fator de correção acumulado
        fator_correcao = (1 + dados['valor']).prod()
        variacao_percentual = (fator_correcao - 1) * 100
        valor_corrigido = valor * fator_correcao
        
        return {
            'sucesso': True,
            'valor_original': valor,
            'valor_corrigido': valor_corrigido,
            'fator_correcao': fator_correcao,
            'variacao_percentual': variacao_percentual,
            'indice': indice,
            'detalhes': dados,
            'mensagem': f'Correção calculada com {len(dados)} períodos'
        }
        
    except Exception as e:
        return {
            'sucesso': False,
            'mensagem': f'Erro ao calcular correção: {str(e)}',
            'valor_corrigido': valor,
            'fator_correcao': 1.0,
            'variacao_percentual': 0.0,
            'indice': indice
        }

def calcular_correcao_media(valor: float, data_original: date, data_referencia: date, indices: List[str]) -> dict:
    """Calcula correção pela média de múltiplos índices"""
    if data_original > data_referencia:
        return {
            'sucesso': False,
            'mensagem': 'Data de referência deve ser posterior à data original',
            'valor_corrigido': valor,
            'fator_correcao': 1.0,
            'variacao_percentual': 0.0
        }
    
    resultados = []
    fatores = []
    indices_com_falha = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(calcular_correcao_individual, valor, data_original, data_referencia, indice): indice 
            for indice in indices
        }
        
        for future in concurrent.futures.as_completed(futures):
            indice = futures[future]
            try:
                resultado = future.result(timeout=30)
                if resultado['sucesso']:
                    fatores.append(resultado['fator_correcao'])
                    resultados.append(resultado)
                else:
                    indices_com_falha.append(indice)
                    st.warning(f"Falha no índice {indice}: {resultado['mensagem']}")
            except Exception as e:
                indices_com_falha.append(indice)
                st.warning(f"Erro no índice {indice}: {str(e)}")
    
    if not fatores:
        return {
            'sucesso': False,
            'mensagem': 'Nenhum índice retornou dados válidos',
            'valor_corrigido': valor,
            'fator_correcao': 1.0,
            'variacao_percentual': 0.0,
            'indices_com_falha': indices_com_falha
        }
    
    # Calcular média geométrica
    fator_medio = 1.0
    for fator in fatores:
        fator_medio *= fator
    fator_medio = fator_medio ** (1/len(fatores))
    
    variacao_percentual = (fator_medio - 1) * 100
    valor_corrigido = valor * fator_medio
    
    return {
        'sucesso': True,
        'valor_original': valor,
        'valor_corrigido': valor_corrigido,
        'fator_correcao': fator_medio,
        'variacao_percentual': variacao_percentual,
        'resultados_parciais': resultados,
        'indices_com_falha': indices_com_falha,
        'mensagem': f'Cálculo com {len(fatores)} de {len(indices)} índices'
    }

def formatar_moeda(valor: float) -> str:
    """Formata valor como moeda brasileira"""
    if valor == 0:
        return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def limpar_cache():
    """Limpa todo o cache"""
    try:
        if os.path.exists("indices_cache.db"):
            os.remove("indices_cache.db")
        cache._init_db()
        st.success("✅ Cache limpo com sucesso!")
    except Exception as e:
        st.error(f"❌ Erro ao limpar cache: {str(e)}")
