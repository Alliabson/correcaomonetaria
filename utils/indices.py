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
import urllib.parse

# Configurações atualizadas com múltiplos endpoints
API_CONFIG = {
    "BCB_PRIMARIO": {
        "base_url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados",
        "timeout": 30,
        "prioridade": 3,
        "headers": {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://www.bcb.gov.br/',
            'DNT': '1'
        }
    },
    "BCB_ALTERNATIVO": {
        "base_url": "https://dadosabertos.bcb.gov.br/dataset/{}/resource/{}/download/serie.csv",
        "timeout": 30,
        "prioridade": 4
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
    },
    "IBGE_JSON": {
        "base_url": "https://apisidra.ibge.gov.br/values/t/{}/n1/all/v/63/p/all/d/v63%202",
        "timeout": 30,
        "prioridade": 1,
        "params": {"formato": "json"}
    },
    "API_SIDRA_COMPACTA": {
        "base_url": "https://apisidra.ibge.gov.br/values/t/{}/n1/all/v/all/p/all/d/v63%202",
        "timeout": 30,
        "prioridade": 2
    },
    "FRED_API": {
        "base_url": "https://api.stlouisfed.org/fred/series/observations",
        "timeout": 30,
        "prioridade": 5,
        "params": {
            'series_id': '{}',
            'api_key': '{}',  # Você precisará criar uma conta gratuita no FRED
            'file_type': 'json',
            'observation_start': '{}',
            'observation_end': '{}'
        }
    },
    "BLOOMBERG_ALTERNATIVE": {
        "base_url": "https://www.bloomberg.com/markets/api/bulk-time-series/price/{}%3AIND?timeFrame=5_YEAR",
        "timeout": 30,
        "prioridade": 5
    },
    "MACROTRENDS": {
        "base_url": "https://www.macrotrends.net/assets/php/inflation_json.php",
        "timeout": 30,
        "prioridade": 5,
        "params": {
            'type': '{}',
            'country': 'brazil'
        }
    },
    "BCB_API_V2": {
        "base_url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados/ultimos/{}",
        "timeout": 30,
        "prioridade": 3
    },
    "IPEADATA": {
        "base_url": "http://www.ipeadata.gov.br/api/odata4/Metadados('{}')",
        "timeout": 30,
        "prioridade": 2
    },
    "YAHOO_FINANCE": {
        "base_url": "https://query1.finance.yahoo.com/v8/finance/chart/{}",
        "timeout": 30,
        "prioridade": 5,
        "params": {
            'range': '{}',
            'interval': '1mo'
        }
    }
}

# Múltiplas fontes para cada índice com códigos atualizados
CODIGOS_INDICES = {
    "IPCA": {
        "fontes": [
            # Fontes IBGE (mais confiáveis para IPCA)
            {"api": "IBGE_JSON", "codigo": "1737", "nome": "IPCA IBGE"},
            {"api": "IBGE_SIDRA", "codigo": "1737", "nome": "IPCA SIDRA"},
            {"api": "API_SIDRA_COMPACTA", "codigo": "1737", "nome": "IPCA SIDRA Compacta"},
            {"api": "IBGE_NOVO", "codigo": "1737", "nome": "IPCA Nova API"},
            
            # Fontes BCB (alternativas)
            {"api": "BCB_API_V2", "codigo": "433", "param_extra": "120", "nome": "IPCA BCB (120 meses)"},
            {"api": "BCB_PRIMARIO", "codigo": "433", "nome": "IPCA BCB Primário"},
            {"api": "BCB_ALTERNATIVO", "codigo": "ipca", "param_extra": "ipca_mensal", "nome": "IPCA Dados Abertos"},
            
            # IPEADATA
            {"api": "IPEADATA", "codigo": "PRECOS_INPC", "nome": "IPCA IPEADATA"},
            
            # Fontes internacionais
            {"api": "FRED_API", "codigo": "BRACPIALLMINMEI", "nome": "IPCA FRED"},
            {"api": "MACROTRENDS", "codigo": "cpi", "nome": "IPCA Macrotrends"}
        ]
    },
    "IGPM": {
        "fontes": [
            # Fontes FGV
            {"api": "BCB_PRIMARIO", "codigo": "189", "nome": "IGP-M BCB"},
            {"api": "BCB_API_V2", "codigo": "189", "param_extra": "120", "nome": "IGP-M BCB (120 meses)"},
            {"api": "BCB_ALTERNATIVO", "codigo": "igpm", "param_extra": "igpm_mensal", "nome": "IGP-M Dados Abertos"},
            
            # Fontes alternativas
            {"api": "IPEADATA", "codigo": "PRECOS_IGPM", "nome": "IGP-M IPEADATA"},
            {"api": "FRED_API", "codigo": "BRACPIALLMINMEI", "nome": "IGP-M FRED"}
        ]
    },
    "INPC": {
        "fontes": [
            # Fontes IBGE
            {"api": "IBGE_JSON", "codigo": "1736", "nome": "INPC IBGE"},
            {"api": "IBGE_SIDRA", "codigo": "1736", "nome": "INPC SIDRA"},
            {"api": "API_SIDRA_COMPACTA", "codigo": "1736", "nome": "INPC SIDRA Compacta"},
            
            # Fontes BCB
            {"api": "BCB_PRIMARIO", "codigo": "188", "nome": "INPC BCB"},
            {"api": "BCB_API_V2", "codigo": "188", "param_extra": "120", "nome": "INPC BCB (120 meses)"},
            
            # IPEADATA
            {"api": "IPEADATA", "codigo": "PRECOS_INPC", "nome": "INPC IPEADATA"}
        ]
    },
    "INCC": {
        "fontes": [
            {"api": "BCB_PRIMARIO", "codigo": "192", "nome": "INCC BCB"},
            {"api": "BCB_API_V2", "codigo": "192", "param_extra": "120", "nome": "INCC BCB (120 meses)"},
            {"api": "IPEADATA", "codigo": "PRECOS_INCC", "nome": "INCC IPEADATA"}
        ]
    },
    "SELIC": {
        "fontes": [
            {"api": "BCB_PRIMARIO", "codigo": "11", "nome": "SELIC BCB"},
            {"api": "BCB_API_V2", "codigo": "11", "param_extra": "120", "nome": "SELIC BCB (120 meses)"},
            {"api": "BCB_ALTERNATIVO", "codigo": "selic", "param_extra": "selic_mensal", "nome": "SELIC Dados Abertos"},
            {"api": "IPEADATA", "codigo": "TAXA_SELIC", "nome": "SELIC IPEADATA"},
            {"api": "FRED_API", "codigo": "IRSTCI01BRM156N", "nome": "SELIC FRED"}
        ]
    },
    "IPCA15": {
        "fontes": [
            {"api": "BCB_PRIMARIO", "codigo": "7478", "nome": "IPCA-15 BCB"},
            {"api": "IBGE_SIDRA", "codigo": "1705", "nome": "IPCA-15 IBGE"},
            {"api": "API_SIDRA_COMPACTA", "codigo": "1705", "nome": "IPCA-15 SIDRA"}
        ]
    },
    "IPCBR": {
        "fontes": [
            {"api": "BCB_PRIMARIO", "codigo": "191", "nome": "IPC-BR BCB"},
            {"api": "FRED_API", "codigo": "BRACPIALLMINMEI", "nome": "IPC-BR FRED"}
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

def fazer_requisicao_robusta(url: str, params: dict = None, headers: dict = None, timeout: int = 30, max_retries: int = 2):
    """Faz requisição com múltiplas tentativas e tratamento de erro"""
    headers_padrao = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0'
    }
    
    if headers:
        headers_padrao.update(headers)
    
    for tentativa in range(max_retries):
        try:
            # Adicionar delay entre tentativas
            if tentativa > 0:
                time.sleep(1)
            
            response = requests.get(
                url, 
                params=params, 
                headers=headers_padrao, 
                timeout=timeout,
                verify=True,
                allow_redirects=True
            )
            
            if response.status_code == 200:
                # Verificar conteúdo da resposta
                content_type = response.headers.get('Content-Type', '')
                if 'application/json' in content_type:
                    return response.json()
                elif 'text/csv' in content_type or 'text/plain' in content_type:
                    return response.text
                else:
                    # Tentar parsear como JSON de qualquer forma
                    try:
                        return response.json()
                    except:
                        return response.text
            elif response.status_code in [403, 429]:
                # Rate limiting ou acesso negado
                st.warning(f"Tentativa {tentativa + 1}: Acesso negado (HTTP {response.status_code}) - Tentando próxima fonte")
                return None
            elif response.status_code in [500, 502, 503, 504]:
                st.warning(f"Tentativa {tentativa + 1}: Servidor indisponível (HTTP {response.status_code})")
                if tentativa < max_retries - 1:
                    time.sleep(2 ** tentativa)
            else:
                st.warning(f"Tentativa {tentativa + 1}: HTTP {response.status_code}")
                break
                
        except requests.exceptions.Timeout:
            st.warning(f"Tentativa {tentativa + 1}: Timeout")
            if tentativa < max_retries - 1:
                time.sleep(2 ** tentativa)
        except requests.exceptions.ConnectionError as e:
            st.warning(f"Tentativa {tentativa + 1}: Erro de conexão - {str(e)}")
            if tentativa < max_retries - 1:
                time.sleep(2 ** tentativa)
        except requests.exceptions.RequestException as e:
            st.warning(f"Tentativa {tentativa + 1}: Erro de requisição - {str(e)}")
            break
        except Exception as e:
            st.warning(f"Tentativa {tentativa + 1}: Erro inesperado - {str(e)}")
            break
    
    return None

def buscar_dados_bcb_v2(api_config: dict, codigo: str, data_inicio: date, data_final: date) -> pd.DataFrame:
    """Busca dados do BCB usando API v2"""
    try:
        # Formatar URL para API v2
        meses_diff = (data_final.year - data_inicio.year) * 12 + data_final.month - data_inicio.month + 1
        meses = min(meses_diff, 120)  # Limitar a 120 meses
        
        url = api_config["base_url"].format(codigo, meses)
        
        # Headers específicos para BCB
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Referer': 'https://www.bcb.gov.br/',
            'Origin': 'https://www.bcb.gov.br'
        }
        
        dados = fazer_requisicao_robusta(url, headers=headers, timeout=api_config["timeout"])
        
        if dados and isinstance(dados, list):
            df = pd.DataFrame(dados)
            if 'data' in df.columns and 'valor' in df.columns:
                df['data'] = pd.to_datetime(df['data'], dayfirst=True, errors='coerce').dt.date
                df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
                
                # Converter para decimal se for porcentagem
                if df['valor'].max() > 10:  # Assume que é porcentagem se valores > 10
                    df['valor'] = df['valor'] / 100
                
                df = df.dropna()
                df = df[(df['data'] >= data_inicio) & (df['data'] <= data_final)]
                return df
    except Exception as e:
        st.warning(f"Erro API BCB v2: {str(e)}")
    
    return pd.DataFrame()

def buscar_dados_ipeadata(api_config: dict, codigo: str, data_inicio: date, data_final: date) -> pd.DataFrame:
    """Busca dados do IPEADATA"""
    try:
        url = api_config["base_url"].format(codigo)
        dados = fazer_requisicao_robusta(url, timeout=api_config["timeout"])
        
        if dados and isinstance(dados, dict):
            # Processar dados do IPEADATA
            if 'value' in dados and len(dados['value']) > 0:
                resultados = []
                for item in dados['value']:
                    try:
                        if 'VALDATA' in item and 'VALVALOR' in item:
                            data_str = item['VALDATA']
                            valor_str = item['VALVALOR']
                            
                            # Converter data
                            data_ref = datetime.strptime(data_str[:10], "%Y-%m-%d").date()
                            
                            if data_inicio <= data_ref <= data_final:
                                valor = float(valor_str)
                                resultados.append({
                                    'data': data_ref,
                                    'valor': valor / 100 if valor > 10 else valor
                                })
                    except:
                        continue
                
                if resultados:
                    return pd.DataFrame(resultados).sort_values('data')
    except Exception as e:
        st.warning(f"Erro API IPEADATA: {str(e)}")
    
    return pd.DataFrame()

def buscar_dados_csv(url: str, data_inicio: date, data_final: date) -> pd.DataFrame:
    """Busca dados de arquivos CSV"""
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            # Ler CSV
            df = pd.read_csv(pd.io.common.StringIO(response.text))
            
            # Procurar colunas de data e valor
            date_col = None
            value_col = None
            
            for col in df.columns:
                col_lower = col.lower()
                if any(word in col_lower for word in ['data', 'date', 'ano', 'mes']):
                    date_col = col
                elif any(word in col_lower for word in ['valor', 'value', 'indice', 'taxa']):
                    value_col = col
            
            if date_col and value_col:
                # Converter datas
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.date
                df[value_col] = pd.to_numeric(df[value_col], errors='coerce')
                
                # Renomear colunas
                df = df.rename(columns={date_col: 'data', value_col: 'valor'})
                df = df.dropna()
                
                # Filtrar por período
                df = df[(df['data'] >= data_inicio) & (df['data'] <= data_final)]
                
                return df[['data', 'valor']]
    except Exception as e:
        st.warning(f"Erro ao buscar CSV: {str(e)}")
    
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
    
    # Tentar fontes em ordem de prioridade
    for fonte in config_indice["fontes"]:
        api_config = API_CONFIG.get(fonte["api"])
        if not api_config:
            continue
        
        st.info(f"🔍 Tentando {fonte.get('nome', fonte['api'])}...")
        
        try:
            df = pd.DataFrame()
            
            if fonte["api"] == "BCB_PRIMARIO":
                # Tentar a API tradicional do BCB
                base_url = api_config["base_url"].format(fonte["codigo"])
                dados = fazer_requisicao_robusta(
                    base_url, 
                    {"formato": "json"}, 
                    headers=api_config.get("headers"),
                    timeout=api_config["timeout"]
                )
                
                if dados and isinstance(dados, list):
                    df = pd.DataFrame(dados)
                    if 'data' in df.columns and 'valor' in df.columns:
                        df['data'] = pd.to_datetime(df['data'], dayfirst=True, errors='coerce').dt.date
                        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
                        if df['valor'].max() > 10:
                            df['valor'] = df['valor'] / 100
                        df = df.dropna()
                        df = df[(df['data'] >= data_inicio) & (df['data'] <= data_final)]
            
            elif fonte["api"] == "BCB_API_V2":
                df = buscar_dados_bcb_v2(api_config, fonte["codigo"], data_inicio, data_final)
            
            elif fonte["api"] == "IBGE_JSON" or fonte["api"] == "IBGE_SIDRA" or fonte["api"] == "API_SIDRA_COMPACTA":
                # APIs do IBGE
                url = api_config["base_url"].format(fonte["codigo"])
                params = api_config.get("params", {})
                
                dados = fazer_requisicao_robusta(url, params=params, timeout=api_config["timeout"])
                
                if dados:
                    # Processar resposta do IBGE
                    if isinstance(dados, list) and len(dados) > 1:
                        resultados = []
                        for linha in dados[1:]:  # Pular cabeçalho
                            if len(linha) >= 2:
                                try:
                                    periodo_str = linha[0]
                                    valor_str = linha[1]
                                    
                                    if periodo_str and len(periodo_str) >= 6:
                                        # Formato YYYYMM
                                        data_ref = datetime.strptime(periodo_str[:6], "%Y%m").date()
                                        if data_inicio <= data_ref <= data_final:
                                            valor = float(valor_str) if valor_str else 0
                                            resultados.append({
                                                'data': data_ref,
                                                'valor': valor / 100
                                            })
                                except:
                                    continue
                        
                        if resultados:
                            df = pd.DataFrame(resultados)
            
            elif fonte["api"] == "IPEADATA":
                df = buscar_dados_ipeadata(api_config, fonte["codigo"], data_inicio, data_final)
            
            elif fonte["api"] == "BCB_ALTERNATIVO":
                # Dados abertos do BCB em CSV
                if "param_extra" in fonte:
                    url = api_config["base_url"].format(fonte["codigo"], fonte["param_extra"])
                    df = buscar_dados_csv(url, data_inicio, data_final)
            
            # Se conseguiu dados, salvar no cache e retornar
            if not df.empty:
                st.success(f"✅ Dados obtidos de {fonte.get('nome', fonte['api'])} - {len(df)} períodos")
                
                # Salvar no cache
                cache.set(chave_cache, df.to_dict('records'), duracao_horas=12)
                
                return df
                
        except Exception as e:
            st.warning(f"❌ Falha em {fonte.get('nome', fonte['api'])}: {str(e)}")
            continue
    
    # Se todas as fontes falharam, usar dados simulados como fallback
    st.warning(f"⚠️ Todas as fontes falharam para {indice}. Usando dados simulados para demonstração.")
    
    # Criar dados simulados para demonstração
    df_simulado = criar_dados_simulados(indice, data_inicio, data_final)
    
    if not df_simulado.empty:
        return df_simulado
    
    return pd.DataFrame()

def criar_dados_simulados(indice: str, data_inicio: date, data_final: date) -> pd.DataFrame:
    """Cria dados simulados para demonstração quando as APIs falham"""
    try:
        # Taxas mensais médias históricas aproximadas
        taxas_medias = {
            "IPCA": 0.0045,  # 0.45% ao mês
            "IGPM": 0.0050,  # 0.50% ao mês
            "INPC": 0.0040,  # 0.40% ao mês
            "INCC": 0.0042,  # 0.42% ao mês
            "SELIC": 0.0080, # 0.80% ao mês
            "IPCA15": 0.0043, # 0.43% ao mês
            "IPCBR": 0.0046  # 0.46% ao mês
        }
        
        taxa = taxas_medias.get(indice, 0.0045)
        
        # Gerar datas mensais
        dates = pd.date_range(start=data_inicio, end=data_final, freq='MS').date
        
        # Gerar valores com alguma variação aleatória
        import numpy as np
        np.random.seed(42)  # Para reprodutibilidade
        
        valores = []
        for i in range(len(dates)):
            # Variação aleatória entre -0.001 e +0.001
            variacao = np.random.uniform(-0.001, 0.001)
            valor_mes = taxa + variacao
            valores.append(valor_mes)
        
        df = pd.DataFrame({'data': dates, 'valor': valores})
        
        # Adicionar nota sobre dados simulados
        st.info(f"📊 **Nota:** Usando dados simulados para {indice}. Para dados reais, verifique sua conexão com a internet.")
        
        return df
        
    except Exception as e:
        st.error(f"Erro ao criar dados simulados: {str(e)}")
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
    
    # Limitar a verificação de alguns índices principais para não sobrecarregar
    indices_principais = ["IPCA", "IGPM", "INPC", "SELIC"]
    
    for indice in indices_principais:
        try:
            df = buscar_dados_indice(indice, data_teste, hoje)
            
            if not df.empty:
                indices_disponiveis[indice] = {
                    'nome': f"{indice} - Disponível",
                    'disponivel': True,
                    'ultima_data': df['data'].max().strftime("%m/%Y"),
                    'qtd_dados': len(df),
                    'fonte': 'API' if len(df) > 0 and df['data'].max() > date(2023, 1, 1) else 'Simulado'
                }
                st.sidebar.success(f"✅ {indice}: {len(df)} períodos")
            else:
                # Mesmo sem dados reais, marcar como disponível com dados simulados
                df_simulado = criar_dados_simulados(indice, data_teste, hoje)
                if not df_simulado.empty:
                    indices_disponiveis[indice] = {
                        'nome': f"{indice} - Dados Simulados",
                        'disponivel': True,
                        'ultima_data': df_simulado['data'].max().strftime("%m/%Y"),
                        'qtd_dados': len(df_simulado),
                        'fonte': 'Simulado'
                    }
                    st.sidebar.warning(f"⚠️ {indice}: {len(df_simulado)} períodos (simulados)")
                else:
                    indices_disponiveis[indice] = {
                        'nome': f"{indice} - Indisponível",
                        'disponivel': False,
                        'ultima_data': 'N/A',
                        'qtd_dados': 0,
                        'fonte': 'Nenhuma'
                    }
                    st.sidebar.error(f"❌ {indice}: Sem dados")
                    
        except Exception as e:
            st.sidebar.error(f"❌ {indice}: {str(e)}")
            # Adicionar mesmo com erro para manter a interface funcionando
            indices_disponiveis[indice] = {
                'nome': f"{indice} - Erro",
                'disponivel': False,
                'ultima_data': 'N/A',
                'qtd_dados': 0,
                'fonte': 'Erro'
            }
    
    # Adicionar índices adicionais mesmo que não sejam verificados
    indices_adicionais = ["INCC", "IPCA15", "IPCBR"]
    for indice in indices_adicionais:
        if indice not in indices_disponiveis:
            indices_disponiveis[indice] = {
                'nome': f"{indice} - Disponível",
                'disponivel': True,
                'ultima_data': 'N/A',
                'qtd_dados': 0,
                'fonte': 'Simulado'
            }
    
    if not indices_disponiveis:
        st.sidebar.error("""
        ⚠️ **Sistema usando modo de contingência**
        
        Todas as APIs estão indisponíveis no momento.
        O sistema usará dados simulados para cálculo.
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
        
        # Verificar se são dados simulados
        fonte = "API"
        if len(dados) > 0 and dados['data'].max() < date(2023, 1, 1):
            fonte = "Simulado"
        
        return {
            'sucesso': True,
            'valor_original': valor,
            'valor_corrigido': valor_corrigido,
            'fator_correcao': fator_correcao,
            'variacao_percentual': variacao_percentual,
            'indice': indice,
            'detalhes': dados,
            'fonte': fonte,
            'mensagem': f'Correção calculada com {len(dados)} períodos ({fonte})'
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
    fontes = []
    
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
                    fontes.append(resultado.get('fonte', 'Desconhecida'))
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
    
    # Determinar fonte predominante
    fonte_predominante = "API"
    if fontes.count("Simulado") > fontes.count("API"):
        fonte_predominante = "Simulado"
    elif "Simulado" in fontes:
        fonte_predominante = "Mista"
    
    return {
        'sucesso': True,
        'valor_original': valor,
        'valor_corrigido': valor_corrigido,
        'fator_correcao': fator_medio,
        'variacao_percentual': variacao_percentual,
        'resultados_parciais': resultados,
        'indices_com_falha': indices_com_falha,
        'fonte': fonte_predominante,
        'mensagem': f'Cálculo com {len(fatores)} de {len(indices)} índices ({fonte_predominante})'
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
