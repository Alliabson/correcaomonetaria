import requests
import pandas as pd
from datetime import datetime, date, timedelta
import streamlit as st
import concurrent.futures
import time
from typing import Dict, List, Optional, Tuple
import json
import socket
from dateutil.relativedelta import relativedelta # Importação adicionada

# Configurações atualizadas das APIs
API_CONFIG = {
    "BCB": {
        "base_url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados",
        "timeout": 30
    },
    "IBGE": {
        "base_url": "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos/-{}/variaveis/63?localidades=N1[all]",
        "timeout": 30,
        "series": {
            "IPCA": "1737",
            "INPC": "1736"
        }
    }
}

# Códigos atualizados das séries (verificados em agosto/2023)
CODIGOS_INDICES = {
    "IPCA": {
        "codigo": 433,    # IPCA
        "fallback": 1619, # IPCA-15
        "api": ["BCB", "IBGE"]
    },
    "IGPM": {
        "codigo": 189,    # IGP-M
        "fallback": None,
        "api": ["BCB"]
    },
    "INPC": {
        "codigo": 188,    # INPC
        "fallback": 11426, # INPC (série nova)
        "api": ["BCB", "IBGE"]
    },
    "INCC": {
        "codigo": 192,    # INCC
        "fallback": 7458,  # INCC-DI
        "api": ["BCB"]
    }
}

# Lista de índices que usam o valor do mês anterior
INDICES_COM_LAG = ["INCC", "IPCA", "INPC"]

@st.cache_data(ttl=3600, show_spinner="Buscando dados econômicos...")
def fetch_api_data(api_name: str, series_code: str, start_date: date, end_date: date) -> pd.DataFrame:
    """Busca dados de qualquer API com tratamento robusto de falhas"""
    config = API_CONFIG.get(api_name)
    if not config:
        return pd.DataFrame()

    try:
        if api_name == "BCB":
            # Tentativa 1: Sem filtro de data (mais estável)
            try:
                url = config["base_url"].format(series_code)
                response = requests.get(url, params={"formato": "json"}, timeout=config["timeout"])
                response.raise_for_status()
                data = response.json()
                
                if data:
                    df = pd.DataFrame(data)
                    df['data'] = pd.to_datetime(df['data'], dayfirst=True).dt.date
                    df['valor'] = pd.to_numeric(df['valor'], errors='coerce') / 100
                    df = df.dropna()
                    
                    # Filtra localmente
                    mask = (df['data'] >= start_date) & (df['data'] <= end_date)
                    filtered_df = df[mask].copy()
                    
                    if not filtered_df.empty:
                        return filtered_df
            except:
                pass
            
            # Tentativa 2: Com filtro de data
            try:
                url = config["base_url"].format(series_code)
                params = {
                    "formato": "json",
                    "dataInicial": start_date.strftime("%d/%m/%Y"),
                    "dataFinal": end_date.strftime("%d/%m/%Y")
                }
                response = requests.get(url, params=params, timeout=config["timeout"])
                response.raise_for_status()
                data = response.json()
                
                if data:
                    df = pd.DataFrame(data)
                    df['data'] = pd.to_datetime(df['data'], dayfirst=True).dt.date
                    df['valor'] = pd.to_numeric(df['valor'], errors='coerce') / 100
                    return df.dropna()
            except Exception as e:
                st.error(f"Erro na API BCB: {str(e)}")
            
            return pd.DataFrame()

        elif api_name == "IBGE":
            # Nova API do IBGE (servicodados.ibge.gov.br)
            if series_code not in config["series"]:
                return pd.DataFrame()
                
            # Calcula o número de meses entre as datas
            num_meses = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1
            periodo = f"{num_meses}%20meses" if num_meses > 1 else "1%20mes"
            
            url = config["base_url"].format(config["series"][series_code], periodo)
            headers = {'User-Agent': 'Mozilla/5.0'}
            
            try:
                response = requests.get(url, headers=headers, timeout=config["timeout"])
                response.raise_for_status()
                data = response.json()
                
                if not data or len(data) == 0:
                    return pd.DataFrame()
                    
                # Processa a nova estrutura da API do IBGE
                resultados = []
                for item in data[0]['resultados'][0]['series'][0]['serie'].values():
                    ano_mes = item['classificacoes'][0]['categoria']
                    valor = item['variavel']['V']
                    
                    try:
                        data_ref = datetime.strptime(ano_mes, "%Y%m").date()
                        if start_date <= data_ref <= end_date:
                            resultados.append({
                                'data': data_ref,
                                'valor': float(valor) / 100
                            })
                    except:
                        continue
                
                if resultados:
                    df = pd.DataFrame(resultados)
                    return df.sort_values('data')
            
            except Exception as e:
                st.error(f"Erro na API IBGE: {str(e)}")
            
            return pd.DataFrame()

    except Exception as e:
        st.error(f"Erro geral na API {api_name}: {str(e)}")
        return pd.DataFrame()

def get_indice_data(indice: str, start_date: date, end_date: date) -> pd.DataFrame:
    """Obtém dados com estratégia de fallback aprimorada"""
    config = CODIGOS_INDICES.get(indice)
    if not config:
        return pd.DataFrame()
    
    # 1. Tentativa: BCB com código principal
    if "BCB" in config.get("api", []):
        df = fetch_api_data("BCB", str(config["codigo"]), start_date, end_date)
        if not df.empty:
            return df
            
        # 2. Tentativa: BCB com fallback
        if config["fallback"]:
            df_fallback = fetch_api_data("BCB", str(config["fallback"]), start_date, end_date)
            if not df_fallback.empty:
                return df_fallback
    
    # 3. Tentativa: IBGE (para INPC e IPCA)
    if indice in ["INPC", "IPCA"] and "IBGE" in config.get("api", []):
        df_ibge = fetch_api_data("IBGE", indice, start_date, end_date)
        if not df_ibge.empty:
            return df_ibge
    
    return pd.DataFrame()

def verificar_dados_inpc():
    """Verifica a disponibilidade de dados do INPC"""
    hoje = date.today()
    ultimo_ano = date(hoje.year - 1, hoje.month, 1)
    
    st.write("### Verificação de dados do INPC")
    
    # Verifica BCB
    try:
        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.188/dados"
        params = {"formato": "json", "dataInicial": "01/01/2020", "dataFinal": hoje.strftime("%d/%m/%Y")}
        response = requests.get(url, params=params, timeout=10)
        data_bcb = response.json()
        st.success(f"BCB: {len(data_bcb)} registros encontrados (último: {data_bcb[-1]['data'] if data_bcb else 'N/A'})")
    except Exception as e:
        st.error(f"Falha ao acessar BCB: {str(e)}")
    
    # Verifica IBGE
    try:
        url = "https://apisidra.ibge.gov.br/values/t/1736/n1/all/v/63/p/all/d/v63%202"
        response = requests.get(url, timeout=10)
        data_ibge = response.json()
        st.success(f"IBGE: {len(data_ibge)-1} registros encontrados" if len(data_ibge) > 1 else "IBGE: Sem dados")
    except Exception as e:
        st.error(f"Falha ao acessar IBGE: {str(e)}")

def verificar_conexao_internet():
    """Verifica se há conexão com a internet"""
    try:
        requests.get("https://www.google.com", timeout=5)
        return True
    except:
        return False
@st.cache_data(ttl=3600)    
def fetch_bcb_data(series_code: str, start_date: date, end_date: date, max_retries: int = 3) -> pd.DataFrame:
    """Busca dados do BCB com tratamento de erros e retentativas"""
    base_url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados"
    
    for attempt in range(max_retries):
        try:
            # Primeiro tenta sem filtro de datas (mais estável)
            if attempt == 0:
                url = base_url.format(series_code)
                params = {"formato": "json"}
            else:
                # Nas tentativas seguintes, tenta com filtro de datas
                url = base_url.format(series_code)
                params = {
                    "formato": "json",
                    "dataInicial": start_date.strftime("%d/%m/%Y"),
                    "dataFinal": end_date.strftime("%d/%m/%Y")
                }
            
            response = requests.get(url, params=params, timeout=30)
            
            # Se for 502, espera um pouco e tenta novamente
            if response.status_code == 502 and attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Backoff exponencial
                continue
                
            response.raise_for_status()
            data = response.json()
            
            if not data:
                return pd.DataFrame()
                
            df = pd.DataFrame(data)
            df['data'] = pd.to_datetime(df['data'], dayfirst=True).dt.date
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce') / 100
            df = df.dropna()
            
            # Filtra localmente se baixamos todos os dados
            if 'dataInicial' not in params:
                mask = (df['data'] >= start_date) & (df['data'] <= end_date)
                df = df[mask].copy()
            
            return df

        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                st.warning(f"Falha ao acessar BCB após {max_retries} tentativas: {str(e)}")
                return pd.DataFrame()
            time.sleep(2 ** attempt)
    
    return pd.DataFrame()
def fetch_ibge_data(series_code: str) -> pd.DataFrame:
    """Busca dados do IBGE com o novo endpoint"""
    series_map = {
        "INPC": "t/1736/n1/all/v/63/p/all/d/v63%202",
        "IPCA": "t/1737/n1/all/v/63/p/all/d/v63%202"
    }
    
    if series_code not in series_map:
        return pd.DataFrame()
    
    base_url = "https://apisidra.ibge.gov.br/values/"
    url = base_url + series_map[series_code]
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if len(data) < 2:
            return pd.DataFrame()
            
        # Processamento dos dados
        df = pd.DataFrame(data[1:], columns=data[0])
        if 'D1C' not in df.columns or 'V' not in df.columns:
            return pd.DataFrame()
            
        df = df[['D1C', 'V']].copy()
        df['data'] = pd.to_datetime(df['D1C'], format='%Y%m', errors='coerce')
        df['valor'] = pd.to_numeric(df['V'], errors='coerce') / 100
        df = df.dropna()
        df['data'] = df['data'].dt.date
        
        return df
        
    except Exception as e:
        st.warning(f"Erro ao acessar IBGE: {str(e)}")
        return pd.DataFrame()
def verificar_disponibilidade_indices():
    """Verifica quais índices estão disponíveis na API com diagnósticos"""
    disponiveis = []
    problemas = []
    hoje = date.today()
    teste_data = date(hoje.year - 1, hoje.month, hoje.day)
    
    if not verificar_conexao_internet():
        st.error("❌ Sem conexão com a internet")
        return [], ["Sem conexão com a internet"]

    for indice, config in CODIGOS_INDICES.items():
        try:
            df = get_indice_data(indice, teste_data, hoje)
            if not df.empty:
                disponiveis.append(indice)
            else:
                problemas.append(f"{indice} (Nenhum dado retornado)")
        except Exception as e:
            problemas.append(f"{indice} (Erro: {str(e)})")
    
    return disponiveis, problemas

def get_indices_disponiveis():
    """Retorna os índices disponíveis com verificação em tempo real"""
    disponiveis, problemas = verificar_disponibilidade_indices()
    
    if not disponiveis:
        mensagem = """
        Nenhum índice econômico disponível no momento.

        🔍 Possíveis causas:
        - Problemas nas APIs dos índices
        - Sem conexão com a internet
        - Códigos das séries desatualizados

        📝 Detalhes técnicos:\n"""
        
        for problema in problemas:
            mensagem += f"• {problema}\n"
        
        mensagem += "\n🛠️ Recomendações:\n"
        mensagem += "- Verifique sua conexão com a internet\n"
        mensagem += "- Tente novamente mais tarde\n"
        mensagem += "- Contate o suporte técnico\n"
        
        st.error(mensagem)
        return {}

    nomes = {
        "IPCA": "IPCA (IBGE/BCB)",
        "IGPM": "IGP-M (FGV/BCB)",
        "INPC": "INPC (IBGE/BCB)",
        "INCC": "INCC (FGV/BCB)"
    }
    
    return {k: v for k, v in nomes.items() if k in disponiveis}

def calcular_correcao_individual(valor_original, data_inicio: date, data_fim: date, indice: str):
    """
    Calcula a correção individual com tratamento robusto de falhas
    Retorna um dicionário com todos os detalhes do cálculo
    """
    # <<< INÍCIO DA ALTERAÇÃO >>>
    # Ajusta as datas de busca para índices com defasagem de 1 mês
    effective_start_date = data_inicio
    effective_end_date = data_fim
    if indice in INDICES_COM_LAG:
        effective_start_date = data_inicio - relativedelta(months=1)
        effective_end_date = data_fim - relativedelta(months=1)

    if effective_start_date >= effective_end_date:
    # <<< FIM DA ALTERAÇÃO >>>
        return {
            'indice': indice,
            'data_inicio': data_inicio.strftime("%m/%Y"),
            'data_fim': data_fim.strftime("%m/%Y"),
            'valor_original': valor_original,
            'fator_correcao': 1.0,
            'variacao_percentual': 0.0,
            'valor_corrigido': valor_original,
            'detalhes': pd.DataFrame(),
            'sucesso': True,
            'mensagem': 'Data de origem posterior à referência'
        }
    
    try:
        # <<< ALTERAÇÃO: Usa as datas efetivas para buscar os dados >>>
        dados = get_indice_data(indice, effective_start_date, effective_end_date)
        
        if dados.empty:
            return {
                'indice': indice,
                'data_inicio': data_inicio.strftime("%m/%Y"),
                'data_fim': data_fim.strftime("%m/%Y"),
                'valor_original': valor_original,
                'fator_correcao': 1.0,
                'variacao_percentual': 0.0,
                'valor_corrigido': valor_original,
                'detalhes': pd.DataFrame(),
                'sucesso': False,
                'mensagem': f'Não foram encontrados dados para {indice} no período'
            }
        
        # Calcular variação acumulada
        fator_correcao = (1 + dados['valor']).prod()
        variacao_percentual = (fator_correcao - 1) * 100
        valor_corrigido = valor_original * fator_correcao
        
        return {
            'indice': indice,
            'data_inicio': data_inicio.strftime("%m/%Y"),
            'data_fim': data_fim.strftime("%m/%Y"),
            'valor_original': valor_original,
            'fator_correcao': fator_correcao,
            'variacao_percentual': variacao_percentual,
            'valor_corrigido': valor_corrigido,
            'detalhes': dados,
            'sucesso': True,
            'mensagem': 'Cálculo realizado com sucesso'
        }
    
    except Exception as e:
        return {
            'indice': indice,
            'data_inicio': data_inicio.strftime("%m/%Y"),
            'data_fim': data_fim.strftime("%m/%Y"),
            'valor_original': valor_original,
            'fator_correcao': 1.0,
            'variacao_percentual': 0.0,
            'valor_corrigido': valor_original,
            'detalhes': pd.DataFrame(),
            'sucesso': False,
            'mensagem': f'Erro ao calcular correção com {indice}: {str(e)}'
        }

def fetch_multiple_indices(indices, start_date, end_date):
    """Busca dados de vários índices simultaneamente"""
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {}
        
        for indice in indices:
            config = CODIGOS_INDICES.get(indice)
            if not config:
                continue
            
            # <<< INÍCIO DA ALTERAÇÃO >>>
            # Ajusta as datas de busca para índices com defasagem
            effective_start_date = start_date
            effective_end_date = end_date
            if indice in INDICES_COM_LAG:
                effective_start_date = start_date - relativedelta(months=1)
                effective_end_date = end_date - relativedelta(months=1)
            
            futures[executor.submit(get_indice_data, indice, effective_start_date, effective_end_date)] = indice
            # <<< FIM DA ALTERAÇÃO >>>
    
        results = {}
        for future in concurrent.futures.as_completed(futures):
            indice = futures[future]
            try:
                results[indice] = future.result()
            except Exception as e:
                st.error(f"Erro ao buscar {indice}: {str(e)}")
                results[indice] = pd.DataFrame()
        
        return results

def calcular_correcao_media(valor_original, data_inicio: date, data_fim: date, indices: list):
    """
    Calcula a correção pela média dos índices (versão paralelizada)
    Retorna um dicionário com todos os detalhes do cálculo
    """
    if data_inicio >= data_fim:
        return {
            'indices': indices,
            'data_inicio': data_inicio.strftime("%m/%Y"),
            'data_fim': data_fim.strftime("%m/%Y"),
            'valor_original': valor_original,
            'fator_correcao': 1.0,
            'variacao_percentual': 0.0,
            'valor_corrigido': valor_original,
            'resultados_parciais': [],
            'indices_com_falha': indices,
            'sucesso': False,
            'mensagem': 'Data de origem posterior à referência'
        }
    
    # <<< ALTERAÇÃO: A lógica de ajuste de data foi movida para dentro de fetch_multiple_indices >>>
    dados_indices = fetch_multiple_indices(indices, data_inicio, data_fim)
    
    resultados = []
    fatores = []
    indices_com_falha = []
    
    for indice, dados in dados_indices.items():
        if dados.empty:
            indices_com_falha.append(indice)
            st.warning(f"Não foram encontrados dados para {indice} no período")
            continue
        
        fator = (1 + dados['valor']).prod()
        fatores.append(fator)
        resultados.append({
            'indice': indice,
            'data_inicio': data_inicio.strftime("%m/%Y"),
            'data_fim': data_fim.strftime("%m/%Y"),
            'valor_original': valor_original,
            'fator_correcao': fator,
            'variacao_percentual': (fator - 1) * 100,
            'valor_corrigido': valor_original * fator,
            'detalhes': dados,
            'sucesso': True,
            'mensagem': 'Cálculo realizado com sucesso'
        })
    
    if not fatores:
        return {
            'indices': indices,
            'data_inicio': data_inicio.strftime("%m/%Y"),
            'data_fim': data_fim.strftime("%m/%Y"),
            'valor_original': valor_original,
            'fator_correcao': 1.0,
            'variacao_percentual': 0.0,
            'valor_corrigido': valor_original,
            'resultados_parciais': [],
            'indices_com_falha': indices,
            'sucesso': False,
            'mensagem': 'Nenhum índice retornou dados válidos'
        }
    
    # Calcula a média geométrica dos fatores
    fator_medio = (pd.Series(fatores).prod()) ** (1/len(fatores))
    variacao_media = (fator_medio - 1) * 100
    
    return {
        'indices': [r['indice'] for r in resultados],
        'data_inicio': data_inicio.strftime("%m/%Y"),
        'data_fim': data_fim.strftime("%m/%Y"),
        'valor_original': valor_original,
        'fator_correcao': fator_medio,
        'variacao_percentual': variacao_media,
        'valor_corrigido': valor_original * fator_medio,
        'resultados_parciais': resultados,
        'indices_com_falha': indices_com_falha,
        'sucesso': True,
        'mensagem': f'Cálculo realizado com {len(resultados)}/{len(indices)} índices'
    }

def formatar_moeda(valor):
    """Formata valores monetários no padrão brasileiro"""
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
