import requests
import pandas as pd
from datetime import datetime, date, timedelta
import streamlit as st
import concurrent.futures
import time
from typing import Dict, List, Optional, Tuple
import json
import socket
from dateutil.relativedelta import relativedelta

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

# Códigos atualizados das séries
CODIGOS_INDICES = {
    "IPCA": {"codigo": 433, "fallback": 1619, "api": ["BCB", "IBGE"]},
    "IGPM": {"codigo": 189, "fallback": None, "api": ["BCB"]},
    "INPC": {"codigo": 188, "fallback": 11426, "api": ["BCB", "IBGE"]},
    "INCC": {"codigo": 192, "fallback": 7458, "api": ["BCB"]}
}

@st.cache_data(ttl=3600, show_spinner="Buscando dados econômicos...")
def fetch_api_data(api_name: str, series_code: str, start_date: date, end_date: date) -> pd.DataFrame:
    config = API_CONFIG.get(api_name)
    if not config:
        return pd.DataFrame()

    try:
        if api_name == "BCB":
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
    except requests.exceptions.RequestException as e:
        st.warning(f"Falha ao buscar dados na API {api_name} para o código {series_code}: {e}")
    except Exception as e:
        st.error(f"Erro ao processar dados da API {api_name}: {e}")
        
    return pd.DataFrame()

def get_indice_data(indice: str, start_date: date, end_date: date) -> pd.DataFrame:
    config = CODIGOS_INDICES.get(indice)
    if not config:
        return pd.DataFrame()

    for api_source in config.get("api", []):
        if api_source == "BCB":
            # Tenta código principal
            df = fetch_api_data("BCB", str(config["codigo"]), start_date, end_date)
            if not df.empty:
                return df
            # Tenta fallback se existir
            if config["fallback"]:
                df_fallback = fetch_api_data("BCB", str(config["fallback"]), start_date, end_date)
                if not df_fallback.empty:
                    return df_fallback
        elif api_source == "IBGE":
            df_ibge = fetch_api_data("IBGE", indice, start_date, end_date)
            if not df_ibge.empty:
                return df_ibge
                
    return pd.DataFrame()

def verificar_conexao_internet():
    try:
        requests.get("https://www.google.com", timeout=5)
        return True
    except requests.exceptions.RequestException:
        return False

def get_indices_disponiveis():
    if not verificar_conexao_internet():
        st.error("Sem conexão com a internet.")
        return {}

    disponiveis = {}
    hoje = date.today()
    teste_data_inicio = date(hoje.year - 1, hoje.month, 1)
    teste_data_fim = hoje - timedelta(days=1)
    
    for indice in CODIGOS_INDICES.keys():
        df = get_indice_data(indice, teste_data_inicio, teste_data_fim)
        if not df.empty:
            disponiveis[indice] = f"{indice} (Fonte: {CODIGOS_INDICES[indice]['api'][0]})"
            
    return disponiveis

def calcular_correcao_individual(valor_original: float, data_inicio: date, data_fim: date, indice: str, igpm_retroacao: int = 1) -> Dict:
    meses_a_retroagir = 0
    if indice in ["INCC", "IPCA", "INPC"]:
        meses_a_retroagir = 1
    elif indice == "IGPM":
        meses_a_retroagir = igpm_retroacao

    effective_start_date = data_inicio - relativedelta(months=meses_a_retroagir)
    effective_end_date = data_fim - relativedelta(months=meses_a_retroagir)

    if effective_start_date >= effective_end_date:
        return {'sucesso': True, 'valor_corrigido': valor_original, 'fator_correcao': 1.0, 'variacao_percentual': 0.0, 'mensagem': 'Data de origem posterior à referência', 'indices': [indice]}

    try:
        dados = get_indice_data(indice, effective_start_date, effective_end_date)
        if dados.empty:
            return {'sucesso': False, 'valor_corrigido': valor_original, 'fator_correcao': 1.0, 'variacao_percentual': 0.0, 'mensagem': f'Não foram encontrados dados para {indice} no período', 'indices': [indice]}
        
        fator_correcao = (1 + dados['valor']).prod()
        variacao_percentual = (fator_correcao - 1) * 100
        valor_corrigido = valor_original * fator_correcao
        
        return {'sucesso': True, 'valor_corrigido': valor_corrigido, 'fator_correcao': fator_correcao, 'variacao_percentual': variacao_percentual, 'detalhes': dados, 'indices': [indice]}
    except Exception as e:
        return {'sucesso': False, 'valor_corrigido': valor_original, 'fator_correcao': 1.0, 'variacao_percentual': 0.0, 'mensagem': f'Erro ao calcular correção com {indice}: {e}', 'indices': [indice]}

def fetch_multiple_indices(indices: List[str], start_date: date, end_date: date, igpm_retroacao: int = 1) -> Dict[str, pd.DataFrame]:
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {}
        for indice in indices:
            if not CODIGOS_INDICES.get(indice):
                continue
            
            meses_a_retroagir = 0
            if indice in ["INCC", "IPCA", "INPC"]:
                meses_a_retroagir = 1
            elif indice == "IGPM":
                meses_a_retroagir = igpm_retroacao
                
            effective_start_date = start_date - relativedelta(months=meses_a_retroagir)
            effective_end_date = end_date - relativedelta(months=meses_a_retroagir)
            
            futures[executor.submit(get_indice_data, indice, effective_start_date, effective_end_date)] = indice
    
        results = {}
        for future in concurrent.futures.as_completed(futures):
            indice = futures[future]
            try:
                results[indice] = future.result()
            except Exception as e:
                st.error(f"Erro ao buscar {indice}: {e}")
                results[indice] = pd.DataFrame()
        return results

def calcular_correcao_media(valor_original: float, data_inicio: date, data_fim: date, indices: List[str], igpm_retroacao: int = 1) -> Dict:
    if data_inicio >= data_fim:
        return {'sucesso': False, 'valor_corrigido': valor_original, 'fator_correcao': 1.0, 'variacao_percentual': 0.0, 'mensagem': 'Data de origem posterior à referência', 'indices': indices}
    
    dados_indices = fetch_multiple_indices(indices, data_inicio, data_fim, igpm_retroacao)
    
    fatores = []
    indices_sucesso = []
    for indice, dados in dados_indices.items():
        if not dados.empty:
            fator = (1 + dados['valor']).prod()
            fatores.append(fator)
            indices_sucesso.append(indice)
        else:
            st.warning(f"Não foram encontrados dados para o índice {indice} no período solicitado.")
    
    if not fatores:
        return {'sucesso': False, 'valor_corrigido': valor_original, 'fator_correcao': 1.0, 'variacao_percentual': 0.0, 'mensagem': 'Nenhum índice retornou dados válidos', 'indices': indices}

    fator_medio = (pd.Series(fatores).prod()) ** (1 / len(fatores))
    variacao_media = (fator_medio - 1) * 100
    
    return {
        'sucesso': True,
        'valor_corrigido': valor_original * fator_medio,
        'fator_correcao': fator_medio,
        'variacao_percentual': variacao_media,
        'indices': indices_sucesso,
        'mensagem': f'Cálculo realizado com {len(indices_sucesso)}/{len(indices)} índices'
    }

def formatar_moeda(valor: float) -> str:
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
