import requests
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st

# Códigos das séries no BCB (SGS)
CODIGOS_INDICES = {
    "IPCA": 433,
    "IGPM": 189,
    "INPC": 188,
    "INCC": 192
}

def fetch_bcb_data(series_code, start_date, end_date):
    """Busca dados do Banco Central"""
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados"
    params = {
        "formato": "json",
        "dataInicial": start_date.strftime("%d/%m/%Y"),
        "dataFinal": end_date.strftime("%d/%m/%Y")
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Converter para DataFrame e processar
        df = pd.DataFrame(data)
        df['data'] = pd.to_datetime(df['data'], dayfirst=True)
        df['valor'] = pd.to_numeric(df['valor'])
        df.set_index('data', inplace=True)
        
        return df
    
    except requests.exceptions.RequestException as e:
        st.error(f"Erro ao buscar dados do BCB para série {series_code}: {str(e)}")
        return pd.DataFrame()

def get_indices_disponiveis():
    """Retorna os índices disponíveis para cálculo"""
    return {
        "IPCA": "IPCA (IBGE/BCB)",
        "IGPM": "IGP-M (FGV/BCB)",
        "INPC": "INPC (IBGE/BCB)",
        "INCC": "INCC (FGV/BCB)"
    }

def get_variacao_indice(indice, data_inicio, data_fim):
    """
    Calcula a variação acumulada de um índice entre duas datas usando API real
    """
    # Verificar se o índice é suportado
    if indice not in CODIGOS_INDICES:
        raise ValueError(f"Índice {indice} não suportado")
    
    # Obter código da série
    codigo_serie = CODIGOS_INDICES[indice]
    
    # Ajustar datas para garantir cobertura
    start_date = data_inicio - timedelta(days=30)
    end_date = data_fim
    
    # Buscar dados
    dados = fetch_bcb_data(codigo_serie, start_date, end_date)
    
    if dados.empty:
        raise ValueError(f"Não foi possível obter dados para {indice} no período solicitado")
    
    # Filtrar para o período exato
    mask = (dados.index >= data_inicio) & (dados.index <= data_fim)
    dados_periodo = dados[mask]
    
    if dados_periodo.empty:
        raise ValueError(f"Nenhum dado disponível para {indice} entre {data_inicio} e {data_fim}")
    
    # Calcular variação acumulada
    variacao_acumulada = (dados_periodo['valor'] + 1).prod() - 1
    
    return variacao_acumulada * 100  # Retorna em porcentagem

def calcular_correcao(valor_original, data_original, data_referencia, indices):
    """
    Calcula o valor corrigido com base nos índices selecionados (usando APIs reais)
    """
    if len(indices) == 1:
        # Correção por índice único
        indice = indices[0]
        variacao = get_variacao_indice(indice, data_original, data_referencia)
        return valor_original * (1 + variacao / 100)
    else:
        # Correção por média de índices
        variacoes = []
        for indice in indices:
            try:
                variacao = get_variacao_indice(indice, data_original, data_referencia)
                variacoes.append(variacao)
            except ValueError as e:
                st.warning(f"Não foi possível calcular variação para {indice}: {str(e)}")
                continue
        
        if not variacoes:
            raise ValueError("Nenhum índice válido para cálculo")
        
        media_variacoes = sum(variacoes) / len(variacoes)
        return valor_original * (1 + media_variacoes / 100)
