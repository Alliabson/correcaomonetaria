import requests
import pandas as pd
from datetime import datetime, date
import streamlit as st
from functools import lru_cache

# Códigos das séries no BCB (SGS)
CODIGOS_INDICES = {
    "IPCA": 433,
    "IGPM": 189,
    "INPC": 188,
    "INCC": 192
}

@lru_cache(maxsize=32)
def fetch_bcb_data(series_code, start_date: date, end_date: date):
    """Busca dados do Banco Central com tratamento de datas"""
    try:
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados"
        params = {
            "formato": "json",
            "dataInicial": start_date.strftime("%d/%m/%Y"),
            "dataFinal": end_date.strftime("%d/%m/%Y")
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            raise ValueError("Nenhum dado retornado pela API")
        
        # Converter para DataFrame e processar
        df = pd.DataFrame(data)
        df['data'] = pd.to_datetime(df['data'], dayfirst=True).dt.date
        df['valor'] = pd.to_numeric(df['valor']) / 100  # Converter porcentagem para decimal
        df = df[(df['data'] >= start_date) & (df['data'] <= end_date)]
        
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

def calcular_correcao_individual(valor_original, data_inicio: date, data_fim: date, indice: str):
    """
    Calcula a correção individual seguindo a lógica do exemplo
    Retorna um dicionário com todos os detalhes do cálculo
    """
    try:
        # Buscar dados do índice
        dados = fetch_bcb_data(CODIGOS_INDICES[indice], data_inicio, data_fim)
        
        if dados.empty:
            raise ValueError(f"Não foram encontrados dados para {indice} no período")
        
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
            'detalhes': dados
        }
    
    except Exception as e:
        raise ValueError(f"Erro ao calcular correção com {indice}: {str(e)}")

def calcular_correcao_media(valor_original, data_inicio: date, data_fim: date, indices: list):
    """
    Calcula a correção pela média dos índices
    Retorna um dicionário com todos os detalhes do cálculo
    """
    resultados = []
    fatores = []
    
    for indice in indices:
        try:
            resultado = calcular_correcao_individual(valor_original, data_inicio, data_fim, indice)
            resultados.append(resultado)
            fatores.append(resultado['fator_correcao'])
        except ValueError as e:
            st.warning(str(e))
            continue
    
    if not fatores:
        raise ValueError("Nenhum índice válido para cálculo da média")
    
    # Calcular média geométrica dos fatores de correção
    fator_medio = (pd.Series(fatores).prod()) ** (1/len(fatores))
    variacao_media = (fator_medio - 1) * 100
    valor_corrigido = valor_original * fator_medio
    
    return {
        'indices': [r['indice'] for r in resultados],
        'data_inicio': data_inicio.strftime("%m/%Y"),
        'data_fim': data_fim.strftime("%m/%Y"),
        'valor_original': valor_original,
        'fator_correcao': fator_medio,
        'variacao_percentual': variacao_media,
        'valor_corrigido': valor_corrigido,
        'resultados_parciais': resultados
    }

def formatar_moeda(valor):
    """Formata valores monetários no padrão brasileiro"""
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
