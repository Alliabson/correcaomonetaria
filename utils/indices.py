import requests
import pandas as pd
from datetime import datetime, date, timedelta
import streamlit as st
import concurrent.futures
import time
from typing import Dict, List, Optional

# Códigos das séries no BCB (SGS) com fallbacks alternativos
CODIGOS_INDICES = {
    "IPCA": {"codigo": 433, "fallback": 1619},  # IPCA15 como fallback
    "IGPM": {"codigo": 189, "fallback": None},
    "INPC": {"codigo": 188, "fallback": None},
    "INCC": {"codigo": 192, "fallback": 7458}  # INCC-DI como fallback
}

@st.cache_data(ttl=3600, show_spinner="Buscando dados do Banco Central...")
def fetch_bcb_data(series_code, start_date: date, end_date: date):
    """Busca dados do Banco Central com tratamento robusto de falhas"""
    base_url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados"
    params = {
        "formato": "json",
        "dataInicial": start_date.strftime("%d/%m/%Y"),
        "dataFinal": end_date.strftime("%d/%m/%Y")
    }
    
    attempts = 0
    max_attempts = 3
    retry_delay = 2  # segundos
    
    while attempts < max_attempts:
        try:
            response = requests.get(
                base_url.format(series_code),
                params=params,
                timeout=30,
                headers={'User-Agent': 'Mozilla/5.0'}
            )
            
            # Tratamento especial para erros 502
            if response.status_code == 502 and attempts < max_attempts - 1:
                st.warning(f"Serviço indisponível (502). Tentando novamente... ({attempts+1}/{max_attempts})")
                time.sleep(retry_delay * (attempts + 1))  # Aumenta o delay a cada tentativa
                attempts += 1
                continue
                
            response.raise_for_status()
            
            data = response.json()
            
            if not data:
                return pd.DataFrame()
                
            # Processamento dos dados
            df = pd.DataFrame(data)
            df['data'] = pd.to_datetime(df['data'], dayfirst=True).dt.date
            df['valor'] = pd.to_numeric(df['valor']) / 100
            return df[(df['data'] >= start_date) & (df['data'] <= end_date)]
            
        except requests.exceptions.RequestException as e:
            if attempts < max_attempts - 1:
                st.warning(f"Erro na requisição ({type(e).__name__}). Tentando novamente... ({attempts+1}/{max_attempts})")
                time.sleep(retry_delay * (attempts + 1))
                attempts += 1
            else:
                st.error(f"Falha ao buscar dados: {str(e)}")
                return pd.DataFrame()
    
    return pd.DataFrame()

def get_indice_data(indice: str, start_date: date, end_date: date) -> pd.DataFrame:
    """Obtém dados com fallback automático"""
    config = CODIGOS_INDICES.get(indice)
    if not config:
        raise ValueError(f"Índice {indice} não configurado")
    
    # Tentativa com código principal
    df = fetch_bcb_data(config["codigo"], start_date, end_date)
    
    # Se falhou e tem fallback, tenta com fallback
    if df.empty and config["fallback"]:
        st.info(f"Usando série alternativa para {indice}...")
        df = fetch_bcb_data(config["fallback"], start_date, end_date)
    
    return df

def verificar_disponibilidade_indices():
    """Verifica quais índices estão disponíveis na API"""
    disponiveis = []
    hoje = date.today()
    teste_data = date(hoje.year - 1, hoje.month, hoje.day)
    
    for indice, config in CODIGOS_INDICES.items():
        try:
            # Tenta primeiro com código principal
            dados = fetch_bcb_data(config["codigo"], teste_data, hoje)
            
            # Se falhou e tem fallback, tenta com fallback
            if dados.empty and config["fallback"]:
                dados = fetch_bcb_data(config["fallback"], teste_data, hoje)
            
            if not dados.empty:
                disponiveis.append(indice)
        except:
            continue
    
    return disponiveis

def get_indices_disponiveis():
    """Retorna os índices disponíveis com verificação em tempo real"""
    disponiveis = verificar_disponibilidade_indices()
    
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
    if data_inicio >= data_fim:
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
        dados = get_indice_data(indice, data_inicio, data_fim)
        
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
                
            # Submete tanto o código principal quanto o fallback
            futures[executor.submit(get_indice_data, indice, start_date, end_date)] = indice
        
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
    
    # Busca todos os índices de uma vez (paralelizado)
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
