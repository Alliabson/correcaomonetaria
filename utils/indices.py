import requests
import pandas as pd
from datetime import datetime, date
import streamlit as st
import concurrent.futures
import time
import sidrapy

# Configuração de Headers para simular um navegador (Resolve o erro 403 do BCB)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive'
}

# Códigos das séries
CODIGOS_INDICES = {
    "IPCA": {
        "source": "IBGE",
        "table_code": "1737", # Tabela IPCA no SIDRA
        "variable": "63",     # Variação mensal
        "fallback_bcb": 433   # Código BCB se IBGE falhar
    },
    "INPC": {
        "source": "IBGE",
        "table_code": "1736", # Tabela INPC no SIDRA
        "variable": "63",
        "fallback_bcb": 188
    },
    "IGPM": {
        "source": "BCB",
        "codigo": 189,
        "fallback": None
    },
    "INCC": {
        "source": "BCB",
        "codigo": 192,
        "fallback": 7458 # INCC-DI
    }
}

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_bcb_data(series_code: int, start_date: date = None, end_date: date = None) -> pd.DataFrame:
    """Busca dados do BCB com Headers corretos para evitar erro 403"""
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados"
    
    try:
        # Tenta pegar tudo primeiro (API do BCB é mais rápida sem filtros complexos às vezes)
        params = {"formato": "json"}
        # Se as datas forem passadas, filtra na API para economizar banda
        if start_date and end_date:
            params["dataInicial"] = start_date.strftime("%d/%m/%Y")
            params["dataFinal"] = end_date.strftime("%d/%m/%Y")

        response = requests.get(url, params=params, headers=HEADERS, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df['data'] = pd.to_datetime(df['data'], dayfirst=True).dt.date
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce') / 100
        df = df.dropna()
        
        # Garantir filtro de data se a API retornou tudo
        if start_date and end_date:
            mask = (df['data'] >= start_date) & (df['data'] <= end_date)
            df = df[mask].copy()
            
        return df.sort_values('data')

    except Exception as e:
        # st.error(f"Erro BCB {series_code}: {str(e)}") # Debug only
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_ibge_data(table_code: str, variable: str, start_date: date, end_date: date) -> pd.DataFrame:
    """Busca dados do IBGE usando sidrapy (Resolve erro 500)"""
    try:
        # Sidrapy espera período no formato YYYYMM
        # Para garantir segurança, pegamos um período amplo ou 'all' e filtramos localmente
        # 'last' pega os últimos meses. Para histórico longo, melhor 'all' ou range específico.
        # Aqui vamos pegar 'all' para garantir histórico, pois cacheamos o resultado.
        
        data = sidrapy.get_table(
            table_code=table_code,
            territorial_level="1", # Brasil
            ibge_territorial_code="all",
            variable=variable,
            period="all" 
        )
        
        if data is None or data.empty or 'D1C' not in data.columns:
            return pd.DataFrame()

        # O primeiro registro geralmente é cabeçalho/metadado no sidrapy
        df = data.iloc[1:].copy()
        
        # D1C é a coluna de data (Mês/Ano)
        # V é o valor
        df['data'] = pd.to_datetime(df['D1C'], format='%Y%m', errors='coerce').dt.date
        df['valor'] = pd.to_numeric(df['V'], errors='coerce') / 100
        
        df = df.dropna(subset=['data', 'valor'])
        
        if start_date and end_date:
            mask = (df['data'] >= start_date) & (df['data'] <= end_date)
            df = df[mask].copy()

        return df.sort_values('data')

    except Exception as e:
        # st.error(f"Erro IBGE {table_code}: {str(e)}") # Debug only
        return pd.DataFrame()

def get_indice_data(indice: str, start_date: date, end_date: date) -> pd.DataFrame:
    """Orquestrador que decide qual API chamar baseado no índice"""
    config = CODIGOS_INDICES.get(indice)
    if not config:
        return pd.DataFrame()

    df = pd.DataFrame()

    # Estratégia para índices do IBGE (IPCA/INPC)
    if config.get("source") == "IBGE":
        # Tenta Sidrapy primeiro (Oficial)
        df = fetch_ibge_data(config["table_code"], config["variable"], start_date, end_date)
        
        # Se falhar, tenta fallback no BCB (Séries espelhadas)
        if df.empty and "fallback_bcb" in config:
            df = fetch_bcb_data(config["fallback_bcb"], start_date, end_date)

    # Estratégia para índices do BCB/FGV (IGPM/INCC)
    elif config.get("source") == "BCB":
        df = fetch_bcb_data(config["codigo"], start_date, end_date)
        
        # Fallback para outra série do BCB se existir
        if df.empty and config.get("fallback"):
            df = fetch_bcb_data(config["fallback"], start_date, end_date)

    return df

def get_indices_disponiveis():
    """Verifica disponibilidade real testando os últimos 12 meses"""
    hoje = date.today()
    inicio_teste = date(hoje.year - 1, hoje.month, 1)
    
    disponiveis = {}
    
    # Lista de índices amigável
    nomes = {
        "IPCA": "IPCA (IBGE/BCB)",
        "IGPM": "IGP-M (FGV/BCB)",
        "INPC": "INPC (IBGE/BCB)",
        "INCC": "INCC (FGV/BCB)"
    }

    # Verifica cada um
    # Usamos ThreadPool para não travar a UI na verificação
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(get_indice_data, k, inicio_teste, hoje): k for k in nomes.keys()}
        
        for future in concurrent.futures.as_completed(futures):
            key = futures[future]
            try:
                df = future.result()
                if not df.empty:
                    disponiveis[key] = nomes[key]
            except:
                pass

    return disponiveis

def calcular_correcao_individual(valor_original, data_inicio: date, data_fim: date, indice: str):
    """Calcula correção de um único valor"""
    if data_inicio >= data_fim:
        return {'sucesso': True, 'valor_corrigido': valor_original, 'fator_correcao': 1.0, 'variacao_percentual': 0.0, 'mensagem': 'Data inicial >= Data final'}

    df = get_indice_data(indice, data_inicio, data_fim)
    
    if df.empty:
        return {'sucesso': False, 'mensagem': f'Sem dados para {indice} no período.', 'valor_corrigido': valor_original}

    # Cálculo acumulado: Produtório de (1 + taxa)
    fator = (1 + df['valor']).prod()
    valor_corrigido = valor_original * fator
    
    return {
        'sucesso': True,
        'valor_corrigido': valor_corrigido,
        'fator_correcao': fator,
        'variacao_percentual': (fator - 1) * 100,
        'detalhes': df,
        'indices': [indice]
    }

def calcular_correcao_media(valor_original, data_inicio: date, data_fim: date, indices: list):
    """Calcula média de múltiplos índices"""
    resultados_individuais = []
    
    for idx in indices:
        res = calcular_correcao_individual(valor_original, data_inicio, data_fim, idx)
        if res['sucesso']:
            resultados_individuais.append(res['fator_correcao'])
    
    if not resultados_individuais:
        return {'sucesso': False, 'mensagem': 'Nenhum índice retornou dados.', 'valor_corrigido': valor_original}
        
    # Média aritmética dos fatores acumulados (prática jurídica comum)
    # Obs: Alguns usam média geométrica, mas a aritmética dos fatores finais é comum em tribunais.
    # Se preferir geométrica: fator_medio = (pd.Series(resultados_individuais).prod()) ** (1/len(resultados_individuais))
    
    fator_medio = sum(resultados_individuais) / len(resultados_individuais)
    valor_corrigido = valor_original * fator_medio
    
    return {
        'sucesso': True,
        'valor_corrigido': valor_corrigido,
        'fator_correcao': fator_medio,
        'variacao_percentual': (fator_medio - 1) * 100,
        'indices': indices
    }

def formatar_moeda(valor):
    if valor is None: return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
