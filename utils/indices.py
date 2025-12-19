import pandas as pd
from datetime import datetime, date
import streamlit as st
import sidrapy
import requests
import logging
import sqlite3
import json
import os
import time

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Indices")

# ==============================================================================
# 1. CONFIGURAÇÃO DAS FONTES
# ==============================================================================
# Mapeamento claro: Quem busca o quê.
# IPCA e INPC -> Melhor buscar no IBGE (Sidra) que é a fonte primária.
# IGPM, INCC, SELIC -> IPEA Data (espelho oficial).

CONFIG_FONTES = {
    "IPCA": {
        "metodo": "IBGE",
        "params": {"tabela": "1737", "variavel": "63", "geral": "2265"}
    },
    "INPC": {
        "metodo": "IBGE",
        "params": {"tabela": "1736", "variavel": "44", "geral": "2289"}
    },
    "IGPM": {
        "metodo": "IPEA",
        "codigo": "IGP12_IGPM12"
    },
    "INCC": {  # Usando INCC-DI
        "metodo": "IPEA",
        "codigo": "IGP12_INCC12"
    },
    "SELIC": {
        "metodo": "IPEA",
        "codigo": "BM12_TJOVER12"  # Taxa acumulada no mês
    }
}

# ==============================================================================
# 2. FUNÇÕES DE COLETA PADRONIZADAS
# ==============================================================================

def _padronizar_dataframe(df: pd.DataFrame, origem: str) -> pd.DataFrame:
    """
    Função vital: Garante que O DATAFRAME TENHA SEMPRE A MESMA CARA.
    Saída obrigatória: colunas ['data', 'valor'], ordenado por data.
    """
    if df.empty:
        return df

    try:
        # 1. Garantir conversão de data para datetime.date (remove horas)
        df['data'] = pd.to_datetime(df['data']).dt.date
        
        # 2. Garantir que valor seja float
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        
        # 3. Remover linhas vazias
        df = df.dropna()
        
        # 4. Tratamento de Escala (Regra dos 10%)
        # Se a média dos valores for maior que 0.10 (10%), assume-se que está em percentual
        # Ex: O dado veio 0.50 (que seria 50% se lido errado, ou 0.50% se lido certo)
        # Dividimos por 100 para virar 0.005
        media_valores = df['valor'].abs().mean()
        if media_valores > 0.5:  # Margem de segurança
            df['valor'] = df['valor'] / 100
            
        # 5. Ordenar e limpar colunas
        return df[['data', 'valor']].sort_values('data')
        
    except Exception as e:
        logger.error(f"Erro ao padronizar dados de {origem}: {e}")
        return pd.DataFrame()

def buscar_ibge_sidra(params: dict) -> pd.DataFrame:
    """Busca no IBGE via Sidrapy e retorna estrutura padrão"""
    try:
        data = sidrapy.get_table(
            table_code=params['tabela'],
            territorial_level="1",
            ibge_territorial_code="all",
            variable=params['variavel'],
            period="last 120",  # Últimos 10 anos (120 meses)
            classifications={"315": params['geral']}
        )
        
        if data.empty or 'V' not in data.columns:
            return pd.DataFrame()
            
        # Sidrapy retorna primeira linha como cabeçalho, precisamos descartar
        df = data.iloc[1:].copy()
        
        # Mapear colunas do IBGE para nosso padrão
        # V = Valor, D2C = Data (formato YYYYMM)
        df_final = pd.DataFrame()
        df_final['valor'] = df['V']
        df_final['data'] = pd.to_datetime(df['D2C'], format="%Y%m", errors='coerce')
        
        # IBGE Sidra SEMPRE retorna o valor em percentual textual (ex: "0.56").
        # Precisamos dividir por 100, mas faremos isso no padronizador.
        
        return _padronizar_dataframe(df_final, "IBGE")
        
    except Exception as e:
        logger.error(f"Erro IBGE: {e}")
        return pd.DataFrame()

def buscar_ipea_api(codigo: str) -> pd.DataFrame:
    """Busca no IPEA via Requests direto (mais leve) e retorna estrutura padrão"""
    url = f"http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{codigo}')"
    
    try:
        # Timeout curto pois IPEA às vezes trava
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            dados = response.json()
            if 'value' in dados and len(dados['value']) > 0:
                df = pd.DataFrame(dados['value'])
                
                # O IPEA retorna: VALDATA (ex: 2023-01-01T00:00:00) e VALVALOR
                df_final = pd.DataFrame()
                df_final['data'] = df['VALDATA']
                df_final['valor'] = df['VALVALOR']
                
                return _padronizar_dataframe(df_final, "IPEA")
                
    except Exception as e:
        logger.error(f"Erro IPEA ({codigo}): {e}")
        
    return pd.DataFrame()

# ==============================================================================
# 3. GERENCIAMENTO DE CACHE (COM CORREÇÃO DE INICIALIZAÇÃO)
# ==============================================================================

def _inicializar_sessao():
    """Garante que a variável de sessão exista antes de usar"""
    if 'cache_indices_ram' not in st.session_state:
        st.session_state['cache_indices_ram'] = {}

def get_dados_indice(nome_indice: str) -> pd.DataFrame:
    # 1. Garante inicialização
    _inicializar_sessao()
    
    # 2. Verifica memória RAM (usando sintaxe de dicionário que é mais segura)
    if nome_indice in st.session_state['cache_indices_ram']:
        return st.session_state['cache_indices_ram'][nome_indice]
    
    # 3. Se não tem, busca na fonte
    config = CONFIG_FONTES.get(nome_indice)
    if not config:
        return pd.DataFrame()
    
    df = pd.DataFrame()
    if config['metodo'] == "IBGE":
        df = buscar_ibge_sidra(config['params'])
    elif config['metodo'] == "IPEA":
        df = buscar_ipea_api(config['codigo'])
        
    # 4. Salva na memória e retorna
    if not df.empty:
        st.session_state['cache_indices_ram'][nome_indice] = df
        
    return df

# ==============================================================================
# 4. FUNÇÕES PARA O APP
# ==============================================================================

def get_indices_disponiveis() -> dict:
    status = {}
    
    # Barra de progresso para dar feedback visual
    prog = st.sidebar.progress(0)
    total = len(CONFIG_FONTES)
    
    for i, nome in enumerate(CONFIG_FONTES.keys()):
        df = get_dados_indice(nome)
        
        disponivel = not df.empty
        msg = "Disponível" if disponivel else "Indisponível"
        
        # Pega a data mais recente para mostrar ao usuário
        ultima_data = "-"
        if disponivel:
            ultima_data = df['data'].max().strftime("%m/%Y")
            
        status[nome] = {
            "nome": nome,
            "disponivel": disponivel,
            "ultima_data": ultima_data
        }
        prog.progress((i + 1) / total)
        
    prog.empty()
    return status

def formatar_moeda(valor):
    if not valor:
        return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def limpar_cache():
    # Reinicializa forçadamente
    st.session_state['cache_indices_ram'] = {}
    st.success("Cache limpo!")

# ==============================================================================
# 5. CÁLCULO
# ==============================================================================

def calcular_correcao_individual(valor: float, data_original: date, data_referencia: date, indice: str) -> dict:
    # Validações básicas
    if data_original >= data_referencia:
        return {'sucesso': True, 'valor_corrigido': valor, 'fator_correcao': 1.0, 'variacao_percentual': 0.0, 'indices': [indice]}
    
    # Busca dados
    df = get_dados_indice(indice)
    
    if df.empty:
        return {'sucesso': False, 'mensagem': f'Índice {indice} sem dados', 'valor_corrigido': valor}
    
    # === LÓGICA DE CORREÇÃO ===
    # Filtra os índices que estão no intervalo:
    # Data >= (Mês/Ano original) E Data < (Mês/Ano referência)
    # Ex: Vencimento 20/01, Pagamento 20/04.
    # Índices aplicados: Jan, Fev, Mar. (Abril não entra pois não fechou ou não é pro-rata, regra padrão TJ)
    
    # Normalizar datas para o dia 1 para comparação correta
    dt_inicio = date(data_original.year, data_original.month, 1)
    dt_fim = date(data_referencia.year, data_referencia.month, 1)
    
    # O PULO DO GATO: Garantir que a coluna 'data' do DF é date, não datetime
    # (Isso já foi garantido no _padronizar_dataframe, mas a comparação segura é vital)
    mask = (df['data'] >= dt_inicio) & (df['data'] < dt_fim)
    subset = df.loc[mask]
    
    if subset.empty:
        # Se for no mesmo mês, correção é zero
        if dt_inicio == dt_fim:
            return {'sucesso': True, 'valor_corrigido': valor, 'fator_correcao': 1.0, 'variacao_percentual': 0.0, 'indices': [indice]}
        
        return {'sucesso': False, 'mensagem': 'Período sem cobertura de índices', 'valor_corrigido': valor}
    
    # Cálculo Produtória: (1+i) * (1+i)...
    fator = (1 + subset['valor']).prod()
    
    # Trava de Sanidade
    if fator > 100:
        return {'sucesso': False, 'mensagem': 'Erro: Taxa explosiva detectada', 'valor_corrigido': valor}
    
    valor_corrigido = valor * fator
    
    return {
        'sucesso': True,
        'valor_corrigido': valor_corrigido,
        'fator_correcao': fator,
        'variacao_percentual': (fator - 1) * 100,
        'indices': [indice],
        'detalhes': f"{len(subset)} meses"
    }

def calcular_correcao_media(valor: float, data_original: date, data_referencia: date, indices: list) -> dict:
    if not indices:
        return {'sucesso': False, 'mensagem': 'Selecione índices'}
    
    fatores = []
    indices_ok = []
    
    for ind in indices:
        res = calcular_correcao_individual(valor, data_original, data_referencia, ind)
        if res['sucesso']:
            fatores.append(res['fator_correcao'])
            indices_ok.append(ind)
        else:
            return {'sucesso': False, 'mensagem': f'Falha no índice {ind}: {res.get("mensagem")}', 'valor_corrigido': valor}
            
    if not fatores:
        return {'sucesso': False, 'mensagem': 'Erro geral', 'valor_corrigido': valor}
        
    # Média Geométrica dos Fatores
    import math
    fator_medio = math.prod(fatores) ** (1 / len(fatores))
    
    return {
        'sucesso': True,
        'valor_corrigido': valor * fator_medio,
        'fator_correcao': fator_medio,
        'variacao_percentual': (fator_medio - 1) * 100,
        'indices': indices_ok
    }
