import pandas as pd
from datetime import datetime, date, timedelta
import streamlit as st
import sidrapy
import ipeadatapy as ipea
import requests
import time
from typing import Dict, List, Optional, Union, Any
import json
import os
import sqlite3
import logging
import math
import urllib3

# Desabilitar avisos de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("IndicesEconomicos")

# ==============================================================================
# CONFIGURAÇÃO DAS FONTES
# ==============================================================================
CODIGOS_REDUNDANTES = {
    "IPCA": {
        "nome": "IPCA (IBGE)",
        "fontes": [
            {"tipo": "ipea", "codigo": "PRECOS12_IPCA12"},
            {"tipo": "sidra", "tabela": "1737", "variavel": "63", "geral": "2265"},
            {"tipo": "bcb_direct", "codigo": "433"}
        ]
    },
    "INPC": {
        "nome": "INPC (IBGE)",
        "fontes": [
            {"tipo": "ipea", "codigo": "PRECOS12_INPC12"},
            {"tipo": "sidra", "tabela": "1736", "variavel": "44", "geral": "2289"},
            {"tipo": "bcb_direct", "codigo": "188"}
        ]
    },
    "IGPM": {
        "nome": "IGP-M (FGV)",
        "fontes": [
            {"tipo": "ipea", "codigo": "IGP12_IGPM12"},
            {"tipo": "bcb_direct", "codigo": "189"}
        ]
    },
    "INCC": {
        "nome": "INCC-DI (FGV)",
        "fontes": [
            {"tipo": "ipea", "codigo": "IGP12_INCC12"},
            {"tipo": "bcb_direct", "codigo": "192"}
        ]
    },
    "SELIC": {
        "nome": "Taxa Selic Mensal",
        "fontes": [
            {"tipo": "ipea", "codigo": "BM12_TJOVER12"}, 
            {"tipo": "bcb_direct", "codigo": "4390"} # 4390 é a série MENSAL acumulada, mais segura que a 11
        ]
    }
}

# ==============================================================================
# SANITIZADOR DE DADOS (O GUARDIÃO)
# ==============================================================================
def sanitizar_taxa_mensal(df: pd.DataFrame) -> pd.DataFrame:
    """
    Corrige distorções de escala (Percentual vs Decimal).
    Regra heurística para Brasil pós-1994:
    - Se valor > 0.50 (50% ao mês), com certeza é erro de escala (ex: 1.0 em vez de 0.01).
    - Divide por 100 para garantir.
    """
    if df.empty: return df
    
    # Remove valores nulos ou infinitos
    df = df.replace([float('inf'), -float('inf')], 0).dropna()
    
    # Lógica de correção de escala
    # Se a média da série for maior que 0.1 (10% a.m), provavelmente está em escala percentual (0-100)
    # e não em escala decimal (0-1). O Real nunca teve média de 10% a.m por longos períodos.
    media = df['valor'].abs().mean()
    
    if media > 0.1: # Gatilho de segurança
        logger.warning(f"⚠️ Detectada escala percentual (Média {media:.4f}). Aplicando correção /100.")
        df['valor'] = df['valor'] / 100
        
    # Trava de segurança secundária linha a linha (para outliers)
    # Se ainda tiver algum valor > 1.0 (100% a.m), divide por 100
    mask_erro = df['valor'].abs() > 1.0
    if mask_erro.any():
        df.loc[mask_erro, 'valor'] = df.loc[mask_erro, 'valor'] / 100
        
    return df

# ==============================================================================
# BANCO DE DADOS (V2 para forçar limpeza)
# ==============================================================================
class DatabaseHandler:
    def __init__(self, db_name="indices_v2.db"): # Mudança de nome para limpar cache antigo
        self.db_name = db_name
        self._init_db()

    def _get_connection(self):
        return sqlite3.connect(self.db_name, check_same_thread=False)

    def _init_db(self):
        try:
            with self._get_connection() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS historico_indices (
                        indice TEXT,
                        data_ref DATE,
                        valor REAL,
                        fonte_origem TEXT,
                        data_update TIMESTAMP,
                        PRIMARY KEY (indice, data_ref)
                    )
                """)
        except Exception:
            pass

    def salvar_dados(self, indice: str, df: pd.DataFrame, fonte: str):
        if df.empty: return
        try:
            # SANITIZAÇÃO ANTES DE SALVAR
            df = sanitizar_taxa_mensal(df)
            
            data_now = datetime.now()
            records = []
            df_save = df.copy()
            df_save['data'] = pd.to_datetime(df_save['data']).dt.strftime('%Y-%m-%d')
            
            for _, row in df_save.iterrows():
                records.append((indice, row['data'], float(row['valor']), fonte, data_now))
            
            with self._get_connection() as conn:
                conn.executemany("""
                    INSERT OR REPLACE INTO historico_indices 
                    (indice, data_ref, valor, fonte_origem, data_update)
                    VALUES (?, ?, ?, ?, ?)
                """, records)
        except Exception as e:
            logger.error(f"Erro DB Save: {e}")

    def recuperar_dados(self, indice: str) -> pd.DataFrame:
        try:
            with self._get_connection() as conn:
                df = pd.read_sql(
                    "SELECT data_ref as data, valor FROM historico_indices WHERE indice = ? ORDER BY data_ref",
                    conn,
                    params=(indice,)
                )
            if not df.empty:
                df['data'] = pd.to_datetime(df['data']).dt.date
                return df
            return pd.DataFrame()
        except Exception:
            return pd.DataFrame()
            
    def limpar_tudo(self):
        try:
            if os.path.exists(self.db_name):
                os.remove(self.db_name)
            self._init_db()
            return True
        except:
            return False

db = DatabaseHandler()

# ==============================================================================
# DRIVERS (COLETA)
# ==============================================================================
class DataDrivers:
    @staticmethod
    def get_ipea(codigo: str) -> pd.DataFrame:
        try:
            serie = ipea.timeseries(codigo)
            if serie.empty: return pd.DataFrame()
            
            df = serie.reset_index()
            cols = df.columns.tolist()
            col_valor = codigo if codigo in cols else cols[-1]
            col_data = 'DATE' if 'DATE' in cols else cols[0]

            new_df = pd.DataFrame()
            new_df['data'] = pd.to_datetime(df[col_data]).dt.date
            # IPEA geralmente vem em % (ex: 0.53 para 0.53%)
            new_df['valor'] = pd.to_numeric(df[col_valor], errors='coerce') / 100
            
            return new_df[new_df['data'] >= date(1994, 1, 1)].dropna().sort_values('data')
        except:
            return pd.DataFrame()

    @staticmethod
    def get_sidra(params: dict) -> pd.DataFrame:
        try:
            data = sidrapy.get_table(
                table_code=params['tabela'],
                territorial_level="1",
                ibge_territorial_code="all",
                variable=params['variavel'],
                period="last 240", 
                classifications={"315": params['geral']}
            )
            if data.empty or 'V' not in data.columns: return pd.DataFrame()

            df = data.iloc[1:].copy()
            new_df = pd.DataFrame()
            # Sidra vem em % (ex: 0.53 para 0.53%)
            new_df['valor'] = pd.to_numeric(df['V'], errors='coerce') / 100
            new_df['data'] = pd.to_datetime(df['D2C'], format="%Y%m", errors='coerce').dt.date
            return new_df.dropna().sort_values('data')
        except:
            return pd.DataFrame()

    @staticmethod
    def get_bcb_stealth(codigo: str) -> pd.DataFrame:
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json'
        }
        try:
            # verify=False é crucial para o BCB
            response = requests.get(url, headers=headers, timeout=10, verify=False)
            if response.status_code == 200:
                df = pd.DataFrame(response.json())
                new_df = pd.DataFrame()
                new_df['data'] = pd.to_datetime(df['data'], dayfirst=True).dt.date
                new_df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
                
                # BCB sempre manda em % (ex: 0.53). Dividir por 100.
                new_df['valor'] = new_df['valor'] / 100
                return new_df.dropna().sort_values('data')
            return pd.DataFrame()
        except:
            return pd.DataFrame()

# ==============================================================================
# ORQUESTRADOR
# ==============================================================================
def atualizar_indice_inteligente(nome_indice: str) -> pd.DataFrame:
    config = CODIGOS_REDUNDANTES.get(nome_indice)
    if not config: return pd.DataFrame()

    for fonte in config['fontes']:
        tipo = fonte['tipo']
        df_temp = pd.DataFrame()
        
        try:
            if tipo == "ipea": df_temp = DataDrivers.get_ipea(fonte['codigo'])
            elif tipo == "sidra": df_temp = DataDrivers.get_sidra(fonte)
            elif tipo == "bcb_direct": df_temp = DataDrivers.get_bcb_stealth(fonte['codigo'])
        except: continue
            
        if not df_temp.empty and len(df_temp) > 10:
            # SANITIZAÇÃO AQUI TAMBÉM
            df_temp = sanitizar_taxa_mensal(df_temp)
            db.salvar_dados(nome_indice, df_temp, tipo)
            return df_temp
            
    return db.recuperar_dados(nome_indice)

# ==============================================================================
# FUNÇÕES EXPOSTAS
# ==============================================================================
@st.cache_data(ttl=3600)
def get_indices_disponiveis() -> Dict[str, dict]:
    status_final = {}
    progress_bar = st.sidebar.progress(0)
    total = len(CODIGOS_REDUNDANTES)
    
    for idx, (key, val) in enumerate(CODIGOS_REDUNDANTES.items()):
        df = db.recuperar_dados(key)
        
        if df.empty or (date.today() - df['data'].max()).days > 45:
            df_novo = atualizar_indice_inteligente(key)
            if not df_novo.empty: df = df_novo

        status_final[key] = {
            'nome': val['nome'],
            'disponivel': not df.empty,
            'ultima_data': df['data'].max().strftime("%m/%Y") if not df.empty else "N/A"
        }
        progress_bar.progress((idx + 1) / total)

    progress_bar.empty()
    return status_final

def formatar_moeda(valor: float) -> str:
    if valor is None: return "R$ 0,00"
    if valor > 1_000_000_000_000: return "R$ Valor Irreal (Erro)" # Trava visual
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def limpar_cache():
    if db.limpar_tudo():
        st.success("Cache limpo com sucesso!")
        time.sleep(1)

def calcular_correcao_individual(valor: float, data_original: date, data_referencia: date, indice: str) -> dict:
    if data_original >= data_referencia:
        return {'sucesso': True, 'valor_corrigido': valor, 'fator_correcao': 1.0, 'variacao_percentual': 0.0, 'indices': [indice]}

    df = db.recuperar_dados(indice)
    if df.empty: df = atualizar_indice_inteligente(indice)
    
    if df.empty:
        return {'sucesso': False, 'mensagem': f'Índice {indice} sem dados.', 'valor_corrigido': valor}

    # FILTRO DE SEGURANÇA FINAL
    df = sanitizar_taxa_mensal(df)

    dt_inicio = date(data_original.year, data_original.month, 1)
    dt_fim = date(data_referencia.year, data_referencia.month, 1)
    
    mask = (df['data'] >= dt_inicio) & (df['data'] < dt_fim)
    subset = df.loc[mask]
    
    if subset.empty:
        if dt_inicio == dt_fim:
             return {'sucesso': True, 'valor_corrigido': valor, 'fator_correcao': 1.0, 'variacao_percentual': 0.0}
        return {'sucesso': False, 'mensagem': 'Período sem dados.', 'valor_corrigido': valor}
    
    fator = (1 + subset['valor']).prod()
    
    # Trava de sanidade para o FATOR total
    if fator > 1000: # Se o valor corrigir mais que 1000x, algo está muito errado para Brasil pós-Real
        return {'sucesso': False, 'mensagem': f'Erro de cálculo: Fator explosivo ({fator:.2f}). Verifique dados.', 'valor_corrigido': valor}
        
    valor_corrigido = valor * fator
    
    return {
        'sucesso': True,
        'valor_corrigido': valor_corrigido,
        'fator_correcao': fator,
        'variacao_percentual': (fator - 1) * 100,
        'indices': [indice]
    }

def calcular_correcao_media(valor: float, data_original: date, data_referencia: date, indices: List[str]) -> dict:
    if not indices: return {'sucesso': False, 'mensagem': 'Sem índices'}
    
    fatores = []
    sucessos = []
    
    for ind in indices:
        res = calcular_correcao_individual(valor, data_original, data_referencia, ind)
        if res['sucesso']:
            fatores.append(res['fator_correcao'])
            sucessos.append(ind)
    
    if not fatores:
        return {'sucesso': False, 'mensagem': 'Falha nos índices'}
    
    prod = math.prod(fatores)
    fator_medio = prod ** (1/len(fatores))
    
    return {
        'sucesso': True,
        'valor_corrigido': valor * fator_medio,
        'fator_correcao': fator_medio,
        'variacao_percentual': (fator_medio - 1) * 100,
        'indices': sucessos
    }
