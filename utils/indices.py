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

# Desabilitar avisos de SSL (necessário para o "Stealth Mode" do BCB)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuração de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s')
logger = logging.getLogger("IndicesEconomicos")

# ==============================================================================
# 1. MAPEAMENTO DE CÓDIGOS (MULTI-FONTE)
# ==============================================================================
# Cada índice possui códigos para 3 fontes diferentes.
CODIGOS_REDUNDANTES = {
    "IPCA": {
        "nome": "IPCA (IBGE)",
        "fontes": [
            # Fonte 1: IPEA (Mais estável para dados históricos)
            {"tipo": "ipea", "codigo": "PRECOS12_IPCA12"},
            # Fonte 2: IBGE (Fonte oficial primária)
            {"tipo": "sidra", "tabela": "1737", "variavel": "63", "geral": "2265"},
            # Fonte 3: BCB Direto (Fallback final)
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
            # IBGE não tem IGPM. Vamos direto pro BCB como fallback
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
            {"tipo": "bcb_direct", "codigo": "11"} 
        ]
    }
}

# ==============================================================================
# 2. SISTEMA DE BANCO DE DADOS (PERSISTÊNCIA ROBUSTA)
# ==============================================================================
class DatabaseHandler:
    def __init__(self, db_name="indices_blindados.db"):
        self.db_name = db_name
        self._init_db()

    def _get_connection(self):
        return sqlite3.connect(self.db_name, check_same_thread=False)

    def _init_db(self):
        """Cria tabela se não existir"""
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
        except Exception as e:
            logger.error(f"Erro DB Init: {e}")

    def salvar_dados(self, indice: str, df: pd.DataFrame, fonte: str):
        """Salva ou atualiza dados no SQLite"""
        if df.empty: return
        try:
            data_now = datetime.now()
            records = []
            
            # Normalização garantida
            df_save = df.copy()
            df_save['data'] = pd.to_datetime(df_save['data']).dt.strftime('%Y-%m-%d')
            
            for _, row in df_save.iterrows():
                records.append((
                    indice, 
                    row['data'], 
                    float(row['valor']), 
                    fonte, 
                    data_now
                ))
            
            with self._get_connection() as conn:
                conn.executemany("""
                    INSERT OR REPLACE INTO historico_indices 
                    (indice, data_ref, valor, fonte_origem, data_update)
                    VALUES (?, ?, ?, ?, ?)
                """, records)
            logger.info(f"Persistência OK: {len(records)} registros para {indice} via {fonte}")
        except Exception as e:
            logger.error(f"Erro ao salvar no DB: {e}")

    def recuperar_dados(self, indice: str) -> pd.DataFrame:
        """Lê do SQLite"""
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
        except Exception as e:
            logger.error(f"Erro leitura DB: {e}")
            return pd.DataFrame()

    def limpar_tudo(self):
        try:
            if os.path.exists(self.db_name):
                os.remove(self.db_name)
            self._init_db()
            return True
        except Exception:
            return False

db = DatabaseHandler()

# ==============================================================================
# 3. MOTORES DE COLETA (DRIVERS)
# ==============================================================================

class DataDrivers:
    """Coleção de métodos estáticos para buscar dados em diferentes lugares"""

    @staticmethod
    def get_ipea(codigo: str) -> pd.DataFrame:
        """Driver para IPEADATA"""
        try:
            serie = ipea.timeseries(codigo)
            if serie.empty: return pd.DataFrame()
            
            df = serie.reset_index()
            cols = df.columns.tolist()
            col_valor = codigo if codigo in cols else cols[-1]
            col_data = 'DATE' if 'DATE' in cols else cols[0]

            new_df = pd.DataFrame()
            new_df['data'] = pd.to_datetime(df[col_data]).dt.date
            # IPEA vem em percentual (ex: 0.53), dividir por 100
            new_df['valor'] = pd.to_numeric(df[col_valor], errors='coerce') / 100
            
            return new_df[new_df['data'] >= date(1994, 1, 1)].dropna().sort_values('data')
        except Exception as e:
            logger.warning(f"Falha IPEA ({codigo}): {e}")
            return pd.DataFrame()

    @staticmethod
    def get_sidra(params: dict) -> pd.DataFrame:
        """Driver para IBGE SIDRA"""
        try:
            # last 240 = últimos 20 anos
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
            new_df['valor'] = pd.to_numeric(df['V'], errors='coerce') / 100
            new_df['data'] = pd.to_datetime(df['D2C'], format="%Y%m", errors='coerce').dt.date
            
            return new_df.dropna().sort_values('data')
        except Exception as e:
            logger.warning(f"Falha SIDRA: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_bcb_stealth(codigo: str) -> pd.DataFrame:
        """
        Driver para Banco Central (Modo Stealth).
        Usa requests puro com headers falsos para evitar erro 403.
        """
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none'
        }

        try:
            response = requests.get(url, headers=headers, timeout=15, verify=False)
            
            if response.status_code == 200:
                dados = response.json()
                df = pd.DataFrame(dados)
                
                new_df = pd.DataFrame()
                new_df['data'] = pd.to_datetime(df['data'], dayfirst=True).dt.date
                new_df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
                
                # Normalização: se > 10, assume que não foi dividido por 100
                if new_df['valor'].max() > 10:
                    new_df['valor'] = new_df['valor'] / 100
                else:
                    # Assumindo padrão moderno que já vem em % (ex 0.53)
                    new_df['valor'] = new_df['valor'] / 100
                
                return new_df.dropna().sort_values('data')
            else:
                logger.warning(f"Falha BCB Status {response.status_code}")
                return pd.DataFrame()
        except Exception as e:
            logger.warning(f"Falha BCB Stealth: {e}")
            return pd.DataFrame()


# ==============================================================================
# 4. ORQUESTRADOR (O MAESTRO)
# ==============================================================================

def atualizar_indice_inteligente(nome_indice: str) -> pd.DataFrame:
    """
    Tenta TODAS as fontes disponíveis em ordem.
    """
    config = CODIGOS_REDUNDANTES.get(nome_indice)
    if not config: return pd.DataFrame()

    # Tentativa 1, 2, 3...
    for i, fonte in enumerate(config['fontes']):
        tipo = fonte['tipo']
        
        logger.info(f"Tentando atualizar {nome_indice} via {tipo} (Tentativa {i+1})...")
        
        if tipo == "ipea":
            df_temp = DataDrivers.get_ipea(fonte['codigo'])
        elif tipo == "sidra":
            df_temp = DataDrivers.get_sidra(fonte)
        elif tipo == "bcb_direct":
            df_temp = DataDrivers.get_bcb_stealth(fonte['codigo'])
            
        if not df_temp.empty and len(df_temp) > 10:
            db.salvar_dados(nome_indice, df_temp, tipo)
            logger.info(f"✅ SUCESSO: {nome_indice} atualizado via {tipo}")
            return df_temp
            
    logger.error(f"❌ TODAS AS FONTES FALHARAM PARA {nome_indice}. Tentando cache...")
    return db.recuperar_dados(nome_indice)

# ==============================================================================
# 5. FUNÇÕES EXPOSTAS AO APP (INTERFACE PÚBLICA)
# ==============================================================================

@st.cache_data(ttl=3600)
def get_indices_disponiveis() -> Dict[str, dict]:
    """Inicializa e verifica o status de todos os índices"""
    status_final = {}
    
    # UI Feedback discreto
    progress_bar = st.sidebar.progress(0)
    status_text = st.sidebar.empty()
    total = len(CODIGOS_REDUNDANTES)
    
    for idx, (key, val) in enumerate(CODIGOS_REDUNDANTES.items()):
        status_text.text(f"Verificando {key}...")
        
        df = db.recuperar_dados(key)
        
        # Se vazio ou muito antigo (> 45 dias), atualiza
        needs_update = False
        if df.empty:
            needs_update = True
        else:
            ultima_data = df['data'].max()
            if (date.today() - ultima_data).days > 45:
                needs_update = True
        
        if needs_update:
            df_novo = atualizar_indice_inteligente(key)
            if not df_novo.empty:
                df = df_novo

        # Monta objeto de status
        if not df.empty:
            status_final[key] = {
                'nome': val['nome'],
                'disponivel': True,
                'ultima_data': df['data'].max().strftime("%m/%Y"),
                'records': len(df)
            }
        else:
            status_final[key] = {
                'nome': val['nome'],
                'disponivel': False,
                'ultima_data': "N/A",
                'records': 0
            }
        
        progress_bar.progress((idx + 1) / total)

    status_text.empty()
    progress_bar.empty()
    return status_final

def formatar_moeda(valor: float) -> str:
    if valor is None: return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def limpar_cache():
    if db.limpar_tudo():
        st.success("Base de dados local resetada. Recarregando fontes...")
        time.sleep(1)
    else:
        st.error("Erro ao limpar banco de dados.")

def calcular_correcao_individual(valor: float, data_original: date, data_referencia: date, indice: str) -> dict:
    if data_original >= data_referencia:
        return {'sucesso': True, 'valor_corrigido': valor, 'fator_correcao': 1.0, 'variacao_percentual': 0.0, 'indices': [indice]}

    df = db.recuperar_dados(indice)
    if df.empty:
        df = atualizar_indice_inteligente(indice)

    if df.empty:
        return {'sucesso': False, 'mensagem': f'Índice {indice} indisponível.', 'valor_corrigido': valor}

    dt_inicio = date(data_original.year, data_original.month, 1)
    dt_fim = date(data_referencia.year, data_referencia.month, 1)
    
    # Filtra período (Mês inicial INCLUSO, Mês final EXCLUSO)
    mask = (df['data'] >= dt_inicio) & (df['data'] < dt_fim)
    subset = df.loc[mask]
    
    if subset.empty:
        if dt_inicio == dt_fim:
             return {'sucesso': True, 'valor_corrigido': valor, 'fator_correcao': 1.0, 'variacao_percentual': 0.0}
        return {'sucesso': False, 'mensagem': f'Sem dados para o período {dt_inicio} a {dt_fim}', 'valor_corrigido': valor}
    
    fator = (1 + subset['valor']).prod()
    valor_corrigido = valor * fator
    
    return {
        'sucesso': True,
        'valor_corrigido': valor_corrigido,
        'fator_correcao': fator,
        'variacao_percentual': (fator - 1) * 100,
        'indices': [indice],
        'debug_info': f"{len(subset)} meses"
    }

def calcular_correcao_media(valor: float, data_original: date, data_referencia: date, indices: List[str]) -> dict:
    if not indices: return {'sucesso': False, 'mensagem': 'Nenhum índice'}
    
    fatores = []
    sucessos = []
    
    for ind in indices:
        res = calcular_correcao_individual(valor, data_original, data_referencia, ind)
        if res['sucesso']:
            fatores.append(res['fator_correcao'])
            sucessos.append(ind)
    
    if not fatores:
        return {'sucesso': False, 'mensagem': 'Falha em todos os índices selecionados'}
    
    prod = math.prod(fatores)
    fator_medio = prod ** (1/len(fatores))
    
    return {
        'sucesso': True,
        'valor_corrigido': valor * fator_medio,
        'fator_correcao': fator_medio,
        'variacao_percentual': (fator_medio - 1) * 100,
        'indices': sucessos
    }
