import requests
import pandas as pd
from datetime import datetime, date
import streamlit as st
import time
import json
import sqlite3
import urllib3
import concurrent.futures

# === CRÍTICO: Desabilita avisos de SSL para o BCB ===
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configurações de API e Cache (Mantido sua estrutura robusta)
API_CONFIG = {
    "BCB_PRIMARIO": {"base_url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados", "timeout": 15, "prioridade": 1},
    "IBGE_NOVO": {"base_url": "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos/-{}%20meses/variaveis/63?localidades=N1[all]", "timeout": 15, "prioridade": 2}
}

CODIGOS_INDICES = {
    "IPCA": {"fontes": [{"api": "BCB_PRIMARIO", "codigo": "433"}, {"api": "IBGE_NOVO", "codigo": "1737"}]},
    "IGPM": {"fontes": [{"api": "BCB_PRIMARIO", "codigo": "189"}]},
    "INPC": {"fontes": [{"api": "BCB_PRIMARIO", "codigo": "188"}, {"api": "IBGE_NOVO", "codigo": "1736"}]},
    "INCC": {"fontes": [{"api": "BCB_PRIMARIO", "codigo": "192"}]}
}

# Cache Simplificado
class CacheManager:
    def __init__(self, db_path="indices_cache.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute('CREATE TABLE IF NOT EXISTS cache_indices (chave TEXT PRIMARY KEY, dados TEXT, expiracao REAL)')
            conn.commit()
            conn.close()
        except: pass

    def get(self, chave):
        try:
            conn = sqlite3.connect(self.db_path)
            res = conn.execute("SELECT dados, expiracao FROM cache_indices WHERE chave = ?", (chave,)).fetchone()
            conn.close()
            if res and time.time() < res[1]: return json.loads(res[0])
        except: return None
        return None

    def set(self, chave, dados, horas=24):
        try:
            conn = sqlite3.connect(self.db_path)
            exp = time.time() + (horas * 3600)
            conn.execute('INSERT OR REPLACE INTO cache_indices VALUES (?, ?, ?)', (chave, json.dumps(dados), exp))
            conn.commit()
            conn.close()
        except: pass

cache = CacheManager()

def fazer_requisicao(url, params=None, timeout=15):
    try:
        headers = {'User-Agent': 'Mozilla/5.0', 'Connection': 'keep-alive'}
        # verify=False é essencial para o governo
        response = requests.get(url, params=params, headers=headers, timeout=timeout, verify=False)
        if response.status_code == 200: return response.json()
    except: pass
    return None

def buscar_dados_indice(indice, data_inicio, data_final):
    # Verifica Cache
    chave = f"{indice}_{data_inicio}_{data_final}"
    cached = cache.get(chave)
    if cached: return pd.DataFrame(cached)

    config = CODIGOS_INDICES.get(indice)
    if not config: return pd.DataFrame()

    df_final = pd.DataFrame()

    # Tenta as fontes configuradas
    for fonte in config['fontes']:
        try:
            api = API_CONFIG.get(fonte['api'])
            if fonte['api'] == "BCB_PRIMARIO":
                url = api['base_url'].format(fonte['codigo'])
                # Tenta pegar tudo (mais rápido que filtrar na API do BCB as vezes)
                dados = fazer_requisicao(url, {"formato": "json"})
                if dados:
                    df = pd.DataFrame(dados)
                    df['data'] = pd.to_datetime(df['data'], dayfirst=True).dt.date
                    df['valor'] = pd.to_numeric(df['valor']) / 100
                    df_final = df[(df['data'] >= data_inicio) & (df['data'] <= data_final)]
                    break
            
            elif fonte['api'] == "IBGE_NOVO":
                # Lógica IBGE simplificada
                url = api['base_url'].format(fonte['codigo'], "120")
                dados = fazer_requisicao(url)
                if dados:
                    res = []
                    for item in dados[0]['resultados'][0]['series'][0]['serie'].items():
                        res.append({'data': datetime.strptime(item[0], "%Y%m").date(), 'valor': float(item[1]['V'])/100})
                    df = pd.DataFrame(res)
                    df_final = df[(df['data'] >= data_inicio) & (df['data'] <= data_final)]
                    break
        except: continue

    if not df_final.empty:
        df_final = df_final.sort_values('data')
        # Salva no cache
        cache.set(chave, df_final.to_dict('records'))
        return df_final
    
    return pd.DataFrame()

# === FUNÇÃO QUE OTIMIZA O APP ===
def get_indices_disponiveis():
    """Retorna apenas a lista estática para não travar a UI"""
    return {k: {'nome': k, 'disponivel': True} for k in CODIGOS_INDICES.keys()}

def calcular_correcao_individual(valor, data_venc, data_ref, indice):
    if data_venc >= data_ref:
        return {'sucesso': True, 'valor_corrigido': valor, 'fator': 1.0, 'msg': 'Data futura'}
    
    df = buscar_dados_indice(indice, data_venc, data_ref)
    if df.empty:
        return {'sucesso': False, 'valor_corrigido': valor, 'fator': 1.0, 'msg': 'Sem índice'}
    
    fator = (1 + df['valor']).prod()
    return {'sucesso': True, 'valor_corrigido': valor * fator, 'fator': fator, 'msg': 'OK'}

def calcular_correcao_media(valor, data_venc, data_ref, indices):
    fatores = []
    for idx in indices:
        res = calcular_correcao_individual(valor, data_venc, data_ref, idx)
        if res['sucesso']: fatores.append(res['fator'])
    
    if not fatores: return {'sucesso': False, 'valor_corrigido': valor, 'fator': 1.0}
    
    media = sum(fatores) / len(fatores)
    return {'sucesso': True, 'valor_corrigido': valor * media, 'fator': media}

def formatar_moeda(valor):
    if not valor: return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def limpar_cache():
    try:
        import os
        if os.path.exists("indices_cache.db"): os.remove("indices_cache.db")
    except: pass
