import requests
import sqlite3
import os
import time
from datetime import date, datetime
from typing import Dict, List
import streamlit as st

# =========================================================
# CONFIGURAÇÕES GERAIS
# =========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "indices_cache.sqlite")

HEADERS = {
    "User-Agent": "python-requests",
    "Accept": "application/json"
}

REQUEST_TIMEOUT = 10

# Códigos SGS do Banco Central
SGS_CODES = {
    "IPCA": 433,
    "IGPM": 189,
    "INPC": 188,
    "INCC": 192,
    "SELIC": 11
}

# =========================================================
# BANCO DE DADOS (CACHE LOCAL)
# =========================================================

def _init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS indices (
            indice TEXT,
            data TEXT,
            valor REAL,
            fonte TEXT,
            PRIMARY KEY (indice, data)
        )
    """)
    conn.commit()
    conn.close()

_init_db()

def limpar_cache():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM indices")
    conn.commit()
    conn.close()

def _salvar_cache(indice: str, data_ref: date, valor: float, fonte: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO indices (indice, data, valor, fonte)
        VALUES (?, ?, ?, ?)
    """, (indice, data_ref.isoformat(), valor, fonte))
    conn.commit()
    conn.close()

def _buscar_cache(indice: str, data_ref: date):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT valor FROM indices
        WHERE indice = ? AND data = ?
    """, (indice, data_ref.isoformat()))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None

# =========================================================
# COLETORES DE DADOS
# =========================================================

def _buscar_bcb(indice: str, data_ref: date) -> float | None:
    try:
        codigo = SGS_CODES[indice]
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json"
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        dados = r.json()

        # último valor <= data_ref
        for item in reversed(dados):
            data_item = datetime.strptime(item["data"], "%d/%m/%Y").date()
            if data_item <= data_ref:
                return float(item["valor"].replace(",", "."))
        return None
    except Exception:
        return None

def _buscar_ibge(indice: str, data_ref: date) -> float | None:
    try:
        tabela_map = {
            "IPCA": "1737",
            "INPC": "1736",
            "IGPM": "1705"
        }
        if indice not in tabela_map:
            return None

        url = f"https://apisidra.ibge.gov.br/values/t/{tabela_map[indice]}/n1/all/v/63/p/all?formato=json"
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        dados = r.json()[1:]

        ano_mes_ref = int(data_ref.strftime("%Y%m"))

        for item in reversed(dados):
            ano_mes = int(item["D3C"])
            if ano_mes <= ano_mes_ref:
                return float(item["V"].replace(",", "."))
        return None
    except Exception:
        return None

def _buscar_ipeadata(indice: str, data_ref: date) -> float | None:
    try:
        serie_map = {
            "IPCA": "PRECOS_IPCA",
            "INPC": "PRECOS_INPC",
            "IGPM": "PRECOS_IGPM",
            "INCC": "PRECOS_INCC",
            "SELIC": "TAXA_SELIC"
        }
        if indice not in serie_map:
            return None

        url = f"http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERIE='{serie_map[indice]}')"
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        dados = r.json()["value"]

        for item in reversed(dados):
            data_item = datetime.fromisoformat(item["VALDATA"]).date()
            if data_item <= data_ref:
                return float(item["VALVALOR"])
        return None
    except Exception:
        return None

# =========================================================
# API PRINCIPAL
# =========================================================

@st.cache_data(show_spinner=False)
def get_indices_disponiveis() -> Dict[str, Dict]:
    indices = {}
    hoje = date.today()

    for indice in SGS_CODES.keys():
        valor = _buscar_cache(indice, hoje)
        if valor is not None:
            indices[indice] = {"nome": indice, "disponivel": True}
            continue

        valor = _buscar_bcb(indice, hoje)
        if valor is not None:
            _salvar_cache(indice, hoje, valor, "BCB")
            indices[indice] = {"nome": indice, "disponivel": True}
            continue

        valor = _buscar_ibge(indice, hoje)
        if valor is not None:
            _salvar_cache(indice, hoje, valor, "IBGE")
            indices[indice] = {"nome": indice, "disponivel": True}
            continue

        valor = _buscar_ipeadata(indice, hoje)
        if valor is not None:
            _salvar_cache(indice, hoje, valor, "IPEADATA")
            indices[indice] = {"nome": indice, "disponivel": True}
            continue

        indices[indice] = {"nome": indice, "disponivel": False}

    return indices

def _fator_correcao(indice: str, data_inicio: date, data_fim: date) -> float:
    if data_inicio >= data_fim:
        return 1.0

    valor_inicio = (
        _buscar_cache(indice, data_inicio)
        or _buscar_bcb(indice, data_inicio)
        or _buscar_ibge(indice, data_inicio)
        or _buscar_ipeadata(indice, data_inicio)
    )

    valor_fim = (
        _buscar_cache(indice, data_fim)
        or _buscar_bcb(indice, data_fim)
        or _buscar_ibge(indice, data_fim)
        or _buscar_ipeadata(indice, data_fim)
    )

    if valor_inicio is None or valor_fim is None:
        raise ValueError(f"Não foi possível obter dados do índice {indice}")

    _salvar_cache(indice, data_inicio, valor_inicio, "AUTO")
    _salvar_cache(indice, data_fim, valor_fim, "AUTO")

    return valor_fim / valor_inicio

def calcular_correcao_individual(
    valor: float,
    data_inicio: date,
    data_fim: date,
    indice: str
) -> Dict:
    try:
        fator = _fator_correcao(indice, data_inicio, data_fim)
        valor_corrigido = valor * fator
        variacao = (fator - 1) * 100

        return {
            "sucesso": True,
            "valor_corrigido": valor_corrigido,
            "fator_correcao": fator,
            "variacao_percentual": variacao,
            "indices": [indice]
        }
    except Exception as e:
        return {
            "sucesso": False,
            "mensagem": str(e),
            "valor_corrigido": valor,
            "fator_correcao": 1.0,
            "variacao_percentual": 0.0
        }

def calcular_correcao_media(
    valor: float,
    data_inicio: date,
    data_fim: date,
    indices: List[str]
) -> Dict:
    fatores = []

    for indice in indices:
        try:
            fatores.append(_fator_correcao(indice, data_inicio, data_fim))
        except Exception:
            pass

    if not fatores:
        return {
            "sucesso": False,
            "mensagem": "Nenhum índice disponível para cálculo",
            "valor_corrigido": valor,
            "fator_correcao": 1.0,
            "variacao_percentual": 0.0
        }

    fator_medio = sum(fatores) / len(fatores)
    valor_corrigido = valor * fator_medio
    variacao = (fator_medio - 1) * 100

    return {
        "sucesso": True,
        "valor_corrigido": valor_corrigido,
        "fator_correcao": fator_medio,
        "variacao_percentual": variacao,
        "indices": indices
    }

def formatar_moeda(valor: float) -> str:
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
