import requests
import sqlite3
import pandas as pd
import streamlit as st
from datetime import date
from dateutil.relativedelta import relativedelta
from typing import Dict, List

DB_PATH = "indices_cache.db"

INDICES_CONFIG = {
    "IPCA": {"nome": "IPCA", "serie_bcb": 433},
    "INPC": {"nome": "INPC", "serie_bcb": 188},
    "INCC": {"nome": "INCC", "serie_bcb": 192},
}

MAX_MESES_CORRECAO = 60
FATOR_ALERTA = 2.5
FATOR_BLOQUEIO = 3.0


# ==========================
# BANCO
# ==========================
def _conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def _init_db():
    with _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS indices (
                indice TEXT,
                data TEXT,
                valor REAL,
                PRIMARY KEY (indice, data)
            )
        """)


_init_db()


def limpar_cache():
    with _conn() as c:
        c.execute("DELETE FROM indices")


def salvar(indice, data, valor):
    with _conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO indices VALUES (?, ?, ?)",
            (indice, data, valor)
        )


def buscar(indice, data):
    cur = _conn().cursor()
    cur.execute(
        "SELECT valor FROM indices WHERE indice = ? AND data = ?",
        (indice, data)
    )
    r = cur.fetchone()
    return r[0] if r else None


# ==========================
# AUXILIARES
# ==========================
def _ultimo_mes_fechado(data_ref: date) -> date:
    return (data_ref.replace(day=1) - relativedelta(months=1))


def formatar_moeda(v):
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# ==========================
# API
# ==========================
def _buscar_bcb(serie, inicio, fim):
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados"
        f"?formato=json"
        f"&dataInicial={inicio.strftime('%d/%m/%Y')}"
        f"&dataFinal={fim.strftime('%d/%m/%Y')}"
    )
    r = requests.get(url, timeout=15)
    r.raise_for_status()

    dados = {}
    for i in r.json():
        data = pd.to_datetime(i["data"], dayfirst=True).strftime("%Y-%m-01")
        dados[data] = float(i["valor"].replace(",", "."))
    return dados


# ==========================
# CORE
# ==========================
def _indices_periodo(indice, inicio, fim):
    meses = pd.date_range(inicio, fim, freq="MS")
    valores = {}

    for d in meses:
        k = d.strftime("%Y-%m-01")
        v = buscar(indice, k)
        if v is not None:
            valores[k] = v

    faltantes = [d for d in meses if d.strftime("%Y-%m-01") not in valores]

    if faltantes:
        dados_api = _buscar_bcb(
            INDICES_CONFIG[indice]["serie_bcb"],
            faltantes[0].date(),
            faltantes[-1].date()
        )
        for k, v in dados_api.items():
            salvar(indice, k, v)
            valores[k] = v

    return valores


def calcular_correcao_individual(valor, data_ini, data_ref, indice):
    fim = _ultimo_mes_fechado(data_ref)
    inicio = data_ini.replace(day=1)

    meses = (fim.year - inicio.year) * 12 + (fim.month - inicio.month)

    if meses <= 0:
        return {"sucesso": False, "mensagem": "Período inválido"}

    if meses > MAX_MESES_CORRECAO:
        return {"sucesso": False, "mensagem": "Período excede limite técnico"}

    indices = _indices_periodo(indice, inicio, fim)

    fator = 1.0
    for v in indices.values():
        fator *= (1 + v / 100)

    if fator > FATOR_BLOQUEIO:
        return {"sucesso": False, "mensagem": "Correção excessiva bloqueada"}

    valor_corrigido = valor * fator
    variacao = (fator - 1) * 100

    return {
        "sucesso": True,
        "valor_corrigido": valor_corrigido,
        "fator_correcao": fator,
        "variacao_percentual": variacao,
        "indice": indice
    }


def calcular_correcao_media(valor, data_ini, data_ref, indices: List[str]):
    fim = _ultimo_mes_fechado(data_ref)
    inicio = data_ini.replace(day=1)

    fatores_mensais = []

    for ind in indices:
        dados = _indices_periodo(ind, inicio, fim)
        fatores_mensais.append(list(dados.values()))

    fator_final = 1.0
    for i in range(len(fatores_mensais[0])):
        media_mes = sum(f[i] for f in fatores_mensais) / len(fatores_mensais)
        fator_final *= (1 + media_mes / 100)

    if fator_final > FATOR_BLOQUEIO:
        return {"sucesso": False, "mensagem": "Correção excessiva bloqueada"}

    return {
        "sucesso": True,
        "valor_corrigido": valor * fator_final,
        "fator_correcao": fator_final,
        "variacao_percentual": (fator_final - 1) * 100,
        "indices": indices
    }
