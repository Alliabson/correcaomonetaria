import requests
import sqlite3
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from typing import List, Dict

DB_PATH = "indices_cache.db"

INDICES_CONFIG = {
    "IPCA": {"serie": 433, "fonte": "IBGE"},
    "INPC": {"serie": 188, "fonte": "IBGE"},
    "IGPM": {"serie": 189, "fonte": "FGV"},
    "INCC": {"serie": 192, "fonte": "FGV"},
}

# ======================
# BANCO LOCAL
# ======================
def _conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def _init_db():
    with _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS indices (
                indice TEXT,
                data TEXT,
                variacao REAL,
                PRIMARY KEY (indice, data)
            )
        """)


_init_db()


def salvar(indice: str, data: str, variacao: float):
    with _conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO indices VALUES (?, ?, ?)",
            (indice, data, variacao)
        )


def buscar(indice: str, data: str):
    cur = _conn().cursor()
    cur.execute(
        "SELECT variacao FROM indices WHERE indice = ? AND data = ?",
        (indice, data)
    )
    r = cur.fetchone()
    return r[0] if r else None


def limpar_cache():
    with _conn() as c:
        c.execute("DELETE FROM indices")


# ======================
# API BACEN
# ======================
def _buscar_bacen(serie: int, inicio: date, fim: date) -> Dict[str, float]:
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados"
        f"?formato=json"
        f"&dataInicial={inicio.strftime('%d/%m/%Y')}"
        f"&dataFinal={fim.strftime('%d/%m/%Y')}"
    )
    r = requests.get(url, timeout=15)
    r.raise_for_status()

    dados = {}
    for item in r.json():
        data_ref = pd.to_datetime(item["data"], dayfirst=True).strftime("%Y-%m-01")
        variacao = float(item["valor"].replace(",", "."))
        dados[data_ref] = variacao

    return dados


# ======================
# OBTÉM ÚLTIMOS 12 MESES
# ======================
def _ultimos_12_meses(data_ref: date):
    fim = data_ref.replace(day=1) - relativedelta(months=1)
    inicio = fim - relativedelta(months=11)
    return inicio, fim


def _variacoes_periodo(indice: str, inicio: date, fim: date) -> Dict[str, float]:
    meses = pd.date_range(inicio, fim, freq="MS")
    valores = {}

    for d in meses:
        k = d.strftime("%Y-%m-01")
        v = buscar(indice, k)
        if v is not None:
            valores[k] = v

    faltantes = [d for d in meses if d.strftime("%Y-%m-01") not in valores]

    if faltantes:
        api = _buscar_bacen(
            INDICES_CONFIG[indice]["serie"],
            faltantes[0].date(),
            faltantes[-1].date()
        )
        for k, v in api.items():
            salvar(indice, k, v)
            valores[k] = v

    return valores


# ======================
# CÁLCULO INDIVIDUAL
# ======================
def calcular_correcao_individual(
    valor: float,
    data_referencia: date,
    indice: str
) -> Dict:

    inicio, fim = _ultimos_12_meses(data_referencia)
    variacoes = _variacoes_periodo(indice, inicio, fim)

    fator = 1.0
    for v in variacoes.values():
        fator *= (1 + v / 100)

    return {
        "sucesso": True,
        "valor_corrigido": valor * fator,
        "fator_correcao": fator,
        "variacao_percentual": (fator - 1) * 100,
        "indice": indice
    }


# ======================
# CÁLCULO MÉDIA ARITMÉTICA MENSAL
# ======================
def calcular_correcao_media(
    valor: float,
    data_referencia: date,
    indices: List[str]
) -> Dict:

    inicio, fim = _ultimos_12_meses(data_referencia)

    series = []
    for ind in indices:
        series.append(
            _variacoes_periodo(ind, inicio, fim)
        )

    fator_final = 1.0

    for mes in series[0].keys():
        media_mes = sum(s[mes] for s in series) / len(series)
        fator_final *= (1 + media_mes / 100)

    return {
        "sucesso": True,
        "valor_corrigido": valor * fator_final,
        "fator_correcao": fator_final,
        "variacao_percentual": (fator_final - 1) * 100,
        "indices": indices
    }
