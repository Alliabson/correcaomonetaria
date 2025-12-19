import requests
from datetime import date, datetime
from typing import Dict, List, Optional
import streamlit as st

# =========================================================
# CONFIGURAÇÕES
# =========================================================

HEADERS = {
    "User-Agent": "python-requests",
    "Accept": "application/json"
}

REQUEST_TIMEOUT = 10

# Códigos SGS do Banco Central
SGS_CODES = {
    "IPCA": 433,      # IPCA - IBGE
    "IGPM": 189,      # IGP-M - FGV
    "INPC": 188,      # INPC - IBGE
    "INCC": 192,      # INCC-DI - FGV (O mais comum em contratos)
    "SELIC": 4390     # Selic acumulada mensal
}

# =========================================================
# COLETA E CÁLCULO
# =========================================================

@st.cache_data(ttl=86400)
def _obter_serie_bcb(codigo: int) -> List[Dict]:
    """Baixa o histórico completo do índice."""
    try:
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json"
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Erro ao baixar dados do código {codigo}: {e}")
        return []

def _calcular_fator_acumulado(indice: str, data_inicio: date, data_fim: date) -> float:
    """
    Calcula o acumulado considerando MÊS CHEIO.
    Se data_inicio é 15/07/2023, considera o índice de Julho/2023 em diante.
    Se data_fim é 20/09/2024, considera até o índice de Setembro/2024 (se divulgado).
    """
    codigo = SGS_CODES.get(indice)
    if not codigo:
        return 1.0

    dados = _obter_serie_bcb(codigo)
    if not dados:
        return 1.0

    fator_acumulado = 1.0
    
    # Transformamos as datas limite em inteiros (Ex: 202307 para Julho 2023)
    # Isso evita erros de comparação de dia (ex: dia 1 vs dia 20)
    inicio_key = data_inicio.year * 100 + data_inicio.month
    fim_key = data_fim.year * 100 + data_fim.month

    for item in dados:
        try:
            # Data do BCB vem sempre dia 01 (Ex: 01/07/2023)
            data_item = datetime.strptime(item["data"], "%d/%m/%Y").date()
            val_raw = item["valor"]
            
            if val_raw == "" or val_raw is None:
                continue
            
            # Chave do mês do índice
            item_key = data_item.year * 100 + data_item.month

            # LÓGICA DE OURO: Intervalo Inclusivo
            if inicio_key <= item_key <= fim_key:
                valor_taxa = float(val_raw.replace(",", "."))
                fator_mes = 1 + (valor_taxa / 100.0)
                fator_acumulado *= fator_mes
                
        except ValueError:
            continue

    return fator_acumulado

# =========================================================
# API PÚBLICA
# =========================================================

def get_indices_disponiveis() -> Dict[str, Dict]:
    indices = {}
    for nome in SGS_CODES.keys():
        indices[nome] = {"nome": nome, "disponivel": True}
    return indices

def calcular_correcao_individual(valor: float, data_inicio: date, data_fim: date, indice: str) -> Dict:
    try:
        fator = _calcular_fator_acumulado(indice, data_inicio, data_fim)
        valor_corrigido = valor * fator
        variacao = (fator - 1) * 100
        return {
            "sucesso": True, "valor_corrigido": valor_corrigido, 
            "fator_correcao": fator, "variacao_percentual": variacao, "indices": [indice]
        }
    except Exception as e:
        return {"sucesso": False, "mensagem": str(e), "valor_corrigido": valor, "fator_correcao": 1.0, "variacao_percentual": 0.0}

def calcular_correcao_media(valor: float, data_inicio: date, data_fim: date, indices: List[str]) -> Dict:
    """
    Média Aritmética dos Índices (Cesta de Moedas).
    Calcula o fator acumulado de cada índice separadamente e tira a média.
    """
    fatores_acumulados = []
    indices_validos = []

    for ind in indices:
        f = _calcular_fator_acumulado(ind, data_inicio, data_fim)
        fatores_acumulados.append(f)
        indices_validos.append(ind)

    if not fatores_acumulados:
        return {
            "sucesso": False, "mensagem": "Nenhum índice válido.",
            "valor_corrigido": valor, "fator_correcao": 1.0, "variacao_percentual": 0.0
        }

    # MÉDIA ARITMÉTICA DOS FATORES
    # Ex: IPCA (1.0449) + INPC (1.0406) + INCC (1.0467) = 3.1322 / 3 = 1.04407
    fator_medio = sum(fatores_acumulados) / len(fatores_acumulados)
    
    valor_corrigido = valor * fator_medio
    variacao = (fator_medio - 1) * 100

    return {
        "sucesso": True,
        "valor_corrigido": valor_corrigido,
        "fator_correcao": fator_medio,
        "variacao_percentual": variacao,
        "indices": indices_validos
    }

def formatar_moeda(valor: float) -> str:
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
