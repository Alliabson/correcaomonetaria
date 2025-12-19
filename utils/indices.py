import requests
import os
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
# 433 = IPCA, 189 = IGPM, 188 = INPC, 192 = INCC, 11 = SELIC (Diária), 4390 = SELIC (Mensal)
# Nota: Para correção monetária mensal, usamos taxas mensais.
SGS_CODES = {
    "IPCA": 433,
    "IGPM": 189,
    "INPC": 188,
    "INCC": 192,
    "SELIC": 4390 # Alterado para Selic Mensal Acumulada para compatibilidade com lógica de meses
}

# =========================================================
# COLETA E CÁLCULO DE DADOS
# =========================================================

@st.cache_data(ttl=86400) # Cache de 24 horas automático do Streamlit
def _obter_serie_bcb(codigo: int) -> List[Dict]:
    """
    Baixa o histórico completo do índice (formato JSON) e faz cache.
    """
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
    Calcula o fator acumulado multiplicando as taxas mensais no período.
    Fórmula: Fator = (1 + taxa_mes_1) * (1 + taxa_mes_2) * ...
    """
    codigo = SGS_CODES.get(indice)
    if not codigo:
        return 1.0

    dados = _obter_serie_bcb(codigo)
    if not dados:
        return 1.0

    fator_acumulado = 1.0
    
    # Normalização de datas para garantir comparação correta
    dt_inicio_norm = data_inicio
    dt_fim_norm = data_fim

    encontrou_dados = False

    for item in dados:
        try:
            data_item = datetime.strptime(item["data"], "%d/%m/%Y").date()
            val_raw = item["valor"]
            
            # O Banco Central às vezes retorna valores vazios ou hífens
            if val_raw == "" or val_raw is None:
                continue
                
            valor_taxa = float(val_raw.replace(",", "."))

            # LÓGICA DE CORREÇÃO:
            # A taxa de um mês (ex: 0.5% em Nov) deve ser aplicada se o período cobre o mês.
            # Regra padrão: A data do índice deve ser posterior à data de início e anterior ou igual à data fim.
            if dt_inicio_norm < data_item <= dt_fim_norm:
                # Transforma percentual em fator (ex: 0.5% vira 1.005)
                fator_mes = 1 + (valor_taxa / 100.0)
                fator_acumulado *= fator_mes
                encontrou_dados = True
                
        except ValueError:
            continue

    return fator_acumulado

# =========================================================
# API PÚBLICA (FUNÇÕES CHAMADAS PELO FRONTEND)
# =========================================================

def get_indices_disponiveis() -> Dict[str, Dict]:
    """
    Retorna a lista de índices configurados.
    """
    indices = {}
    for nome in SGS_CODES.keys():
        indices[nome] = {"nome": nome, "disponivel": True}
    return indices

def calcular_correcao_individual(
    valor: float,
    data_inicio: date,
    data_fim: date,
    indice: str
) -> Dict:
    try:
        fator = _calcular_fator_acumulado(indice, data_inicio, data_fim)
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
            "mensagem": f"Erro no cálculo: {str(e)}",
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
    """
    Calcula a correção pela MÉDIA ARITMÉTICA dos índices.
    Ex: Se IPCA acumulou 10% (fator 1.10) e IGPM acumulou 20% (fator 1.20),
    o fator médio será 1.15.
    """
    fatores = []
    indices_validos = []

    for ind in indices:
        f = _calcular_fator_acumulado(ind, data_inicio, data_fim)
        # Consideramos 1.0 como falha ou sem variação, mas vamos incluir no cálculo
        # Se for estritamente necessário ignorar erros, adicione logica aqui.
        fatores.append(f)
        indices_validos.append(ind)

    if not fatores:
        return {
            "sucesso": False,
            "mensagem": "Nenhum índice válido encontrado para o período.",
            "valor_corrigido": valor,
            "fator_correcao": 1.0,
            "variacao_percentual": 0.0
        }

    # Média dos fatores acumulados
    fator_medio = sum(fatores) / len(fatores)
    
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
