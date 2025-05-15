import streamlit as st
import pandas as pd
import numpy as np
import pdfplumber
import requests
from datetime import datetime
import matplotlib.pyplot as plt

# Configuração inicial da página
st.set_page_config(page_title="Correção Monetária", page_icon="📈", layout="wide")
st.title("📈 Sistema de Correção Monetária por Índices Econômicos")

def obter_indice(codigo, data):
    """
    Obtém o valor acumulado de um índice econômico até uma data específica
    usando a API do Banco Central do Brasil.
    
    Códigos dos principais índices:
    - IPCA: 433
    - IGPM: 189
    - INPC: 188
    - INCC: 192
    """
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json&dataInicial=01/01/1900&dataFinal={data.strftime('%d/%m/%Y')}"
    
    try:
        response = requests.get(url, timeout=10)
        dados = response.json()
        if dados:
            # Pegamos o último valor disponível antes ou na data especificada
            ultimo_valor = dados[-1]['valor']
            return float(ultimo_valor)
        else:
            return None
    except Exception as e:
        st.error(f"Erro ao obter índice {codigo}: {str(e)}")
        return None

# Dicionário com os índices disponíveis
INDICES = {
    "IPCA": {"codigo": 433, "nome": "IPCA"},
    "IGPM": {"codigo": 189, "nome": "IGP-M (FGV)"},
    "INPC": {"codigo": 188, "nome": "INPC (IBGE)"},
    "INCC": {"codigo": 192, "nome": "INCC (FGV)"}
}

def extrair_dados_pdf(arquivo_pdf):
    """
    Extrai valores e datas de um arquivo PDF.
    Esta função é um modelo básico - você precisará adaptar conforme a estrutura do seu relatório.
    """
    dados = []
    
    with pdfplumber.open(arquivo_pdf) as pdf:
        for pagina in pdf.pages:
            texto = pagina.extract_text()
            
            # Exemplo de extração - você precisará ajustar conforme seu PDF específico
            linhas = texto.split('\n')
            for linha in linhas:
                if "R$" in linha and "/" in linha:  # Exemplo de padrão para identificar valores e datas
                    partes = linha.split()
                    try:
                        valor = float(partes[partes.index("R$")+1].replace('.','').replace(',','.'))
                        # Tenta encontrar uma data no formato dd/mm/aaaa
                        data_str = next((p for p in partes if '/' in p and len(p.split('/')) == 3), None)
                        if data_str:
                            data = datetime.strptime(data_str, "%d/%m/%Y").date()
                            dados.append({"Data Original": data, "Valor Original": valor})
                    except:
                        continue
    
    return pd.DataFrame(dados)

def processar_upload_pdf():
    """
    Interface para upload do PDF e processamento inicial
    """
    uploaded_file = st.file_uploader("Carregue seu relatório em PDF", type="pdf")
    
    if uploaded_file is not None:
        try:
            df = extrair_dados_pdf(uploaded_file)
            if not df.empty:
                st.success("Dados extraídos com sucesso!")
                st.dataframe(df.head())
                return df
            else:
                st.warning("Não foi possível extrair dados do PDF. Verifique o formato.")
        except Exception as e:
            st.error(f"Erro ao processar PDF: {str(e)}")
    
    return None

def selecionar_indices():
    """
    Cria a interface para seleção dos índices de correção
    """
    st.subheader("Selecione o Método de Correção")
    
    opcao = st.radio("Método:", 
                    ["Índice Único", "Média de Múltiplos Índices"])
    
    if opcao == "Índice Único":
        indice = st.selectbox("Selecione o índice:", list(INDICES.keys()))
        return [indice]
    else:
        st.write("Selecione 3 ou mais índices para calcular a média:")
        col1, col2, col3, col4 = st.columns(4)
        selecionados = []
        
        with col1:
            if st.checkbox("IPCA"):
                selecionados.append("IPCA")
        with col2:
            if st.checkbox("IGPM"):
                selecionados.append("IGPM")
        with col3:
            if st.checkbox("INPC"):
                selecionados.append("INPC")
        with col4:
            if st.checkbox("INCC"):
                selecionados.append("INCC")
        
        if len(selecionados) < 3:
            st.warning("Selecione pelo menos 3 índices para calcular a média.")
            return None
        
        return selecionados

def calcular_correcao(valor_original, data_original, indices, data_atual):
    """
    Calcula o valor corrigido usando um ou mais índices
    """
    try:
        if not indices:
            return valor_original
        
        # Obtém os valores acumulados dos índices nas datas original e atual
        valores_originais = []
        valores_atuais = []
        
        for indice in indices:
            codigo = INDICES[indice]["codigo"]
            
            # Valor na data original
            valor_original_indice = obter_indice(codigo, data_original)
            if valor_original_indice is None:
                st.error(f"Não foi possível obter o índice {indice} para a data {data_original}")
                return None
            valores_originais.append(valor_original_indice)
            
            # Valor na data atual
            valor_atual_indice = obter_indice(codigo, data_atual)
            if valor_atual_indice is None:
                st.error(f"Não foi possível obter o índice {indice} para a data {data_atual}")
                return None
            valores_atuais.append(valor_atual_indice)
        
        # Calcula a variação
        if len(indices) == 1:
            # Correção por um único índice
            variacao = valores_atuais[0] / valores_originais[0]
        else:
            # Correção pela média dos índices
            variacoes = [atuais / originais for atuais, originais in zip(valores_atuais, valores_originais)]
            variacao = sum(variacoes) / len(variacoes)
        
        valor_corrigido = valor_original * variacao
        return round(valor_corrigido, 2)
    
    except Exception as e:
        st.error(f"Erro ao calcular correção: {str(e)}")
        return None

def main():
    # Processa o upload do PDF
    df = processar_upload_pdf()
    
    if df is not None:
        # Seleciona os índices
        indices = selecionar_indices()
        
        if indices:
            data_atual = st.date_input("Data para correção:", datetime.now().date())
            
            if st.button("Calcular Correção Monetária"):
                # Calcula os valores corrigidos
                resultados = []
                for _, row in df.iterrows():
                    valor_corrigido = calcular_correcao(
                        row["Valor Original"],
                        row["Data Original"],
                        indices,
                        data_atual
                    )
                    
                    if valor_corrigido is not None:
                        resultados.append({
                            "Data Original": row["Data Original"],
                            "Valor Original": row["Valor Original"],
                            "Valor Corrigido": valor_corrigido,
                            "Variação": f"{((valor_corrigido / row['Valor Original']) - 1) * 100:.2f}%",
                            "Índices Utilizados": ", ".join(indices)
                        })
                
                if resultados:
                    df_resultados = pd.DataFrame(resultados)
                    
                    # Exibe os resultados
                    st.subheader("Resultados da Correção Monetária")
                    st.dataframe(df_resultados)
                    
                    # Gráfico de comparação
                    st.subheader("Comparação: Valores Originais vs. Corrigidos")
                    
                    fig, ax = plt.subplots(figsize=(10, 6))
                    ax.bar(df_resultados["Data Original"].astype(str), 
                          df_resultados["Valor Original"], 
                          label="Original", alpha=0.6)
                    ax.bar(df_resultados["Data Original"].astype(str), 
                          df_resultados["Valor Corrigido"], 
                          label="Corrigido", alpha=0.6)
                    ax.set_ylabel("Valor (R$)")
                    ax.set_xlabel("Data Original")
                    ax.legend()
                    st.pyplot(fig)
                    
                    # Opção para exportar resultados
                    st.download_button(
                        label="Baixar Resultados em CSV",
                        data=df_resultados.to_csv(index=False).encode('utf-8'),
                        file_name="resultados_correcao_monetaria.csv",
                        mime="text/csv"
                    )
                else:
                    st.warning("Não foi possível calcular a correção para os dados extraídos.")

if __name__ == "__main__":
    main()
