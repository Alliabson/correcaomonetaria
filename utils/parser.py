import pandas as pd
import pdfplumber
import re
from datetime import datetime

def extract_payment_data(uploaded_file):
    if hasattr(uploaded_file, 'name'):
        if uploaded_file.name.endswith('.pdf'):
            return extract_from_pdf(uploaded_file)
        elif uploaded_file.name.endswith(('.xlsx', '.xls')):
            return pd.read_excel(uploaded_file)
    return pd.DataFrame()

def parse_monetary(value):
    if not value: return 0.0
    try:
        # Remove caracteres indesejados (aspas, R$, letras)
        clean = re.sub(r'[R$\s"\'a-zA-Z]', '', str(value))
        
        # Lógica BR: 1.000,00
        if ',' in clean and '.' in clean:
            return float(clean.replace('.', '').replace(',', '.'))
        # Lógica BR simples: 1000,00
        elif ',' in clean:
            return float(clean.replace(',', '.'))
        # Lógica Internacional/Sistema: 1000.00
        elif '.' in clean:
            return float(clean)
        return float(clean)
    except:
        return 0.0

def extract_from_pdf(pdf_file):
    parcelas = []
    
    # Regex Poderoso para formato CSV/Tabela do seu PDF
    # Explicação:
    # 1. ["']? -> Pode ter aspas
    # 2. ([A-Z]+\.?\d+/\d+) -> Código (E.1/3 ou P.1/24)
    # 3. .*? -> Lixo no meio
    # 4. (\d{2}/\d{2}/\d{4}) -> Data vencimento
    # 5. .*? -> Lixo no meio
    # 6. ([\d\.,]+) -> Valor
    regex_linha = r'["\']?([A-Z]+\.?\d+/\d+)["\']?.*?(\d{2}/\d{2}/\d{4}).*?([\d\.,]+)'
    
    try:
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text: continue
                
                lines = text.split('\n')
                for line in lines:
                    # Ignora linhas de totais ou cabeçalhos
                    if any(x in line.lower() for x in ["total", "geral", "resumo", "página"]):
                        continue

                    match = re.search(regex_linha, line)
                    if match:
                        codigo = match.group(1)
                        dt_venc_str = match.group(2)
                        
                        # Extração inteligente de valores
                        # O parser procura todas as sequências de dinheiro e datas na linha
                        todos_valores = re.findall(r'(\d{1,3}(?:[\.,]\d{3})*[\.,]\d{2})', line)
                        todas_datas = re.findall(r'(\d{2}/\d{2}/\d{4})', line)
                        
                        dt_venc = datetime.strptime(dt_venc_str, "%d/%m/%Y").date()
                        
                        # Assume o primeiro valor como original
                        valor_original = parse_monetary(todos_valores[0]) if todos_valores else 0.0
                        
                        dt_pag = None
                        valor_pago = 0.0
                        
                        # Se tiver uma segunda data, é o recebimento
                        if len(todas_datas) > 1:
                            try:
                                dt_pag = datetime.strptime(todas_datas[1], "%d/%m/%Y").date()
                            except: pass

                        # Se tiver múltiplos valores, tentamos achar o valor pago
                        # Geralmente é o último valor da linha no seu relatório
                        if len(todos_valores) > 1:
                            possivel_pago = parse_monetary(todos_valores[-1])
                            # Evita pegar totais somados (valor pago não deve ser 10x o original)
                            if possivel_pago < (valor_original * 10): 
                                valor_pago = possivel_pago

                        parcelas.append({
                            'Parcela': codigo,
                            'Dt Vencim': dt_venc,
                            'Valor Original': valor_original,
                            'Dt Receb': dt_pag,
                            'Valor Pago': valor_pago
                        })
                        
    except Exception as e:
        print(f"Erro parser: {str(e)}")
        
    return pd.DataFrame(parcelas)
