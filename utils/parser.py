import pandas as pd
import pdfplumber
import re
from datetime import datetime

def extract_payment_data(file):
    # Detecta se é PDF ou Excel
    if hasattr(file, 'name'):
        if file.name.endswith('.pdf'): return extract_from_pdf(file)
        if file.name.endswith(('.xls', '.xlsx')): return pd.read_excel(file)
    return pd.DataFrame()

def parse_monetary(val_str):
    try:
        return float(val_str.replace('.', '').replace(',', '.'))
    except: return 0.0

def extract_from_pdf(pdf_file):
    parcelas = []
    # Regex ajustado para o layout do seu PDF (Aquarela)
    # Procura linhas que começam com algo como "E.1/3" ou "P.1/24" seguido de data
    regex_linha = r'([A-Z]?\.?\d+/\d+)\s+(\d{2}/\d{2}/\d{4})\s+.*(\d{1,3}(?:\.\d{3})*,\d{2})'
    
    try:
        with pdfplumber.open(pdf_file) as pdf:
            full_text = ""
            for page in pdf.pages:
                full_text += page.extract_text() or ""
            
            lines = full_text.split('\n')
            for line in lines:
                # Procura o padrão da parcela
                match = re.search(regex_linha, line)
                if match:
                    codigo = match.group(1)
                    dt_venc_str = match.group(2)
                    valor_orig_str = match.group(3)
                    
                    # Tenta achar data de pagamento e valor pago na mesma linha
                    # O layout parece ter: Cod | Venc | ... | Valor | Receb | ValorPago
                    # Vamos tentar pegar todas as ocorrências de dinheiro e data
                    todas_datas = re.findall(r'(\d{2}/\d{2}/\d{4})', line)
                    todos_valores = re.findall(r'(\d{1,3}(?:\.\d{3})*,\d{2})', line)
                    
                    valor_original = parse_monetary(valor_orig_str)
                    dt_venc = datetime.strptime(dt_venc_str, "%d/%m/%Y").date()
                    
                    dt_pag = None
                    valor_pago = 0.0
                    
                    # Se tiver mais de uma data, a segunda é pgto
                    if len(todas_datas) > 1:
                        dt_pag = datetime.strptime(todas_datas[1], "%d/%m/%Y").date()
                    
                    # Se tiver mais de um valor, o último costuma ser o pago
                    if len(todos_valores) > 1:
                        valor_pago = parse_monetary(todos_valores[-1])

                    parcelas.append({
                        'Parcela': codigo,
                        'Dt Vencim': dt_venc,
                        'Valor Original': valor_original,
                        'Dt Receb': dt_pag,
                        'Valor Pago': valor_pago
                    })
                    
    except Exception as e:
        print(f"Erro parser: {e}")
        
    return pd.DataFrame(parcelas)
