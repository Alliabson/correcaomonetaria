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
        # Remove R$, espaços e aspas que possam ter vindo na extração
        clean = re.sub(r'[R$\s"\'a-zA-Z]', '', str(value))
        
        # Lógica para 1.000,00 (Brasil)
        if ',' in clean and '.' in clean:
            return float(clean.replace('.', '').replace(',', '.'))
        # Lógica para 1000,00
        elif ',' in clean:
            return float(clean.replace(',', '.'))
        # Lógica para 1000.00
        elif '.' in clean:
            return float(clean)
        return float(clean)
    except:
        return 0.0

def extract_from_pdf(pdf_file):
    parcelas = []
    
    # Regex flexível para capturar:
    # 1. Código (P.1/24, E.1/3, etc)
    # 2. Data (dd/mm/aaaa)
    # 3. Valor (números com ponto ou vírgula)
    # O ".*?" significa "ignore qualquer lixo entre eles"
    regex_linha = r'([A-Z]+\.?\d+/\d+).*?(\d{2}/\d{2}/\d{4}).*?([\d\.,]+)'
    
    try:
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text: continue
                
                lines = text.split('\n')
                for line in lines:
                    # Ignora linhas de totais
                    if "Total" in line or "Geral" in line: continue

                    # Procura o padrão na linha
                    match = re.search(regex_linha, line)
                    if match:
                        codigo = match.group(1)
                        dt_venc_str = match.group(2)
                        valor_str = match.group(3) # Pega o primeiro valor (Original)
                        
                        # Tenta encontrar TODOS os valores monetários na linha
                        # Motivo: Geralmente a linha é: Cod | Venc | ValorOrig | ... | ValorPago
                        todos_valores = re.findall(r'(\d{1,3}(?:[\.,]\d{3})*[\.,]\d{2})', line)
                        todas_datas = re.findall(r'(\d{2}/\d{2}/\d{4})', line)
                        
                        dt_venc = datetime.strptime(dt_venc_str, "%d/%m/%Y").date()
                        valor_original = parse_monetary(valor_str)
                        
                        dt_pag = None
                        valor_pago = 0.0
                        
                        # Se tiver mais de uma data, a segunda é pagamento
                        if len(todas_datas) > 1:
                            try:
                                dt_pag = datetime.strptime(todas_datas[1], "%d/%m/%Y").date()
                            except: pass

                        # Se tiver mais de um valor e o último for diferente do original, pode ser o pago
                        # No seu relatório, o último valor da linha costuma ser o pago
                        if len(todos_valores) > 1:
                            possivel_pago = parse_monetary(todos_valores[-1])
                            # Validação simples: Valor pago não costuma ser absurdamente maior que o original (ex: totais somados)
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
