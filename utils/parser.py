import pandas as pd
import pdfplumber

def extract_payment_data(uploaded_file):
    if uploaded_file.name.endswith('.pdf'):
        return extract_from_pdf(uploaded_file)
    elif uploaded_file.name.endswith(('.xlsx', '.xls')):
        return extract_from_excel(uploaded_file)
    else:
        raise ValueError("Formato de arquivo não suportado")

def extract_from_pdf(pdf_file):
    parcelas = []
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text: continue
            lines = text.split('\n')
            for line in lines:
                if line.startswith('PR.'):
                    parts = line.split()
                    try:
                        data_vencimento = pd.to_datetime(parts[1], format='%d/%m/%Y').date()
                        valor_str = parts[2].replace('.', '').replace(',', '.')
                        valor = float(valor_str)
                        parcela = {
                            'Parcela': parts[0],
                            'Dt Vencim': data_vencimento,
                            'Valor Parcela': valor
                        }
                        parcelas.append(parcela)
                    except (IndexError, ValueError):
                        continue
    return pd.DataFrame(parcelas)

def extract_from_excel(excel_file):
    df = pd.read_excel(excel_file)
    required_columns = ['Parcela', 'Dt Vencim', 'Valor Parcela']
    
    for col in df.columns:
        if 'parcela' in col.lower():
            df.rename(columns={col: 'Parcela'}, inplace=True)
        elif 'vencim' in col.lower():
            df.rename(columns={col: 'Dt Vencim'}, inplace=True)
        elif 'valor' in col.lower() and 'parcela' in col.lower():
            df.rename(columns={col: 'Valor Parcela'}, inplace=True)
            
    if all(col in df.columns for col in required_columns):
        return df[required_columns].copy()
    else:
        raise ValueError("Colunas necessárias não encontradas no Excel")
