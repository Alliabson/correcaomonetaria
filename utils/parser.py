import pandas as pd
import pdfplumber

def extract_payment_data(uploaded_file):
    """
    Extrai dados de parcelas de um arquivo PDF ou Excel
    """
    if uploaded_file.name.endswith('.pdf'):
        return extract_from_pdf(uploaded_file)
    elif uploaded_file.name.endswith(('.xlsx', '.xls')):
        return extract_from_excel(uploaded_file)
    else:
        raise ValueError("Formato de arquivo não suportado")
# Parcelas
def extract_from_pdf(pdf_file):
    """Extrai dados de parcelas de um PDF no formato do exemplo"""
    parcelas = []
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text: continue
            lines = text.split('\n')
            
            for line in lines:
                if line.strip().startswith('PR.'):  # Identifica linhas de parcelas
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
                    except (IndexError, ValueError) as e:
                        print(f"Erro ao processar linha: {line}. Erro: {str(e)}")
                        continue
    
    return pd.DataFrame(parcelas)

def extract_from_excel(excel_file):
    """
    Extrai dados de parcelas de um arquivo Excel
    """
    df = pd.read_excel(excel_file)
    
    required_columns_map = {
        'Parcela': ['parcela', 'parc.'],
        'Dt Vencim': ['vencim', 'vencimento'],
        'Valor Parcela': ['valor', 'valor parcela']
    }
    
    rename_dict = {}
    for standard_col, possible_names in required_columns_map.items():
        for col in df.columns:
            if col.lower() in possible_names:
                rename_dict[col] = standard_col
                break
    
    df.rename(columns=rename_dict, inplace=True)
    
    if all(col in df.columns for col in required_columns_map.keys()):
        return df[list(required_columns_map.keys())].copy()
    else:
        raise ValueError("Não foi possível identificar as colunas necessárias no arquivo Excel")
