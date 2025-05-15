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

def extract_from_pdf(pdf_file):
    """
    Extrai dados de parcelas de um PDF no formato do exemplo
    """
    parcelas = []
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            
            # Exemplo de extração - ajustar conforme o layout real do PDF
            lines = text.split('\n')
            
            # Procurar por linhas que contêm dados de parcelas
            for line in lines:
                if line.startswith('PR.'):  # Identifica linhas de parcelas
                    parts = line.split()
                    
                    try:
                        parcela = {
                            'Parcela': parts[0],
                            'Dt Vencim': pd.to_datetime(parts[1], format='%d/%m/%Y'),
                            'Valor Parcela': float(parts[2].replace('.', '').replace(',', '.'))
                        }
                        parcelas.append(parcela)
                    except (IndexError, ValueError):
                        continue
    
    return pd.DataFrame(parcelas)

def extract_from_excel(excel_file):
    """
    Extrai dados de parcelas de um arquivo Excel
    """
    df = pd.read_excel(excel_file)
    
    # Supondo que o Excel tenha colunas nomeadas de forma padrão
    # Ajustar conforme o layout real do arquivo
    required_columns = ['Parcela', 'Dt Vencim', 'Valor Parcela']
    
    # Verificar se as colunas necessárias existem
    if all(col in df.columns for col in required_columns):
        return df[required_columns].copy()
    else:
        # Tentar identificar colunas automaticamente
        # (implementação básica - melhorar conforme necessidade)
        for col in df.columns:
            if 'parcela' in col.lower():
                df.rename(columns={col: 'Parcela'}, inplace=True)
            elif 'vencim' in col.lower() or 'vencimento' in col.lower():
                df.rename(columns={col: 'Dt Vencim'}, inplace=True)
            elif 'valor' in col.lower() and 'parcela' in col.lower():
                df.rename(columns={col: 'Valor Parcela'}, inplace=True)
        
        if all(col in df.columns for col in required_columns):
            return df[required_columns].copy()
        else:
            raise ValueError("Não foi possível identificar as colunas necessárias no arquivo Excel")
