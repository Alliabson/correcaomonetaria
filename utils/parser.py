import pandas as pd
import pdfplumber
from datetime import datetime

def extract_payment_data(uploaded_file):
    """Extrai dados de parcelas de arquivos PDF ou Excel"""
    if uploaded_file.name.endswith('.pdf'):
        return extract_from_pdf(uploaded_file)
    elif uploaded_file.name.endswith(('.xlsx', '.xls')):
        return extract_from_excel(uploaded_file)
    else:
        raise ValueError("Formato de arquivo não suportado")
#Extratos de info dos PDF's
import pandas as pd
import pdfplumber
from datetime import datetime

def extract_payment_data(uploaded_file):
    """Extrai dados de parcelas de arquivos PDF ou Excel"""
    if uploaded_file.name.endswith('.pdf'):
        return extract_from_pdf(uploaded_file)
    elif uploaded_file.name.endswith(('.xlsx', '.xls')):
        return extract_from_excel(uploaded_file)
    else:
        raise ValueError("Formato de arquivo não suportado")
#Extratos de info dos PDF's
def extract_from_pdf(pdf_file):
    """Extrai dados específicos do PDF da Aquarela da Mata"""
    parcelas = []
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            lines = text.split('\n')
            
            in_payment_table = False
            header_found = False
            
            for line in lines:
                # Verifica se é a linha de cabeçalho da tabela
                if ("Parcela" in line and "Dt Vencim" in line and "Valor Parc." in line and 
                    "Dt. Receb." in line and "Vlr da Parcela" in line):
                    in_payment_table = True
                    header_found = True
                    continue
                
                # Processa as linhas de dados que começam com PR.
                if in_payment_table and (line.strip().startswith('PR.') or line.strip().startswith('E.') or 
                         line.strip().startswith('P.') or line.strip().startswith('B.') or line.strip().startswith('BR.') or line.strip().startswith('SM.') or line.strip().startswith('1.')
 or line.strip().startswith('BR.') or line.strip().startswith('ER.') ):
                    # Remove múltiplos espaços e divide corretamente
                    cleaned_line = ' '.join(line.split())
                    parts = cleaned_line.split()
                    
                    try:
                        # Ajuste os índices conforme necessário para o seu PDF
                        parcela_info = {
                            'Parcela': parts[0],
                            'Dt Vencim': parse_date(parts[1]),
                            'Valor Parcela': parse_currency(parts[3]),
                            'Dt Recebimento': parse_date(parts[4]),
                            'Valor Recebido': parse_currency(parts[10])
                        }
                        parcelas.append(parcela_info)
                        
                    except (IndexError, ValueError) as e:
                        print(f"Erro ao processar linha: {line}. Erro: {str(e)}")
                        continue
                
                # Finaliza quando encontrar o total
                if in_payment_table and "Total a pagar:" in line:
                    break
    
    if not parcelas:
        print("Nenhuma parcela encontrada. Linhas disponíveis:")
        for i, line in enumerate(lines):
            print(f"{i}: {line}")
    
    return pd.DataFrame(parcelas)

def parse_date(date_str):
    """Converte string de data no formato DD/MM/YYYY para objeto date"""
    return datetime.strptime(date_str, '%d/%m/%Y').date()

def parse_currency(value_str):
    """Converte valores monetários com vírgula decimal para float"""
    return float(value_str.replace('.', '').replace(',', '.'))

def extract_from_excel(excel_file):
    """Extrai dados de arquivos Excel com estrutura similar"""
    df = pd.read_excel(excel_file)
    
    # Mapeamento de colunas alternativas
    col_mapping = {
        'parcela': 'Parcela',
        'dt vencim': 'Dt Vencim',
        'valor parc': 'Valor Parcela',
        'dt receb': 'Dt Recebimento',
        'vlr parcela': 'Valor Recebido'
    }
    
    # Normalizar nomes de colunas
    df.columns = [col_mapping.get(col.lower().strip(), col) for col in df.columns]
    
    # Verificar se todas as colunas necessárias estão presentes
    required_columns = ['Parcela', 'Dt Vencim', 'Valor Parcela', 'Dt Recebimento', 'Valor Recebido']
    if not all(col in df.columns for col in required_columns):
        raise ValueError("Não foi possível identificar as colunas necessárias no arquivo Excel")
    
    # Converter tipos de dados
    date_columns = ['Dt Vencim', 'Dt Recebimento']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True).dt.date
    
    currency_columns = ['Valor Parcela', 'Valor Recebido']
    for col in currency_columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: parse_currency(str(x)) if isinstance(x, str) else float(x))
    
    return df[required_columns]

def parse_date(date_str):
    """Converte string de data no formato DD/MM/YYYY para objeto date"""
    return datetime.strptime(date_str, '%d/%m/%Y').date()

def parse_currency(value_str):
    """Converte valores monetários com vírgula decimal para float"""
    return float(value_str.replace('.', '').replace(',', '.'))

def extract_from_excel(excel_file):
    """Extrai dados de arquivos Excel com estrutura similar"""
    df = pd.read_excel(excel_file)
    
    # Mapeamento de colunas alternativas
    col_mapping = {
        'parcela': 'Parcela',
        'dt vencim': 'Dt Vencim',
        'valor parc': 'Valor Parcela',
        'dt receb': 'Dt Recebimento',
        'vlr parcela': 'Valor Recebido'
    }
    
    # Normalizar nomes de colunas
    df.columns = [col_mapping.get(col.lower().strip(), col) for col in df.columns]
    
    # Verificar se todas as colunas necessárias estão presentes
    required_columns = ['Parcela', 'Dt Vencim', 'Valor Parcela', 'Dt Recebimento', 'Valor Recebido']
    if not all(col in df.columns for col in required_columns):
        raise ValueError("Não foi possível identificar as colunas necessárias no arquivo Excel")
    
    # Converter tipos de dados
    date_columns = ['Dt Vencim', 'Dt Recebimento']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True).dt.date
    
    currency_columns = ['Valor Parcela', 'Valor Recebido']
    for col in currency_columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: parse_currency(str(x)) if isinstance(x, str) else float(x))
    
    return df[required_columns]
