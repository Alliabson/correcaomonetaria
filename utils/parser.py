import pandas as pd
import pdfplumber
from datetime import datetime
import re

def extract_payment_data(file):
    """Função unificada para extrair dados de PDF ou Excel"""
    if file.name.lower().endswith('.pdf'):
        return extract_from_pdf_with_tables(file)
    elif file.name.lower().endswith(('.xls', '.xlsx')):
        return extract_from_excel(file)
    else:
        raise ValueError("Formato de arquivo não suportado")

def extract_from_pdf_with_tables(pdf_file):
    """Extrai dados usando análise de tabelas do PDFplumber"""
    parcelas = []
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            # Tenta extrair tabelas
            tables = page.extract_tables({
                "vertical_strategy": "text", 
                "horizontal_strategy": "text",
                "explicit_vertical_lines": [],
                "explicit_horizontal_lines": [],
                "intersection_y_tolerance": 10
            })
            
            for table in tables:
                for row in table:
                    # Verifica se a linha parece ser uma parcela
                    if len(row) > 2 and re.match(r'^(E|P|B)\.\d+/\d+', str(row[0])):
                        try:
                            # Processa a linha da tabela
                            parcela = str(row[0]).strip()
                            dt_vencim = parse_date(str(row[1]))
                            valor_parc = parse_currency(str(row[2]))
                            
                            # Tenta encontrar data de recebimento e valor
                            dt_receb = None
                            valor_recebido = 0.0
                            
                            for i, cell in enumerate(row[3:], 3):
                                if cell and parse_date(str(cell))):
                                    dt_receb = parse_date(str(cell))
                                    if i+1 < len(row) and row[i+1]:
                                        valor_recebido = parse_currency(str(row[i+1]))
                                    break
                            
                            # Adiciona à lista de parcelas
                            parcela_info = {
                                'Parcela': parcela,
                                'Dt Vencim': dt_vencim,
                                'Valor Parcela': valor_parc,
                                'Dt Recebimento': dt_receb,
                                'Valor Recebido': valor_recebido,
                                'Status Pagamento': 'Pago' if valor_recebido > 0 else 'Pendente',
                                'Arquivo Origem': pdf_file.name
                            }
                            parcelas.append(parcela_info)
                            
                        except (IndexError, ValueError, AttributeError) as e:
                            print(f"Erro ao processar linha: {row}. Erro: {str(e)}")
                            continue
    
    # Cria DataFrame
    df = pd.DataFrame(parcelas)
    
    if not df.empty:
        # Calcula dias de atraso e valor pendente
        df['Dias Atraso'] = df.apply(
            lambda x: (x['Dt Recebimento'] - x['Dt Vencim']).days 
            if x['Dt Recebimento'] and x['Dt Recebimento'] > x['Dt Vencim'] 
            else 0,
            axis=1
        )
        df['Valor Pendente'] = df['Valor Parcela'] - df['Valor Recebido']
    
    return df

def parse_date(date_str):
    """Converte string de data no formato DD/MM/YYYY para objeto date"""
    try:
        date_str = str(date_str).strip()
        if not date_str or date_str.lower() == 'nan':
            return None
        return datetime.strptime(date_str, '%d/%m/%Y').date()
    except (ValueError, TypeError):
        return None

def parse_currency(value_str):
    """Converte valores monetários com vírgula decimal para float"""
    try:
        value_str = str(value_str).strip()
        if not value_str or value_str.lower() == 'nan':
            return 0.0
            
        # Remove caracteres não numéricos exceto vírgula e ponto
        cleaned = re.sub(r'[^\d,.-]', '', value_str)
        
        # Caso tenha tanto ponto quanto vírgula (1.234,56)
        if '.' in cleaned and ',' in cleaned:
            cleaned = cleaned.replace('.', '').replace(',', '.')
        # Caso tenha apenas vírgula (1234,56)
        elif ',' in cleaned:
            cleaned = cleaned.replace(',', '.')
        # Caso tenha apenas ponto (1234.56 ou 1.234)
        elif '.' in cleaned:
            # Verifica se é separador de milhar (1.234)
            parts = cleaned.split('.')
            if len(parts[-1]) == 3 and len(parts) > 1:
                cleaned = cleaned.replace('.', '')
            # Caso contrário, assume que é decimal (1234.56)
        
        return float(cleaned)
    except (ValueError, AttributeError):
        return 0.0

def extract_from_excel(excel_file):
    """Extrai dados de arquivos Excel"""
    try:
        df = pd.read_excel(excel_file, engine='openpyxl')
        
        col_mapping = {
            'parcela': 'Parcela',
            'numero parcela': 'Parcela',
            'dt vencim': 'Dt Vencim',
            'data vencimento': 'Dt Vencim',
            'vencimento': 'Dt Vencim',
            'valor parc': 'Valor Parcela',
            'valor': 'Valor Parcela',
            'valor parcela': 'Valor Parcela',
            'dt receb': 'Dt Recebimento',
            'data recebimento': 'Dt Recebimento',
            'recebimento': 'Dt Recebimento',
            'vlr parcela': 'Valor Recebido',
            'valor recebido': 'Valor Recebido',
            'vlr recebido': 'Valor Recebido',
            'pagamento': 'Valor Recebido'
        }
        
        df.columns = [col_mapping.get(col.lower().strip(), col) for col in df.columns]
        
        required_columns = ['Parcela', 'Dt Vencim', 'Valor Parcela', 'Dt Recebimento', 'Valor Recebido']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Colunas obrigatórias não encontradas: {', '.join(missing_columns)}")
        
        date_columns = ['Dt Vencim', 'Dt Recebimento']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.date
        
        currency_columns = ['Valor Parcela', 'Valor Recebido']
        for col in currency_columns:
            df[col] = df[col].apply(lambda x: parse_currency(str(x)) if pd.notna(x) else 0.0)
        
        df['Status Pagamento'] = df.apply(
            lambda x: 'Pago' if x['Valor Recebido'] > 0 else 'Pendente', 
            axis=1
        )
        df['Dias Atraso'] = (pd.to_datetime(df['Dt Recebimento']) - pd.to_datetime(df['Dt Vencim'])).dt.days
        df['Valor Pendente'] = df['Valor Parcela'] - df['Valor Recebido']
        df['Arquivo Origem'] = excel_file.name
        
        return df[required_columns + ['Status Pagamento', 'Dias Atraso', 'Valor Pendente', 'Arquivo Origem']]
    
    except Exception as e:
        raise ValueError(f"Erro ao processar arquivo Excel: {str(e)}")
