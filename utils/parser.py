import pandas as pd
import pdfplumber
from datetime import datetime
import re

def extract_payment_data(file):
    """Função unificada para extrair dados de PDF ou Excel"""
    if file.name.lower().endswith('.pdf'):
        return extract_from_pdf_specific(file)
    elif file.name.lower().endswith(('.xls', '.xlsx')):
        return extract_from_excel(file)
    else:
        raise ValueError("Formato de arquivo não suportado")

def extract_from_pdf_specific(pdf_file):
    """Extrai dados específicos do PDF 15-AM005-362.pdf"""
    parcelas = []
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
                
            lines = text.split('\n')
            in_payment_section = False
            
            for line in lines:
                # Identifica o início da seção de pagamentos
                if "Parcela" in line and "DI Veném" in line and "Valor Parc." in line:
                    in_payment_section = True
                    continue
                
                # Processa as linhas de pagamento
                if in_payment_section:
                    # Padrão para identificar linhas de parcelas (E., P., etc.)
                    if re.match(r'^(E|P|B)\.\d+/\d+', line.strip()):
                        try:
                            # Processamento específico para o formato do PDF
                            parts = re.split(r'\s{2,}', line.strip())
                            
                            # Extrai informações básicas
                            parcela = parts[0]
                            dt_vencim = parse_date(parts[1])
                            valor_parc = parse_currency(parts[2])
                            
                            # Data e valor de recebimento podem estar em posições variáveis
                            dt_receb = None
                            valor_recebido = 0.0
                            
                            # Procura por padrão de data (dd/mm/aaaa)
                            date_pattern = r'\d{2}/\d{2}/\d{4}'
                            for i, part in enumerate(parts[3:], 3):
                                if re.match(date_pattern, part):
                                    dt_receb = parse_date(part)
                                    # O valor recebido geralmente vem após a data
                                    if i+1 < len(parts):
                                        valor_recebido = parse_currency(parts[i+1])
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
                            print(f"Erro ao processar linha: {line}. Erro: {str(e)}")
                            continue
                    
                    # Finaliza quando encontrar o total
                    if "Total a pagar:" in line:
                        break
    
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
        return datetime.strptime(date_str, '%d/%m/%Y').date()
    except (ValueError, TypeError):
        return None

def parse_currency(value_str):
    """Converte valores monetários com vírgula decimal para float"""
    try:
        if isinstance(value_str, (int, float)):
            return float(value_str)
            
        cleaned_value = str(value_str).replace('R$', '').replace('.', '').replace(',', '.').strip()
        return float(cleaned_value)
    except (ValueError, AttributeError):
        return 0.0

def extract_from_excel(excel_file):
    """Extrai dados de arquivos Excel (mantido igual)"""
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
