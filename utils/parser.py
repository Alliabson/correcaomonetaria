import pandas as pd
import pdfplumber
from datetime import datetime
import re

def extract_payment_data(file):
    """Função unificada para extrair dados de PDF ou Excel"""
    if file.name.lower().endswith('.pdf'):
        return extract_from_pdf(file)
    elif file.name.lower().endswith(('.xls', '.xlsx')):
        return extract_from_excel(file)
    else:
        raise ValueError("Formato de arquivo não suportado")

def extract_from_pdf(pdf_file):
    """Extrai dados específicos do PDF com tratamento completo"""
    parcelas = []
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
                
            lines = text.split('\n')
            in_payment_table = False
            
            for line in lines:
                # Verificação robusta do cabeçalho
                if all(term in line for term in ["Parcela", "Dt Vencim", "Valor Parc.", "Dt. Receb.", "Vlr da Parcela"]):
                    in_payment_table = True
                    continue
                    
                if in_payment_table and line.strip():
                    # Padrão completo para capturar todos os dados
                    pattern = (
                        r'^(?P<parcela>[A-Z]{1,3}\.\d+/\d+)\s+'  # Parcela (E.1/1, P.1/35)
                        r'(?P<dt_vencim>\d{2}/\d{2}/\d{4})\s+'   # Data vencimento (20/01/2024)
                        r'(?P<valor_parc>[\d.,]+)\s+'            # Valor parcela (100.000,00)
                        r'(?P<dt_receb>\d{2}/\d{2}/\d{4})?\s*'   # Data recebimento (opcional)
                        r'(?P<valor_recebido>[\d.,]+)?\s*'       # Valor recebido (opcional)
                    )
                    
                    match = re.search(pattern, line.strip())
                    if match:
                        try:
                            # Processa os dados básicos
                            parcela = match.group('parcela')
                            dt_vencim = datetime.strptime(match.group('dt_vencim'), '%d/%m/%Y').date()
                            valor_parc = float(match.group('valor_parc').replace('.', '').replace(',', '.'))
                            
                            # Processa dados de recebimento (opcionais)
                            dt_receb = None
                            valor_recebido = 0.0
                            
                            if match.group('dt_receb'):
                                dt_receb = datetime.strptime(match.group('dt_receb'), '%d/%m/%Y').date()
                            
                            if match.group('valor_recebido'):
                                valor_recebido = float(match.group('valor_recebido').replace('.', '').replace(',', '.'))
                            
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
                            
                        except Exception as e:
                            print(f"Erro ao processar linha: {line}. Erro: {str(e)}")
                            continue
                    
                    # Finaliza quando encontrar o total
                    if any(t in line for t in ["Total a pagar:", "TOTAL GERAL:", "RECEBIDO :"]):
                        break
    
    # Cria DataFrame com todas as parcelas
    df = pd.DataFrame(parcelas)
    
    if not df.empty:
        # Calcula campos adicionais
        df['Dias Atraso'] = df.apply(
            lambda x: (x['Dt Recebimento'] - x['Dt Vencim']).days 
            if pd.notnull(x['Dt Recebimento']) and x['Dt Recebimento'] > x['Dt Vencim'] 
            else 0,
            axis=1
        )
        df['Valor Pendente'] = df['Valor Parcela'] - df['Valor Recebido']
        
        # Ordena por data de vencimento
        df = df.sort_values('Dt Vencim')
    
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
        cleaned_value = str(value_str).replace('R$', '').strip()
        return float(cleaned_value.replace('.', '').replace(',', '.'))
    except (ValueError, AttributeError):
        return 0.0

def extract_from_excel(excel_file):
    """Extrai dados de arquivos Excel com estrutura similar"""
    try:
        df = pd.read_excel(excel_file, engine='openpyxl')
        
        # Mapeamento de colunas
        col_mapping = {
            'parcela': 'Parcela',
            'numero parcela': 'Parcela',
            'dt vencim': 'Dt Vencim',
            'data vencimento': 'Dt Vencim',
            'valor parc': 'Valor Parcela',
            'valor parcela': 'Valor Parcela',
            'dt receb': 'Dt Recebimento',
            'data recebimento': 'Dt Recebimento',
            'vlr parcela': 'Valor Recebido',
            'valor recebido': 'Valor Recebido'
        }
        
        # Normaliza nomes de colunas
        df.columns = [col_mapping.get(col.lower().strip(), col) for col in df.columns]
        
        # Verifica colunas obrigatórias
        required_columns = ['Parcela', 'Dt Vencim', 'Valor Parcela']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Colunas obrigatórias não encontradas: {', '.join(missing_columns)}")
        
        # Conversão de tipos
        if 'Dt Vencim' in df.columns:
            df['Dt Vencim'] = pd.to_datetime(df['Dt Vencim'], dayfirst=True, errors='coerce').dt.date
        if 'Dt Recebimento' in df.columns:
            df['Dt Recebimento'] = pd.to_datetime(df['Dt Recebimento'], dayfirst=True, errors='coerce').dt.date
        
        if 'Valor Parcela' in df.columns:
            df['Valor Parcela'] = df['Valor Parcela'].apply(lambda x: parse_currency(str(x)))
        if 'Valor Recebido' in df.columns:
            df['Valor Recebido'] = df['Valor Recebido'].apply(lambda x: parse_currency(str(x)))
        
        # Adiciona colunas calculadas
        df['Status Pagamento'] = df.apply(
            lambda x: 'Pago' if x.get('Valor Recebido', 0) > 0 else 'Pendente', 
            axis=1
        )
        df['Dias Atraso'] = df.apply(
            lambda x: (x['Dt Recebimento'] - x['Dt Vencim']).days 
            if 'Dt Recebimento' in df.columns and pd.notnull(x['Dt Recebimento']) and x['Dt Recebimento'] > x['Dt Vencim'] 
            else 0,
            axis=1
        )
        df['Valor Pendente'] = df['Valor Parcela'] - df.get('Valor Recebido', 0)
        df['Arquivo Origem'] = excel_file.name
        
        return df
    
    except Exception as e:
        raise ValueError(f"Erro ao processar arquivo Excel: {str(e)}")
