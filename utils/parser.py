import pandas as pd
import pdfplumber
from datetime import datetime
import re
from typing import List, Dict

def extract_payment_data(file):
    """Função principal para extração de dados"""
    if file.name.lower().endswith('.pdf'):
        return extract_from_pdf(file)
    elif file.name.lower().endswith(('.xls', '.xlsx')):
        return extract_from_excel(file)
    else:
        raise ValueError("Formato de arquivo não suportado")

def extract_from_pdf(pdf_file) -> Dict:
    """Extrai dados do PDF e retorna estrutura com tabela e metadados"""
    result = {
        'raw_text': '',
        'tables': [],
        'columns': [],
        'parcelas': []
    }
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            # Extrai texto bruto para análise
            text = page.extract_text()
            result['raw_text'] += text + "\n\n"
            
            # Extrai tabelas usando algoritmo otimizado
            tables = page.extract_tables({
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
                "intersection_y_tolerance": 10
            })
            
            for table in tables:
                if len(table) > 1:  # Ignora tabelas vazias
                    result['tables'].append(table)
                    
                    # Identifica cabeçalhos
                    if any("Parcela" in str(cell) for cell in table[0]):
                        headers = [str(cell).strip() for cell in table[0]]
                        result['columns'] = headers
                        
                        # Processa linhas de dados
                        for row in table[1:]:
                            if len(row) >= len(headers):
                                parcela = {
                                    'raw_data': row,
                                    'processed': False
                                }
                                for i, header in enumerate(headers):
                                    parcela[header] = row[i] if i < len(row) else None
                                result['parcelas'].append(parcela)
    
    return result

def process_extracted_data(raw_data: Dict, config: Dict) -> pd.DataFrame:
    """Processa os dados extraídos conforme configuração"""
    parcelas = []
    
    for parcela in raw_data['parcelas']:
        try:
            processed = {
                'Parcela': parcela.get(config.get('col_parcela', 'Parcela'), ''),
                'Dt Vencim': parse_date(parcela.get(config.get('col_dt_vencim', 'DI Veném Atraso'), '')),
                'Valor Parcela': parse_currency(parcela.get(config.get('col_valor_parc', 'Valor Parc.'), '0')),
                'Dt Recebimento': parse_date(parcela.get(config.get('col_dt_receb', 'Di. Receb.'), '')),
                'Valor Recebido': parse_currency(parcela.get(config.get('col_valor_receb', 'Vir da Parcela'), '0')),
                'Arquivo Origem': 'PDF'
            }
            
            processed['Status Pagamento'] = 'Pago' if processed['Valor Recebido'] > 0 else 'Pendente'
            processed['Dias Atraso'] = (processed['Dt Recebimento'] - processed['Dt Vencim']).days if processed['Dt Recebimento'] else 0
            processed['Valor Pendente'] = processed['Valor Parcela'] - processed['Valor Recebido']
            
            parcelas.append(processed)
            
        except Exception as e:
            print(f"Erro ao processar parcela: {parcela}. Erro: {str(e)}")
            continue
    
    return pd.DataFrame(parcelas)

def parse_date(date_str):
    """Converte string de data para objeto date"""
    try:
        date_str = str(date_str).strip()
        if not date_str or date_str.lower() == 'nan':
            return None
        
        # Tenta vários formatos de data
        for fmt in ['%d%m/%Y', '%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d']:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        return None
    except:
        return None

def parse_currency(value_str):
    """Converte valores monetários para float"""
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
        
        return float(cleaned)
    except:
        return 0.0

def extract_from_excel(excel_file):
    """Extrai dados de arquivos Excel"""
    try:
        df = pd.read_excel(excel_file)
        
        # Mapeamento de colunas
        col_map = {
            'parcela': 'Parcela',
            'numero_parcela': 'Parcela',
            'vencimento': 'Dt Vencim',
            'data_vencimento': 'Dt Vencim',
            'valor': 'Valor Parcela',
            'valor_parcela': 'Valor Parcela',
            'recebimento': 'Dt Recebimento',
            'data_recebimento': 'Dt Recebimento',
            'valor_recebido': 'Valor Recebido',
            'vlr_recebido': 'Valor Recebido'
        }
        
        # Normaliza nomes de colunas
        df.columns = [col_map.get(col.lower().replace(' ', '_'), col) for col in df.columns]
        
        # Converte datas
        for col in ['Dt Vencim', 'Dt Recebimento']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.date
        
        # Converte valores
        for col in ['Valor Parcela', 'Valor Recebido']:
            if col in df.columns:
                df[col] = df[col].apply(parse_currency_specific)
        
        # Status de pagamento
        df['Status Pagamento'] = df.apply(
            lambda x: 'Pago' if pd.notna(x.get('Dt Recebimento')) and x.get('Valor Recebido', 0) > 0 
            else 'Pendente', 
            axis=1
        )
        
        # Dias de atraso
        if all(col in df.columns for col in ['Dt Recebimento', 'Dt Vencim']):
            df['Dias Atraso'] = (pd.to_datetime(df['Dt Recebimento']) - pd.to_datetime(df['Dt Vencim'])).dt.days
            df['Dias Atraso'] = df['Dias Atraso'].clip(lower=0)
        else:
            df['Dias Atraso'] = 0
        
        # Valor pendente
        if all(col in df.columns for col in ['Valor Parcela', 'Valor Recebido']):
            df['Valor Pendente'] = df['Valor Parcela'] - df['Valor Recebido']
        else:
            df['Valor Pendente'] = df['Valor Parcela']
        
        df['Arquivo Origem'] = excel_file.name
        
        return df
        
    except Exception as e:
        raise ValueError(f"Erro ao processar Excel: {str(e)}")
