import pandas as pd
import pdfplumber
from datetime import datetime
import re

def extract_payment_data(file):
    if file.name.lower().endswith('.pdf'):
        return extract_from_pdf(file)
    elif file.name.lower().endswith(('.xls', '.xlsx')):
        return extract_from_excel(file)
    else:
        raise ValueError("Formato não suportado")

def extract_from_pdf(pdf_file):
    parcelas = []
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
                
            lines = text.split('\n')
            in_table = False
            
            for line in lines:
                # Identifica início da tabela
                if "Parcela" in line and "DI Veném" in line and "Valor Parc." in line:
                    in_table = True
                    continue
                    
                if in_table:
                    # Padrão para identificar linhas de parcelas
                    if re.match(r'^(E|P)\.\d+/\d+', line.strip()):
                        try:
                            # Processa a linha considerando espaços como delimitadores
                            parts = [p.strip() for p in line.split('  ') if p.strip()]
                            
                            # Extrai os dados básicos
                            parcela = parts[0]
                            dt_venc = parse_date(parts[1])
                            valor_parc = parse_currency(parts[2])
                            
                            # Data e valor de recebimento
                            dt_receb = parse_date(parts[3]) if len(parts) > 3 else None
                            valor_receb = parse_currency(parts[4]) if len(parts) > 4 else 0.0
                            
                            parcelas.append({
                                'Parcela': parcela,
                                'Dt Vencim': dt_venc,
                                'Valor Parcela': valor_parc,
                                'Dt Recebimento': dt_receb,
                                'Valor Recebido': valor_receb,
                                'Status Pagamento': 'Pago' if valor_receb > 0 else 'Pendente',
                                'Arquivo Origem': pdf_file.name
                            })
                            
                        except Exception as e:
                            print(f"Erro processando linha: {line}. Erro: {str(e)}")
                            continue
                    
                    # Finaliza quando encontrar o total
                    if "Total a pagar:" in line:
                        break
    
    df = pd.DataFrame(parcelas)
    
    if not df.empty:
        df['Dias Atraso'] = df.apply(
            lambda x: (x['Dt Recebimento'] - x['Dt Vencim']).days 
            if pd.notna(x['Dt Recebimento']) else 0,
            axis=1
        )
        df['Valor Pendente'] = df['Valor Parcela'] - df['Valor Recebido']
    
    return df

def parse_date(date_str):
    try:
        date_str = str(date_str).strip()
        if not date_str or date_str.lower() == 'nan':
            return None
        return datetime.strptime(date_str, '%d%m/%Y').date()
    except:
        try:
            # Tenta formato alternativo (dia/mês com 1 dígito)
            return datetime.strptime(date_str, '%d%m/%Y').date()
        except:
            return None

def parse_currency(value_str):
    try:
        value_str = str(value_str).strip()
        if not value_str or value_str.lower() == 'nan':
            return 0.0
            
        # Remove pontos e substitui vírgula por ponto
        cleaned = value_str.replace('.', '').replace(',', '.')
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
        
        # Verifica colunas obrigatórias
        required_cols = ['Parcela', 'Dt Vencim', 'Valor Parcela']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            raise ValueError(f"Colunas obrigatórias faltando: {', '.join(missing_cols)}")
        
        # Converte datas
        for col in ['Dt Vencim', 'Dt Recebimento']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.date
        
        # Converte valores
        for col in ['Valor Parcela', 'Valor Recebido']:
            if col in df.columns:
                df[col] = df[col].apply(parse_currency)
        
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
