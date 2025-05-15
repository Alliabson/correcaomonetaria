import pandas as pd
import pdfplumber
from datetime import datetime
import re

def extract_payment_data(file):
    """Função principal para extrair dados de pagamento"""
    if file.name.lower().endswith('.pdf'):
        return extract_from_pdf(file)
    elif file.name.lower().endswith(('.xls', '.xlsx')):
        return extract_from_excel(file)
    else:
        raise ValueError("Formato de arquivo não suportado")

def extract_from_pdf(pdf_file):
    """Extrai dados do PDF com padrão robusto de identificação de parcelas"""
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
                if "Parcela" in line and ("DI Veném" in line or "Dt Vencim" in line) and "Valor Parc." in line:
                    in_payment_section = True
                    continue
                
                if in_payment_section:
                    # Padrão robusto para identificar parcelas (E., P., PR., BR., SM., etc.)
                    parcela_pattern = r'^([A-Z]{1,3}\.\d+/\d+)'
                    match = re.search(parcela_pattern, line.strip())
                    
                    if match:
                        try:
                            # Divide a linha em partes
                            parts = re.split(r'\s{2,}', line.strip())
                            
                            # Extrai informações básicas
                            parcela = parts[0]
                            dt_vencim = parse_date(parts[1])
                            valor_parc = parse_currency(parts[2])
                            
                            # Verifica se há data de recebimento e valor
                            dt_receb = None
                            valor_recebido = 0.0
                            
                            # Procura por padrão de data (dd/mm/aaaa)
                            for i, part in enumerate(parts[3:], 3):
                                if re.match(r'\d{2}/\d{2}/\d{4}', part):
                                    dt_receb = parse_date(part)
                                    if i+1 < len(parts):
                                        valor_recebido = parse_currency(parts[i+1])
                                    break
                            
                            # Adiciona à lista de parcelas
                            parcelas.append({
                                'Parcela': parcela,
                                'Dt Vencim': dt_vencim,
                                'Valor Parcela': valor_parc,
                                'Dt Recebimento': dt_receb,
                                'Valor Recebido': valor_recebido,
                                'Status Pagamento': 'Pago' if valor_recebido > 0 else 'Pendente',
                                'Arquivo Origem': pdf_file.name
                            })
                            
                        except Exception as e:
                            print(f"Erro ao processar linha: {line}. Erro: {str(e)}")
                            continue
                    
                    # Finaliza quando encontrar o total
                    if "Total a pagar:" in line or "TOTAL GERAL:" in line:
                        break
    
    # Cria DataFrame com tratamento seguro
    df = pd.DataFrame(parcelas)
    
    if not df.empty:
        # Calcula dias de atraso apenas para parcelas pagas
        df['Dias Atraso'] = df.apply(
            lambda x: (x['Dt Recebimento'] - x['Dt Vencim']).days 
            if pd.notna(x['Dt Recebimento']) and x['Dt Recebimento'] > x['Dt Vencim']
            else 0,
            axis=1
        )
        
        df['Valor Pendente'] = df['Valor Parcela'] - df['Valor Recebido']
    
    return df

def parse_date(date_str):
    """Converte string de data para objeto date"""
    try:
        date_str = str(date_str).strip()
        if not date_str or date_str.lower() == 'nan':
            return None
        
        # Tenta vários formatos de data
        for fmt in ['%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d', '%d.%m.%Y']:
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
            
        # Remove R$ e espaços
        value_str = re.sub(r'[^\d,-.]', '', value_str)
        
        # Caso 1.234,56 → 1234.56
        if '.' in value_str and ',' in value_str:
            return float(value_str.replace('.', '').replace(',', '.'))
        # Caso 1,234.56 → 1234.56
        elif ',' in value_str and value_str.count(',') == 1 and len(value_str.split(',')[1]) == 2:
            return float(value_str.replace(',', ''))
        # Caso 1234,56 → 1234.56
        elif ',' in value_str:
            return float(value_str.replace(',', '.'))
        else:
            return float(value_str)
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
