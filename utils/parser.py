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
    """Extrai dados do PDF específico 15-AM005-362.pdf"""
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
                
                if in_payment_section:
                    # Padrão para identificar linhas de parcelas
                    if re.match(r'^(E|P|B)\.\d+/\d+', line.strip()):
                        try:
                            # Processa a linha usando espaços como delimitadores
                            parts = re.split(r'\s{2,}', line.strip())
                            
                            # Extrai informações básicas
                            parcela = parts[0]
                            dt_vencim = parse_date(parts[1])
                            valor_parc = parse_currency(parts[2])
                            
                            # Verifica se há data de recebimento e valor
                            dt_receb = None
                            valor_recebido = 0.0
                            
                            if len(parts) > 3 and '/' in parts[3]:
                                dt_receb = parse_date(parts[3])
                                if len(parts) > 4:
                                    valor_recebido = parse_currency(parts[4])
                            
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
                    if "Total a pagar:" in line:
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
        return datetime.strptime(date_str, '%d/%m/%Y').date()
    except:
        return None

def parse_currency(value_str):
    """Converte valores monetários para float"""
    try:
        value_str = str(value_str).strip()
        if not value_str or value_str.lower() == 'nan':
            return 0.0
            
        # Remove R$ e espaços
        value_str = value_str.replace('R$', '').strip()
        
        # Verifica se tem ponto e vírgula
        if '.' in value_str and ',' in value_str:
            return float(value_str.replace('.', '').replace(',', '.'))
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
            'vencimento': 'Dt Vencim',
            'valor': 'Valor Parcela',
            'recebimento': 'Dt Recebimento',
            'valor recebido': 'Valor Recebido'
        }
        
        # Renomeia colunas
        df.columns = [col_map.get(col.lower(), col) for col in df.columns]
        
        # Converte datas
        for col in ['Dt Vencim', 'Dt Recebimento']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], dayfirst=True).dt.date
        
        # Converte valores
        for col in ['Valor Parcela', 'Valor Recebido']:
            if col in df.columns:
                df[col] = df[col].apply(parse_currency)
        
        # Calcula campos adicionais
        if 'Dt Recebimento' in df.columns:
            df['Status Pagamento'] = df['Dt Recebimento'].apply(lambda x: 'Pago' if pd.notna(x) else 'Pendente')
        else:
            df['Status Pagamento'] = 'Pendente'
        
        if all(col in df.columns for col in ['Dt Recebimento', 'Dt Vencim']):
            df['Dias Atraso'] = (df['Dt Recebimento'] - df['Dt Vencim']).dt.days
            df['Dias Atraso'] = df['Dias Atraso'].apply(lambda x: x if x > 0 else 0)
        else:
            df['Dias Atraso'] = 0
        
        if all(col in df.columns for col in ['Valor Parcela', 'Valor Recebido']):
            df['Valor Pendente'] = df['Valor Parcela'] - df['Valor Recebido']
        else:
            df['Valor Pendente'] = df['Valor Parcela']
        
        df['Arquivo Origem'] = excel_file.name
        
        return df
        
    except Exception as e:
        raise ValueError(f"Erro ao processar Excel: {str(e)}")
