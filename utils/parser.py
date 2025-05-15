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
    """Extrai dados específicos do PDF com tratamento para formatos complexos"""
    parcelas = []
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            lines = text.split('\n')
            
            in_payment_table = False
            header_found = False
            
            for line in lines:
                # Verificação flexível de cabeçalho
                if ("Parcela" in line and "Dt Vencim" in line and "Valor Parc." in line and 
                    "Dt. Receb." in line and "Vlr da Parcela" in line):
                    in_payment_table = True
                    header_found = True
                    continue
                
                # Processa linhas dentro da tabela
                if in_payment_table and line.strip():
                    # Padrão para identificar linhas de parcelas
                    parcela_pattern = r'^([A-Z]\.\d+/\d+|P\.\d+/\d+|E\.\d+/\d+)'
                    match = re.search(parcela_pattern, line.strip())
                    
                    if match:
                        cleaned_line = ' '.join(line.split())
                        parts = cleaned_line.split()
                        
                        try:
                            # Extrai parcela
                            parcela = parts[0]
                            
                            # Converte data de vencimento (formato DDMM/AAAA)
                            dt_vencim_str = parts[1].replace('/', '')
                            dt_vencim = datetime.strptime(dt_vencim_str[:2] + '/' + dt_vencim_str[2:4] + '/' + dt_vencim_str[4:], '%d/%m/%Y').date()
                            
                            # Extrai valor da parcela (trata diferentes formatos)
                            valor_parc_str = parts[2].replace('.', '').replace(',', '.')
                            valor_parcela = float(valor_parc_str)
                            
                            # Verifica se há data de recebimento
                            dt_receb = None
                            valor_recebido = 0.0
                            
                            if len(parts) > 3 and '/' in parts[3]:
                                dt_receb_str = parts[3].replace('/', '')
                                dt_receb = datetime.strptime(dt_receb_str[:2] + '/' + dt_receb_str[2:4] + '/' + dt_receb_str[4:], '%d/%m/%Y').date()
                                
                                if len(parts) > 4:
                                    valor_receb_str = parts[4].replace('.', '').replace(',', '.')
                                    valor_recebido = float(valor_receb_str)
                            
                            # Adiciona à lista de parcelas
                            parcela_info = {
                                'Parcela': parcela,
                                'Dt Vencim': dt_vencim,
                                'Valor Parcela': valor_parcela,
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
                if in_payment_table and any(t in line for t in ["Total a pagar:", "TOTAL GERAL:"]):
                    break
    
    # Consolida múltiplos pagamentos para a mesma parcela
    df = pd.DataFrame(parcelas)
    if not df.empty:
        # Agrupa por parcela e data de vencimento, somando os valores recebidos
        df = df.groupby(['Parcela', 'Dt Vencim']).agg({
            'Valor Parcela': 'first',
            'Dt Recebimento': lambda x: x.dropna().iloc[0] if not x.dropna().empty else None,
            'Valor Recebido': 'sum',
            'Status Pagamento': lambda x: 'Pago' if any(s == 'Pago' for s in x) else 'Pendente',
            'Arquivo Origem': 'first'
        }).reset_index()
        
        # Calcula dias de atraso e valor pendente
        df['Dias Atraso'] = df.apply(
            lambda x: (x['Dt Recebimento'] - x['Dt Vencim']).days if x['Dt Recebimento'] and x['Dt Recebimento'] > x['Dt Vencim'] else 0,
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
        cleaned_value = str(value_str).replace('R$', '').strip()
        return float(cleaned_value.replace('.', '').replace(',', '.'))
    except (ValueError, AttributeError):
        return 0.0

def extract_from_excel(excel_file):
    """Extrai dados de arquivos Excel com estrutura similar"""
    try:
        df = pd.read_excel(excel_file, engine='openpyxl')
        
        # Mapeamento completo de colunas alternativas
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
        
        # Normalizar nomes de colunas
        df.columns = [col_mapping.get(col.lower().strip(), col) for col in df.columns]
        
        # Verificação robusta das colunas necessárias
        required_columns = ['Parcela', 'Dt Vencim', 'Valor Parcela', 'Dt Recebimento', 'Valor Recebido']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Colunas obrigatórias não encontradas: {', '.join(missing_columns)}")
        
        # Conversão de tipos com tratamento de erros
        date_columns = ['Dt Vencim', 'Dt Recebimento']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.date
        
        currency_columns = ['Valor Parcela', 'Valor Recebido']
        for col in currency_columns:
            df[col] = df[col].apply(lambda x: parse_currency(str(x)) if pd.notna(x) else 0.0)
        
        # Adiciona colunas complementares
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
