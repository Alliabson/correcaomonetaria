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
    """Extrai dados específicos do PDF com foco nas colunas Valor Parc. e Vlr. da Parcela"""
    parcelas = []
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            lines = text.split('\n')
            
            in_payment_table = False
            
            for line in lines:
                # Verifica se é o cabeçalho da tabela de pagamentos
                if "Parcela" in line and "Dt Vencim" in line and "Valor Parc." in line:
                    in_payment_table = True
                    continue
                
                # Processa linhas dentro da tabela de pagamentos
                if in_payment_table and line.strip():
                    # Padrão para identificar linhas de parcelas (E. ou P.)
                    if re.match(r'^(E|P|B)\.\d+/\d+', line.strip()):
                        try:
                            # Divide a linha em partes - tratamento especial para a estrutura
                            parts = re.split(r'\s{2,}', line.strip())
                            
                            # Extrai informações básicas
                            parcela = parts[0]
                            dt_vencim = datetime.strptime(parts[1], '%d/%m/%Y').date()
                            valor_parc = parse_currency(parts[2])
                            
                            # Verifica se há data de recebimento e valor recebido
                            dt_receb = None
                            valor_recebido = 0.0
                            
                            if len(parts) > 4 and '/' in parts[3]:
                                dt_receb = datetime.strptime(parts[3], '%d/%m/%Y').date()
                                valor_recebido = parse_currency(parts[4])
                            
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
                if in_payment_table and "Total a pagar:" in line:
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

def parse_currency(value_str):
    """Converte valores monetários com vírgula decimal para float"""
    try:
        if isinstance(value_str, (int, float)):
            return float(value_str)
            
        cleaned_value = str(value_str).replace('R$', '').strip()
        # Remove pontos como separador de milhar e substitui vírgula decimal por ponto
        cleaned_value = cleaned_value.replace('.', '').replace(',', '.')
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
