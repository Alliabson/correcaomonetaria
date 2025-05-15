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
    """Extrai dados específicos do PDF com tratamento completo para todos os formatos"""
    parcelas = []
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
                
            lines = text.split('\n')
            in_payment_table = False
            
            for line in lines:
                # Verificação robusta do cabeçalho da tabela
                if ("Parcela" in line and "Dt Vencim" in line and "Valor Parc." in line and 
                    "Dt. Receb." in line and "Vlr da Parcela" in line):
                    in_payment_table = True
                    continue
                    
                if in_payment_table:
                    # Padrão completo para capturar todas as informações da linha
                    pattern = (
                        r'^(?P<parcela>[A-Z]{1,3}\.\d+/\d+)\s+'  # Parcela (PR.1/10)
                        r'(?P<dt_vencim>\d{2}/\d{2}/\d{4})\s+'   # Data vencimento
                        r'(?P<dias_atraso>\d+)?\s*'              # Dias atraso (opcional)
                        r'(?P<valor_parc>[\d.,]+)\s+'            # Valor parcela
                        r'(?P<dt_receb>\d{2}/\d{2}/\d{4})?\s*'   # Data recebimento (opcional)
                        r'(?P<valor_recebido>[\d.,]+)?\s*'       # Valor recebido (opcional)
                        r'(?P<correcao>[\d.,]+)?\s*'             # Correção (opcional)
                        r'(?P<multa>[\d.,]+)?\s*'                # Multa (opcional)
                        r'(?P<juros>[\d.,]+)?\s*'                # Juros (opcional)
                        r'(?P<desconto>[\d.,]+)?\s*'             # Desconto (opcional)
                        r'(?P<corr_atr>[\d.,]+)?\s*'             # Correção atraso (opcional)
                        r'(?P<outros>[\d.,]+)?'                  # Outros (opcional)
                    )
                    
                    match = re.search(pattern, line.strip())
                    if match:
                        try:
                            # Processa os dados capturados
                            parcela = match.group('parcela')
                            
                            dt_vencim = datetime.strptime(
                                match.group('dt_vencim'), '%d/%m/%Y'
                            ).date()
                            
                            valor_parc = float(
                                match.group('valor_parc').replace('.', '').replace(',', '.')
                            )
                            
                            # Trata dados opcionais
                            dt_receb = None
                            if match.group('dt_receb'):
                                dt_receb = datetime.strptime(
                                    match.group('dt_receb'), '%d/%m/%Y'
                                ).date()
                            
                            valor_recebido = 0.0
                            if match.group('valor_recebido'):
                                valor_recebido = float(
                                    match.group('valor_recebido').replace('.', '').replace(',', '.')
                                )
                            
                            # Adiciona campos adicionais
                            dias_atraso = int(match.group('dias_atraso')) if match.group('dias_atraso') else 0
                            
                            # Adiciona à lista de parcelas
                            parcela_info = {
                                'Parcela': parcela,
                                'Dt Vencim': dt_vencim,
                                'Dias Atraso': dias_atraso,
                                'Valor Parcela': valor_parc,
                                'Dt Recebimento': dt_receb,
                                'Valor Recebido': valor_recebido,
                                'Status Pagamento': 'Pago' if valor_recebido > 0 else 'Pendente',
                                'Correcao': float(match.group('correcao').replace('.', '').replace(',', '.')) 
                                           if match.group('correcao') else 0.0,
                                'Multa': float(match.group('multa').replace('.', '').replace(',', '.')) 
                                       if match.group('multa') else 0.0,
                                'Juros': float(match.group('juros').replace('.', '').replace(',', '.')) 
                                       if match.group('juros') else 0.0,
                                'Desconto': float(match.group('desconto').replace('.', '').replace(',', '.')) 
                                           if match.group('desconto') else 0.0,
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
            'pagamento': 'Valor Recebido',
            'dias atraso': 'Dias Atraso',
            'atraso': 'Dias Atraso'
        }
        
        # Normalizar nomes de colunas
        df.columns = [col_mapping.get(col.lower().strip(), col) for col in df.columns]
        
        # Verificação robusta das colunas necessárias
        required_columns = ['Parcela', 'Dt Vencim', 'Valor Parcela']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Colunas obrigatórias não encontradas: {', '.join(missing_columns)}")
        
        # Conversão de tipos com tratamento de erros
        date_columns = ['Dt Vencim', 'Dt Recebimento']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.date
        
        currency_columns = ['Valor Parcela', 'Valor Recebido']
        for col in currency_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: parse_currency(str(x)) if pd.notna(x) else 0.0)
        
        # Adiciona colunas complementares se não existirem
        if 'Status Pagamento' not in df.columns:
            df['Status Pagamento'] = df.apply(
                lambda x: 'Pago' if x.get('Valor Recebido', 0) > 0 else 'Pendente', 
                axis=1
            )
        
        if 'Dias Atraso' not in df.columns:
            df['Dias Atraso'] = 0
        
        if 'Valor Pendente' not in df.columns:
            df['Valor Pendente'] = df['Valor Parcela'] - df.get('Valor Recebido', 0)
        
        df['Arquivo Origem'] = excel_file.name
        
        return df
    
    except Exception as e:
        raise ValueError(f"Erro ao processar arquivo Excel: {str(e)}")
