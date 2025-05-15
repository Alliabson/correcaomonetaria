import pandas as pd
import pdfplumber
from datetime import datetime
import re

def extract_from_pdf(pdf_file):
    """Extrai dados de parcelas de PDFs complexos com tabelas mal formatadas"""
    parcelas = []
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
                
            lines = text.split('\n')
            in_table = False
            header_found = False
            
            for line in lines:
                # Verifica se é o cabeçalho da tabela
                if ("Parcela" in line and "Dt Vencim" in line and 
                    "Valor Parc." in line and "Vlr da Parcela" in line):
                    in_table = True
                    header_found = True
                    continue
                
                if in_table:
                    # Padrão para identificar linhas de parcelas
                    parcela_match = re.match(r'^([A-Z]{1,3}\.\d+/\d+)', line.strip())
                    if parcela_match:
                        try:
                            # Processa a linha da parcela
                            parcela_data = process_parcela_line(line)
                            if parcela_data:
                                parcelas.append(parcela_data)
                        except Exception as e:
                            print(f"Erro ao processar linha: {line}\nErro: {str(e)}")
                            continue
                    
                    # Verifica fim da tabela
                    if any(x in line for x in ["Total a pagar:", "Ano:", "TOTAL GERAL:"]):
                        in_table = False
    
    # Cria DataFrame
    df = pd.DataFrame(parcelas)
    
    if not df.empty:
        # Calcula campos adicionais
        df['Dias Atraso'] = df.apply(
            lambda x: (x['Dt Recebimento'] - x['Dt Vencim']).days 
            if x['Dt Recebimento'] and x['Dt Recebimento'] > x['Dt Vencim'] 
            else 0,
            axis=1
        )
        df['Valor Pendente'] = df['Valor Parcela'] - df['Valor Recebido']
        df['Status Pagamento'] = df['Valor Recebido'].apply(
            lambda x: 'Pago' if x > 0 else 'Pendente'
        )
    
    return df if not df.empty else pd.DataFrame()

def process_parcela_line(line):
    """Processa uma linha de parcela individual"""
    # Normaliza espaços e remove caracteres problemáticos
    cleaned = re.sub(r'\s+', ' ', line.strip())
    
    # Extrai os componentes principais
    parts = re.split(r'\s{2,}', cleaned)  # Divide por múltiplos espaços
    
    # Padrão para identificar parcelas (E.1/1, P.1/144, B.1/12, etc.)
    parcela_pattern = r'^([A-Z]{1,3}\.\d+/\d+)'
    parcela_match = re.match(parcela_pattern, parts[0])
    if not parcela_match:
        return None
    
    # Extrai os dados básicos
    parcela = parcela_match.group(1)
    dt_vencim = parse_date(parts[1]) if len(parts) > 1 else None
    valor_parc = parse_currency(parts[2]) if len(parts) > 2 else 0
    
    # Encontra a data de recebimento (pode estar em posições variáveis)
    dt_receb = None
    valor_recebido = 0
    
    for i, part in enumerate(parts):
        if re.match(r'\d{2}/\d{2}/\d{4}', part):
            # Assume que após a data de receb vem o valor recebido
            dt_receb = parse_date(part)
            if len(parts) > i+1:
                valor_recebido = parse_currency(parts[i+1])
            break
    
    return {
        'Parcela': parcela,
        'Dt Vencim': dt_vencim,
        'Valor Parcela': valor_parc,
        'Dt Recebimento': dt_receb,
        'Valor Recebido': valor_recebido,
        'Arquivo Origem': 'PDF'
    }

def parse_date(date_str):
    """Converte string de data para objeto date"""
    try:
        return datetime.strptime(date_str, '%d/%m/%Y').date()
    except (ValueError, TypeError):
        return None

def parse_currency(value_str):
    """Converte valores monetários para float"""
    try:
        cleaned = str(value_str).replace('R$', '').replace('.', '').replace(',', '.').strip()
        return float(cleaned)
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
