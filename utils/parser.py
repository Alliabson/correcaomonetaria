import pandas as pd
import pdfplumber
from datetime import datetime
import re
import logging
from typing import Union, Dict, List, Optional

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_payment_data(file) -> pd.DataFrame:
    """Função principal para extração de dados de arquivos PDF ou Excel"""
    try:
        if file.name.lower().endswith('.pdf'):
            return extract_from_pdf(file)
        elif file.name.lower().endswith(('.xls', '.xlsx')):
            return extract_from_excel(file)
        else:
            raise ValueError("Formato de arquivo não suportado")
    except Exception as e:
        logger.error(f"Erro em extract_payment_data: {str(e)}")
        raise

def extract_from_pdf(pdf_file) -> pd.DataFrame:
    """Extrai dados de parcelas de PDFs com estrutura complexa"""
    all_parcelas = []
    
    try:
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                
                # Processa tanto tabelas quanto texto livre
                table_data = extract_from_table(page)
                text_data = extract_from_text(text)
                
                if table_data:
                    all_parcelas.extend(table_data)
                if text_data:
                    all_parcelas.extend(text_data)
        
        df = create_dataframe(all_parcelas)
        return df
    
    except Exception as e:
        logger.error(f"Erro em extract_from_pdf: {str(e)}")
        raise

def extract_from_table(page) -> List[Dict]:
    """Extrai dados de tabelas no PDF"""
    parcelas = []
    
    try:
        # Tenta extrair tabelas usando pdfplumber
        for table in page.extract_tables():
            for row in table:
                if len(row) >= 5:  # Número mínimo de colunas esperadas
                    parcela_data = parse_table_row(row)
                    if parcela_data:
                        parcelas.append(parcela_data)
    except Exception as e:
        logger.warning(f"Erro ao extrair tabelas: {str(e)}")
    
    return parcelas

def extract_from_text(text: str) -> List[Dict]:
    """Extrai dados do texto livre do PDF"""
    parcelas = []
    lines = text.split('\n')
    in_table = False
    
    for line in lines:
        # Verifica início da tabela
        if not in_table and ("Parcela" in line and "Dt Vencim" in line and "Valor Parc." in line):
            in_table = True
            continue
        
        if in_table:
            # Verifica fim da tabela
            if any(x in line for x in ["Total a pagar:", "TOTAL GERAL:", "Ano:"]):
                break
            
            # Processa linha de parcela
            parcela_data = parse_text_line(line)
            if parcela_data:
                parcelas.append(parcela_data)
    
    return parcelas

def parse_table_row(row: List[str]) -> Optional[Dict]:
    """Processa uma linha de tabela do PDF"""
    try:
        # Verifica se é uma linha válida
        if not row[0] or not re.match(r'^[A-Z]{1,3}\.\d+/\d+', row[0].strip()):
            return None
        
        return {
            'Parcela': row[0].strip(),
            'Dt Vencim': parse_date(row[1]),
            'Valor Parcela': parse_currency(row[2]),
            'Dt Recebimento': parse_date(row[3]),
            'Valor Recebido': parse_currency(row[4]) if len(row) > 4 else 0,
            'Correção': parse_currency(row[5]) if len(row) > 5 else 0,
            'Multa': parse_currency(row[6]) if len(row) > 6 else 0,
            'Juros Atr.': parse_currency(row[7]) if len(row) > 7 else 0,
            'Desconto': parse_currency(row[8]) if len(row) > 8 else 0,
            'Atraso': int(row[9]) if len(row) > 9 and row[9].isdigit() else 0
        }
    except Exception as e:
        logger.warning(f"Erro ao processar linha da tabela: {row}. Erro: {str(e)}")
        return None

def parse_text_line(line: str) -> Optional[Dict]:
    """Processa uma linha de texto do PDF"""
    try:
        # Padrão para identificar parcelas (E.1/1, P.1/144, B.1/12, etc.)
        if not re.match(r'^[A-Z]{1,3}\.\d+/\d+', line.strip()):
            return None
        
        # Limpa e divide a linha
        cleaned = re.sub(r'\s+', ' ', line.strip())
        parts = re.split(r'\s{2,}', cleaned)
        
        # Extrai dados básicos
        parcela = parts[0]
        dt_vencim = parse_date(parts[1]) if len(parts) > 1 else None
        valor_parc = parse_currency(parts[2]) if len(parts) > 2 else 0
        
        # Encontra dados adicionais
        dt_receb = None
        valor_recebido = 0
        correcao = 0
        multa = 0
        juros = 0
        desconto = 0
        atraso = 0
        
        for i, part in enumerate(parts):
            if re.match(r'\d{2}/\d{2}/\d{4}', part):
                dt_receb = parse_date(part)
                if len(parts) > i+1:
                    valor_recebido = parse_currency(parts[i+1])
            
            if 'Correção' in part or 'Corr.' in part:
                correcao = parse_currency(parts[i+1] if len(parts) > i+1 else 0)
            
            if 'Multa' in part:
                multa = parse_currency(parts[i+1] if len(parts) > i+1 else 0)
            
            if 'Juros' in part:
                juros = parse_currency(parts[i+1] if len(parts) > i+1 else 0)
            
            if 'Desconto' in part:
                desconto = parse_currency(parts[i+1] if len(parts) > i+1 else 0)
            
            if 'Atraso' in part and part.replace('Atraso', '').strip().isdigit():
                atraso = int(part.replace('Atraso', '').strip())
        
        return {
            'Parcela': parcela,
            'Dt Vencim': dt_vencim,
            'Valor Parcela': valor_parc,
            'Dt Recebimento': dt_receb,
            'Valor Recebido': valor_recebido,
            'Correção': correcao,
            'Multa': multa,
            'Juros Atr.': juros,
            'Desconto': desconto,
            'Atraso': atraso
        }
    
    except Exception as e:
        logger.warning(f"Erro ao processar linha de texto: {line}. Erro: {str(e)}")
        return None

def create_dataframe(parcelas: List[Dict]) -> pd.DataFrame:
    """Cria DataFrame com tratamento completo dos dados"""
    if not parcelas:
        return pd.DataFrame()
    
    df = pd.DataFrame(parcelas)
    
    # Calcula campos derivados
    df['Dias Atraso'] = df.apply(
        lambda x: (x['Dt Recebimento'] - x['Dt Vencim']).days 
        if pd.notnull(x['Dt Recebimento']) and x['Dt Recebimento'] > x['Dt Vencim'] 
        else 0,
        axis=1
    )
    
    df['Valor Pendente'] = df['Valor Parcela'] - df['Valor Recebido']
    df['Status Pagamento'] = df.apply(
        lambda x: 'Pago' if x['Valor Recebido'] > 0 else 'Pendente',
        axis=1
    )
    
    # Ordena por data de vencimento
    if 'Dt Vencim' in df.columns:
        df = df.sort_values('Dt Vencim')
    
    return df

def parse_date(date_str: str) -> Optional[datetime.date]:
    """Converte string de data para objeto date"""
    try:
        if not date_str or str(date_str).lower() == 'nan':
            return None
        
        # Remove possíveis espaços ou caracteres estranhos
        date_str = str(date_str).strip()
        
        # Tenta vários formatos de data
        for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d'):
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        return None
    except Exception as e:
        logger.warning(f"Erro ao converter data: {date_str}. Erro: {str(e)}")
        return None

def parse_currency(value_str: str) -> float:
    """Converte valores monetários para float"""
    try:
        if not value_str or str(value_str).lower() == 'nan':
            return 0.0
        
        # Remove caracteres não numéricos (exceto vírgula e ponto)
        cleaned = re.sub(r'[^\d.,]', '', str(value_str).strip())
        
        # Verifica se tem vírgula como separador decimal
        if ',' in cleaned and '.' in cleaned:
            if cleaned.index(',') > cleaned.index('.'):  # 1.000,00
                cleaned = cleaned.replace('.', '').replace(',', '.')
            else:  # 1,000.00
                cleaned = cleaned.replace(',', '')
        elif ',' in cleaned:  # 1,000 ou 1,00
            if len(cleaned.split(',')[-1]) == 2:  # 1,00 (centavos)
                cleaned = cleaned.replace('.', '').replace(',', '.')
            else:  # 1,000
                cleaned = cleaned.replace(',', '')
        
        return float(cleaned)
    except Exception as e:
        logger.warning(f"Erro ao converter valor monetário: {value_str}. Erro: {str(e)}")
        return 0.0

def extract_from_excel(excel_file) -> pd.DataFrame:
    """Extrai dados de arquivos Excel com estrutura similar ao PDF"""
    try:
        df = pd.read_excel(excel_file, engine='openpyxl')
        
        # Mapeamento de colunas alternativas
        col_mapping = {
            'parcela': 'Parcela',
            'numero_parcela': 'Parcela',
            'dt_vencim': 'Dt Vencim',
            'data_vencimento': 'Dt Vencim',
            'valor_parc': 'Valor Parcela',
            'valor_parcela': 'Valor Parcela',
            'dt_receb': 'Dt Recebimento',
            'data_recebimento': 'Dt Recebimento',
            'valor_recebido': 'Valor Recebido',
            'vlr_recebido': 'Valor Recebido',
            'correcao': 'Correção',
            'multa': 'Multa',
            'juros': 'Juros Atr.',
            'desconto': 'Desconto',
            'dias_atraso': 'Atraso'
        }
        
        # Normaliza nomes de colunas
        df.columns = [col_mapping.get(col.lower().strip(), col) for col in df.columns]
        
        # Verifica colunas obrigatórias
        required_cols = ['Parcela', 'Dt Vencim', 'Valor Parcela']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            raise ValueError(f"Colunas obrigatórias faltando: {missing_cols}")
        
        # Converte tipos de dados
        date_cols = ['Dt Vencim', 'Dt Recebimento']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.date
        
        currency_cols = ['Valor Parcela', 'Valor Recebido', 'Correção', 'Multa', 'Juros Atr.', 'Desconto']
        for col in currency_cols:
            if col in df.columns:
                df[col] = df[col].apply(parse_currency)
        
        # Preence valores faltantes
        if 'Valor Recebido' not in df.columns:
            df['Valor Recebido'] = 0.0
        if 'Atraso' not in df.columns:
            df['Atraso'] = 0
        
        return create_dataframe(df.to_dict('records'))
    
    except Exception as e:
        logger.error(f"Erro em extract_from_excel: {str(e)}")
        raise
