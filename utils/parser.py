import pandas as pd
import pdfplumber
from datetime import datetime
import re
import logging

# Configura logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_payment_data(file):
    """Função unificada para extrair dados de PDF ou Excel"""
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

def extract_from_pdf(pdf_file):
    """Extrai dados de parcelas de PDFs complexos"""
    parcelas = []
    
    try:
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                    
                lines = text.split('\n')
                in_table = False
                
                for line in lines:
                    if "Parcela" in line and "Dt Vencim" in line and "Valor Parc." in line:
                        in_table = True
                        continue
                    
                    if in_table:
                        if re.match(r'^[A-Z]{1,3}\.\d+/\d+', line.strip()):
                            try:
                                parcela_data = parse_parcela_line(line)
                                if parcela_data:
                                    parcelas.append(parcela_data)
                            except Exception as e:
                                logger.warning(f"Erro ao processar linha: {line}\nErro: {str(e)}")
                                continue
                        
                        if "Total a pagar:" in line:
                            break
        
        df = pd.DataFrame(parcelas)
        
        if not df.empty:
            df['Dias Atraso'] = df.apply(calculate_days_late, axis=1)
            df['Valor Pendente'] = df['Valor Parcela'] - df['Valor Recebido']
            df['Status Pagamento'] = df['Valor Recebido'].apply(
                lambda x: 'Pago' if x > 0 else 'Pendente'
            )
        
        return df
    
    except Exception as e:
        logger.error(f"Erro em extract_from_pdf: {str(e)}")
        raise

def parse_parcela_line(line):
    """Processa uma linha de parcela individual"""
    try:
        # Normaliza espaços e remove caracteres problemáticos
        cleaned = re.sub(r'\s+', ' ', line.strip())
        
        # Divide a linha em componentes
        parts = re.split(r'\s{2,}', cleaned)
        
        # Extrai os dados básicos
        parcela = parts[0]
        dt_vencim = parse_date(parts[1]) if len(parts) > 1 else None
        valor_parc = parse_currency(parts[2]) if len(parts) > 2 else 0
        
        # Encontra dados de recebimento
        dt_receb = None
        valor_recebido = 0
        
        for i, part in enumerate(parts):
            if re.match(r'\d{2}/\d{2}/\d{4}', part):
                dt_receb = parse_date(part)
                if len(parts) > i+1:
                    valor_recebido = parse_currency(parts[i+1])
                break
        
        return {
            'Parcela': parcela,
            'Dt Vencim': dt_vencim,
            'Valor Parcela': valor_parc,
            'Dt Recebimento': dt_receb,
            'Valor Recebido': valor_recebido
        }
    
    except Exception as e:
        logger.error(f"Erro em parse_parcela_line: {str(e)}")
        raise

def calculate_days_late(row):
    """Calcula dias de atraso para uma parcela"""
    try:
        if row['Dt Recebimento'] and row['Dt Vencim']:
            return (row['Dt Recebimento'] - row['Dt Vencim']).days
        return 0
    except Exception as e:
        logger.warning(f"Erro ao calcular dias de atraso: {str(e)}")
        return 0

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
    """Extrai dados de arquivos Excel (implementação simplificada)"""
    try:
        df = pd.read_excel(excel_file, engine='openpyxl')
        
        # Verifica colunas mínimas necessárias
        required_cols = ['Parcela', 'Dt Vencim', 'Valor Parcela']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            raise ValueError(f"Colunas obrigatórias faltando: {missing_cols}")
        
        # Preenche colunas opcionais se não existirem
        if 'Dt Recebimento' not in df.columns:
            df['Dt Recebimento'] = None
        if 'Valor Recebido' not in df.columns:
            df['Valor Recebido'] = 0.0
        
        # Converte tipos
        df['Dt Vencim'] = pd.to_datetime(df['Dt Vencim'], dayfirst=True).dt.date
        df['Dt Recebimento'] = pd.to_datetime(df['Dt Recebimento'], dayfirst=True, errors='coerce').dt.date
        df['Valor Parcela'] = df['Valor Parcela'].apply(parse_currency)
        df['Valor Recebido'] = df['Valor Recebido'].apply(parse_currency)
        
        return df
    
    except Exception as e:
        logger.error(f"Erro em extract_from_excel: {str(e)}")
        raise
