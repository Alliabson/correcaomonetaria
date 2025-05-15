import pdfplumber
import pandas as pd
from datetime import datetime
import re
from typing import List, Dict, Optional
import streamlit as st

def extract_pdf_data(pdf_file, selected_columns: List[str]) -> pd.DataFrame:
    """Extrai dados de PDF com seleção de colunas"""
    all_data = []
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue

            # Tenta extrair tabelas
            tables = page.extract_tables()
            if tables:
                for table in tables:
                    for row in table:
                        if len(row) >= 3:  # Mínimo de colunas
                            parsed = parse_table_row(row, selected_columns)
                            if parsed:
                                all_data.append(parsed)
            else:
                # Fallback para extração por texto
                lines = text.split('\n')
                for line in lines:
                    parsed = parse_text_line(line, selected_columns)
                    if parsed:
                        all_data.append(parsed)

    return pd.DataFrame(all_data) if all_data else pd.DataFrame()

def parse_table_row(row: List[str], selected_columns: List[str]) -> Optional[Dict]:
    """Processa uma linha de tabela do PDF"""
    try:
        data = {}
        
        # Mapeamento básico de colunas (adaptar conforme seu PDF)
        column_mapping = {
            'Parcela': 0,
            'Dt Vencim': 1,
            'Valor Parc.': 2,
            'Dt. Receb.': 3,
            'Vlr da Parcela': 4,
            'Correção': 5,
            'Multa': 6,
            'Juros Atr.': 7,
            'Desconto': 8,
            'Atraso': 9
        }
        
        for col in selected_columns:
            idx = column_mapping.get(col, -1)
            if idx < len(row) and idx >= 0:
                value = row[idx].strip()
                
                # Conversão especial para alguns campos
                if 'Dt' in col:
                    data[col] = parse_date(value)
                elif 'Valor' in col or 'Vlr' in col or 'Multa' in col or 'Juros' in col:
                    data[col] = parse_currency(value)
                elif 'Atraso' in col:
                    data[col] = int(value) if value.isdigit() else 0
                else:
                    data[col] = value
        
        return data if data else None
    
    except Exception as e:
        st.warning(f"Erro ao processar linha: {row}\nErro: {str(e)}")
        return None

def parse_text_line(line: str, selected_columns: List[str]) -> Optional[Dict]:
    """Processa uma linha de texto do PDF"""
    try:
        # Padrão para identificar linhas de parcelas
        if not re.match(r'^[A-Z]{1,3}\.\d+/\d+', line.strip()):
            return None
        
        # Limpa e divide a linha
        cleaned = re.sub(r'\s+', ' ', line.strip())
        parts = re.split(r'\s{2,}', cleaned)
        
        data = {}
        # Mapeamento simplificado (ajustar conforme necessário)
        if len(parts) >= 3:
            data['Parcela'] = parts[0]
            data['Dt Vencim'] = parse_date(parts[1])
            data['Valor Parc.'] = parse_currency(parts[2])
            
            if len(parts) >= 4 and 'Dt. Receb.' in selected_columns:
                data['Dt. Receb.'] = parse_date(parts[3])
            
            if len(parts) >= 5 and 'Vlr da Parcela' in selected_columns:
                data['Vlr da Parcela'] = parse_currency(parts[4])
        
        return {k: v for k, v in data.items() if k in selected_columns}
    
    except Exception as e:
        st.warning(f"Erro ao processar linha: {line}\nErro: {str(e)}")
        return None

def parse_date(date_str: str) -> Optional[datetime.date]:
    """Converte string de data para objeto date"""
    try:
        if not date_str:
            return None
            
        for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d'):
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        return None
    except Exception:
        return None

def parse_currency(value_str: str) -> float:
    """Converte valores monetários para float"""
    try:
        if not value_str:
            return 0.0
            
        cleaned = re.sub(r'[^\d.,]', '', str(value_str).strip())
        
        if ',' in cleaned and '.' in cleaned:
            if cleaned.index(',') > cleaned.index('.'):  # 1.000,00
                cleaned = cleaned.replace('.', '').replace(',', '.')
            else:  # 1,000.00
                cleaned = cleaned.replace(',', '')
        elif ',' in cleaned:
            if len(cleaned.split(',')[-1]) == 2:  # 1,00 (centavos)
                cleaned = cleaned.replace('.', '').replace(',', '.')
            else:  # 1,000
                cleaned = cleaned.replace(',', '')
        
        return float(cleaned)
    except Exception:
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
