import pandas as pd
import pdfplumber
from datetime import datetime
import re

def extract_payment_data(file):
    """Função unificada para extrair dados de PDF ou Excel"""
    if file.name.lower().endswith('.pdf'):
        return extract_from_pdf_with_tables(file)
    elif file.name.lower().endswith(('.xls', '.xlsx')):
        return extract_from_excel(file)
    else:
        raise ValueError("Formato de arquivo não suportado")

def extract_from_pdf_with_tables(pdf_file):
    """Extrai dados usando análise de tabelas do PDFplumber com tratamento robusto de datas"""
    parcelas = []
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            # Tenta extrair tabelas
            tables = page.extract_tables({
                "vertical_strategy": "text", 
                "horizontal_strategy": "text",
                "intersection_y_tolerance": 10
            })
            
            for table in tables:
                for row_idx, row in enumerate(table):
                    # Verifica se a linha parece ser uma parcela
                    if len(row) > 2 and row[0] and re.match(r'^(E|P|B)\.\d+/\d+', str(row[0]).strip()):
                        try:
                            # Processa a linha da tabela
                            parcela = str(row[0]).strip()
                            
                            # Data de vencimento (obrigatória)
                            dt_vencim = parse_date(str(row[1]))
                            if not dt_vencim:
                                print(f"Aviso: Data de vencimento inválida na linha {row_idx}: {row}")
                                continue
                            
                            # Valor da parcela (obrigatório)
                            valor_parc = parse_currency(str(row[2]))
                            
                            # Data e valor de recebimento (opcionais)
                            dt_receb = None
                            valor_recebido = 0.0
                            status = 'Pendente'
                            
                            # Procura por data de recebimento
                            for i in range(3, len(row)):
                                cell = str(row[i]).strip()
                                if not cell:
                                    continue
                                    
                                # Verifica se é uma data
                                temp_date = parse_date(cell)
                                if temp_date:
                                    dt_receb = temp_date
                                    # O valor recebido geralmente vem após a data
                                    if i+1 < len(row):
                                        valor_recebido = parse_currency(str(row[i+1]))
                                        status = 'Pago' if valor_recebido > 0 else 'Pendente'
                                    break
                            
                            # Adiciona à lista de parcelas
                            parcela_info = {
                                'Parcela': parcela,
                                'Dt Vencim': dt_vencim,
                                'Valor Parcela': valor_parc,
                                'Dt Recebimento': dt_receb,
                                'Valor Recebido': valor_recebido,
                                'Status Pagamento': status,
                                'Arquivo Origem': pdf_file.name
                            }
                            parcelas.append(parcela_info)
                            
                        except Exception as e:
                            print(f"Erro ao processar linha {row_idx}: {row}. Erro: {str(e)}")
                            continue
    
    # Cria DataFrame com tratamento seguro para datas
    df = pd.DataFrame(parcelas)
    
    if not df.empty:
        # Calcula dias de atraso (apenas para parcelas pagas com data válida)
        df['Dias Atraso'] = df.apply(
            lambda x: (x['Dt Recebimento'] - x['Dt Vencim']).days 
            if pd.notnull(x['Dt Recebimento']) and x['Dt Recebimento'] > x['Dt Vencim']
            else 0,
            axis=1
        )
        
        # Calcula valor pendente
        df['Valor Pendente'] = df['Valor Parcela'] - df['Valor Recebido']
    
    return df

def parse_date(date_str):
    """Converte string de data no formato DD/MM/YYYY para objeto date com tratamento robusto"""
    if not date_str or str(date_str).lower() in ['nan', 'none', '']:
        return None
    
    try:
        # Remove possíveis espaços e caracteres estranhos
        date_str = str(date_str).strip()
        
        # Tenta parsear no formato DD/MM/YYYY
        try:
            return datetime.strptime(date_str, '%d/%m/%Y').date()
        except ValueError:
            pass
        
        # Tenta outros formatos comuns
        formats = ['%d-%m-%Y', '%Y-%m-%d', '%m/%d/%Y', '%d.%m.%Y']
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
                
        return None
    except Exception:
        return None

def parse_currency(value_str):
    """Converte valores monetários com tratamento robusto"""
    try:
        if pd.isna(value_str) or not str(value_str).strip():
            return 0.0
            
        value_str = str(value_str).strip()
        
        # Remove caracteres não numéricos exceto vírgula, ponto e sinal negativo
        cleaned = re.sub(r'[^\d,-.]', '', value_str)
        
        # Caso tenha tanto ponto quanto vírgula (1.234,56)
        if '.' in cleaned and ',' in cleaned:
            cleaned = cleaned.replace('.', '').replace(',', '.')
        # Caso tenha apenas vírgula (1234,56)
        elif ',' in cleaned:
            cleaned = cleaned.replace(',', '.')
        # Caso tenha apenas ponto (1234.56 ou 1.234)
        elif '.' in cleaned:
            parts = cleaned.split('.')
            if len(parts) > 1 and len(parts[-1]) == 3:  # Verifica se é separador de milhar
                cleaned = cleaned.replace('.', '')
        
        return float(cleaned)
    except Exception:
        return 0.0

def extract_from_excel(excel_file):
    """Extrai dados de arquivos Excel com tratamento seguro de datas"""
    try:
        df = pd.read_excel(excel_file, engine='openpyxl')
        
        # Mapeamento de colunas
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
        
        # Normaliza nomes de colunas
        df.columns = [col_mapping.get(str(col).lower().strip(), col) for col in df.columns]
        
        # Verifica colunas obrigatórias
        required_columns = ['Parcela', 'Dt Vencim', 'Valor Parcela']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Colunas obrigatórias não encontradas: {', '.join(missing_columns)}")
        
        # Converte datas - tratamento seguro
        date_columns = ['Dt Vencim', 'Dt Recebimento']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.date
        
        # Converte valores monetários - tratamento seguro
        currency_columns = ['Valor Parcela', 'Valor Recebido']
        for col in currency_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: parse_currency(str(x)) if pd.notna(x) else 0.0)
        
        # Define status de pagamento
        if 'Valor Recebido' in df.columns and 'Dt Recebimento' in df.columns:
            df['Status Pagamento'] = df.apply(
                lambda x: 'Pago' if pd.notnull(x['Dt Recebimento']) and x['Valor Recebido'] > 0 
                else 'Pendente', 
                axis=1
            )
        else:
            df['Status Pagamento'] = 'Pendente'
        
        # Calcula dias de atraso (apenas para parcelas pagas)
        if 'Dt Recebimento' in df.columns and 'Dt Vencim' in df.columns:
            df['Dias Atraso'] = df.apply(
                lambda x: (x['Dt Recebimento'] - x['Dt Vencim']).days 
                if pd.notnull(x['Dt Recebimento']) and pd.notnull(x['Dt Vencim']) 
                   and x['Dt Recebimento'] > x['Dt Vencim']
                else 0,
                axis=1
            )
        else:
            df['Dias Atraso'] = 0
        
        # Calcula valor pendente
        if 'Valor Parcela' in df.columns and 'Valor Recebido' in df.columns:
            df['Valor Pendente'] = df['Valor Parcela'] - df['Valor Recebido']
        else:
            df['Valor Pendente'] = df['Valor Parcela']
        
        df['Arquivo Origem'] = excel_file.name
        
        return df
    
    except Exception as e:
        raise ValueError(f"Erro ao processar arquivo Excel: {str(e)}")
