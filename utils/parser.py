import pdfplumber
import pandas as pd
from datetime import datetime
import re
from typing import Dict, List, Optional
import locale

class PDFParser:
    def __init__(self):
        # Configura locale para formatação de valores
        try:
            locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
        except:
            locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil.1252')

    def extract_data(self, pdf_file) -> Dict:
        """Extrai todos os dados do PDF de forma estruturada"""
        result = {
            'client_data': {},
            'payment_data': [],
            'yearly_totals': {},
            'summary': {}
        }

        with pdfplumber.open(pdf_file) as pdf:
            full_text = ""
            
            for page in pdf.pages:
                # Extrai texto e tabelas de cada página
                page_text = page.extract_text()
                full_text += page_text + "\n\n"
                
                # Processa tabelas (se existirem)
                tables = page.extract_tables({
                    "vertical_strategy": "text", 
                    "horizontal_strategy": "text",
                    "intersection_y_tolerance": 10
                })
                
                for table in tables:
                    if len(table) > 1 and any("Parcela" in str(cell) for row in table for cell in row):
                        self._process_table(table, result)

            # Processa o texto completo para extrair informações adicionais
            self._process_full_text(full_text, result)

        return result

    def _process_table(self, table: List[List[str]], result: Dict):
        """Processa uma tabela encontrada no PDF"""
        # Identifica cabeçalhos
        headers = []
        data_start = 0
        
        for i, row in enumerate(table):
            if any("Parcela" in str(cell) for cell in row):
                headers = [str(cell).strip() for cell in row]
                data_start = i + 1
                break
        
        if not headers:
            return

        # Processa linhas de dados
        for row in table[data_start:]:
            if not row or len(row) < len(headers):
                continue
                
            payment = {}
            for i, header in enumerate(headers):
                if i >= len(row):
                    continue
                    
                value = str(row[i]).strip()
                
                # Processa campos especiais
                if "Data" in header or "Dt" in header or "Vencim" in header:
                    payment[header] = self._parse_date(value)
                elif "Valor" in header or "Vlr" in header or "Multa" in header or "Juros" in header:
                    payment[header] = self._parse_currency(value)
                else:
                    payment[header] = value
            
            if payment:
                result['payment_data'].append(payment)

    def _process_full_text(self, text: str, result: Dict):
        """Extrai informações adicionais do texto completo"""
        # Extrai dados do cliente
        client_data = {
            'name': self._extract_value(text, 'Cliente :', 'Venda:'),
            'contract_number': self._extract_value(text, 'Venda:', 'Dt. Venda:'),
            'contract_date': self._extract_value(text, 'Dt. Venda:', 'Empreend.:'),
            'project': self._extract_value(text, 'Empreend.:', 'End.:'),
            'address': self._extract_value(text, 'End.:', 'Cidade :'),
            'city': self._extract_value(text, 'Cidade :', 'UF :'),
            'state': self._extract_value(text, 'UF :', 'CEP :'),
            'zip_code': self._extract_value(text, 'CEP :', 'Fones:'),
            'phone': self._extract_value(text, 'Fones:', 'Valor da venda:'),
            'contract_value': self._extract_value(text, 'Valor da venda:', 'Status da venda:'),
            'status': self._extract_value(text, 'Status da venda:', 'Titular')
        }
        result['client_data'] = client_data

        # Extrai totais por ano
        yearly_totals = {}
        for year_match in re.finditer(r'Ano:\s*(\d{4}).*?Total pago:\s*([\d.,]+).*?Total a pagar:\s*([\d.,]+)', text, re.DOTALL):
            year = year_match.group(1)
            yearly_totals[year] = {
                'paid': self._parse_currency(year_match.group(2)),
                'to_pay': self._parse_currency(year_match.group(3))
            }
        result['yearly_totals'] = yearly_totals

        # Extrai resumo financeiro
        summary = {
            'total_received': self._extract_value(text, 'RECEBIDO :', 'RECEBER'),
            'total_to_receive': self._extract_value(text, 'RECEBER \+ RESID\. :', 'GERAL'),
            'paid_percentage': self._extract_value(text, '% Recebida :', '% A Receber'),
            'to_receive_percentage': self._extract_value(text, '% A Receber :', '\n')
        }
        result['summary'] = summary

    def _extract_value(self, text: str, start_marker: str, end_marker: str) -> str:
        """Extrai valor entre marcadores"""
        start = re.search(start_marker, text)
        if not start:
            return ""
            
        end = re.search(end_marker, text[start.end():])
        if not end:
            return text[start.end():].strip()
            
        return text[start.end():start.end()+end.start()].strip()

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Converte string de data para formato padronizado"""
        try:
            if not date_str or date_str.lower() == 'nan':
                return None
                
            date_str = re.sub(r'[^\d/]', '', date_str)
            
            for fmt in ('%d/%m/%Y', '%d%m/%Y', '%Y-%m-%d'):
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime('%d/%m/%Y')
                except ValueError:
                    continue
            return None
        except:
            return None

    def _parse_currency(self, value_str: str) -> float:
        """Converte valores monetários para float"""
        try:
            if not value_str:
                return 0.0
                
            # Remove caracteres não numéricos exceto vírgula e ponto
            cleaned = re.sub(r'[^\d,.-]', '', value_str)
            
            # Caso tenha tanto ponto quanto vírgula (1.234,56)
            if '.' in cleaned and ',' in cleaned:
                if cleaned.index(',') > cleaned.index('.'):  # 1.234,56
                    cleaned = cleaned.replace('.', '').replace(',', '.')
                else:  # 1,234.56
                    cleaned = cleaned.replace(',', '')
            # Caso tenha apenas vírgula (1234,56)
            elif ',' in cleaned:
                cleaned = cleaned.replace(',', '.')
            
            return float(cleaned)
        except:
            return 0.0

    def to_dataframe(self, data: Dict) -> pd.DataFrame:
        """Converte os dados extraídos para DataFrame"""
        if not data.get('payment_data'):
            return pd.DataFrame()
            
        # Padroniza nomes de colunas
        column_mapping = {
            'Parcela': 'installment',
            'Dt Vencim': 'due_date',
            'Valor Parc.': 'installment_value',
            'Dt. Receb.': 'payment_date',
            'Vlr da Parcela': 'total_value',
            'Correção': 'correction',
            'Multa': 'fine',
            'Juros Atr.': 'interest',
            'Desconto': 'discount',
            'Atraso': 'days_late'
        }
        
        # Cria DataFrame com os dados mapeados
        df_data = []
        for payment in data['payment_data']:
            row = {}
            for pdf_col, df_col in column_mapping.items():
                if pdf_col in payment:
                    row[df_col] = payment[pdf_col]
            
            # Calcula campos derivados
            if 'due_date' in row and 'payment_date' in row:
                try:
                    due_date = datetime.strptime(row['due_date'], '%d/%m/%Y')
                    pay_date = datetime.strptime(row['payment_date'], '%d/%m/%Y')
                    row['days_late'] = (pay_date - due_date).days if pay_date > due_date else 0
                except:
                    row['days_late'] = 0
            
            if 'installment_value' in row and 'total_value' in row:
                row['pending_value'] = row['installment_value'] - row['total_value']
            
            df_data.append(row)
        
        return pd.DataFrame(df_data)
