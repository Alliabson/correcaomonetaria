import pandas as pd
import pdfplumber
import re
from datetime import datetime
from typing import List, Dict, Optional
import streamlit as st

class PDFParser:
    def __init__(self):
        self.parcelas = []
        self.cliente = {}
        self.venda = {}
    
    def extract_from_pdf(self, pdf_file) -> pd.DataFrame:
        """Extrai dados de parcelas de PDFs brasileiros"""
        
        try:
            with pdfplumber.open(pdf_file) as pdf:
                full_text = ""
                
                # Extrair texto de todas as páginas
                for page_num, page in enumerate(pdf.pages):
                    text = page.extract_text()
                    if text:
                        full_text += text + "\n"
                    
                    # Também tentar extrair tabelas
                    try:
                        tables = page.extract_tables()
                        for table in tables:
                            if table and len(table) > 1:
                                self._process_table(table)
                    except:
                        pass
                
                # Processar o texto completo
                self._process_text(full_text)
                
                # Se não encontrou parcelas no texto, tentar métodos alternativos
                if not self.parcelas:
                    self._try_alternative_parsing(pdf)
                
                return self._create_dataframe()
                
        except Exception as e:
            st.error(f"Erro ao processar PDF: {str(e)}")
            return pd.DataFrame()
    
    def _process_text(self, text: str):
        """Processa o texto extraído do PDF"""
        
        # Padrões comuns em documentos brasileiros
        padroes = [
            # Padrão 1: "PR.01/12 15/01/2023 1.500,00"
            r'(PR\.\d+/\d+|PARCELA\s+\d+|\d+/\d+)\s+(\d{2}/\d{2}/\d{4})\s+([\d\.]+,\d{2})',
            
            # Padrão 2: "01/12 15/01/2023 R$ 1.500,00"
            r'(\d+/\d+)\s+(\d{2}/\d{2}/\d{4})\s+R\$\s*([\d\.]+,\d{2})',
            
            # Padrão 3: "Parcela 01 15/01/2023 1500,00"
            r'Parcela\s+(\d+)\s+(\d{2}/\d{2}/\d{4})\s+([\d\.]+,\d{2})',
            
            # Padrão 4: Linhas com datas e valores
            r'(\d{2}/\d{2}/\d{4})\s+([\d\.]+,\d{2})\s+(\d{2}/\d{2}/\d{4})?\s*([\d\.]+,\d{2})?',
            
            # Padrão 5: Com descrição "Prestação"
            r'(Prestação|PRESTAÇÃO)\s+[^0-9]*(\d{2}/\d{2}/\d{4})[^0-9]*([\d\.]+,\d{2})'
        ]
        
        # Procurar por informações do cliente
        self._extract_cliente_info(text)
        
        # Procurar parcelas com múltiplos padrões
        for padrao in padroes:
            matches = re.finditer(padrao, text, re.IGNORECASE)
            for match in matches:
                self._process_match(match, padrao)
    
    def _process_match(self, match, padrao_used: str):
        """Processa um match encontrado"""
        try:
            groups = match.groups()
            
            # Diferentes padrões têm grupos em posições diferentes
            if "PR\." in padrao_used or "PARCELA" in padrao_used:
                # Padrão 1, 2 ou 3
                codigo = groups[0]
                data_str = groups[1]
                valor_str = groups[2]
            elif "Prestação" in padrao_used:
                # Padrão 5
                codigo = groups[0]
                data_str = groups[1]
                valor_str = groups[2]
            else:
                # Padrão 4 (mais genérico)
                if len(groups) >= 2 and groups[0] and groups[1]:
                    codigo = f"Parcela {len(self.parcelas) + 1}"
                    data_str = groups[0]
                    valor_str = groups[1]
                else:
                    return
            
            # Validar e converter
            if not self._is_valid_date(data_str):
                return
            
            # Converter valor
            valor = self._parse_currency(valor_str)
            if valor <= 0:
                return
            
            # Verificar duplicatas
            parcela_key = f"{codigo}_{data_str}_{valor}"
            if parcela_key not in [f"{p['codigo']}_{p['data']}_{p['valor']}" for p in self.parcelas]:
                self.parcelas.append({
                    'codigo': codigo.strip(),
                    'data': data_str.strip(),
                    'valor': valor,
                    'padrao': padrao_used[:20]
                })
                
        except Exception as e:
            # Silenciosamente ignorar erros de parsing individuais
            pass
    
    def _process_table(self, table):
        """Processa tabelas extraídas"""
        for row in table:
            if len(row) >= 3:
                # Procurar por colunas que possam conter dados de parcelas
                for i, cell in enumerate(row):
                    if cell and isinstance(cell, str):
                        # Verificar se parece com dados de parcela
                        if self._looks_like_parcela_data(cell):
                            self._parse_table_row(row)
                            break
    
    def _looks_like_parcela_data(self, text: str) -> bool:
        """Verifica se o texto parece conter dados de parcela"""
        patterns = [
            r'PR\.\d+/\d+',
            r'Parcela\s+\d+',
            r'\d{2}/\d{2}/\d{4}',
            r'R\$\s*[\d\.]+,\d{2}',
            r'[\d\.]+,\d{2}\s*$'
        ]
        
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        return False
    
    def _parse_table_row(self, row):
        """Tenta extrair dados de uma linha de tabela"""
        try:
            # Procurar por data
            data_str = None
            valor_str = None
            codigo = f"Parcela {len(self.parcelas) + 1}"
            
            for cell in row:
                if cell and isinstance(cell, str):
                    cell = cell.strip()
                    
                    # Verificar se é data
                    if not data_str and re.match(r'\d{2}/\d{2}/\d{4}', cell):
                        if self._is_valid_date(cell):
                            data_str = cell
                    
                    # Verificar se é valor monetário
                    if not valor_str:
                        # Padrões de valores
                        valor_match = re.search(r'([\d\.]+,\d{2})', cell)
                        if valor_match:
                            valor_str = valor_match.group(1)
            
            if data_str and valor_str:
                valor = self._parse_currency(valor_str)
                if valor > 0:
                    self.parcelas.append({
                        'codigo': codigo,
                        'data': data_str,
                        'valor': valor,
                        'padrao': 'tabela'
                    })
        except:
            pass
    
    def _try_alternative_parsing(self, pdf):
        """Tenta métodos alternativos de parsing"""
        try:
            # Tentar extrair por posição (coordenadas)
            for page in pdf.pages:
                # Extrair palavras com suas posições
                words = page.extract_words()
                
                # Agrupar palavras por linha (baseado na posição Y)
                lines = {}
                for word in words:
                    y = round(word['top'])
                    if y not in lines:
                        lines[y] = []
                    lines[y].append(word['text'])
                
                # Processar cada linha
                for line_words in lines.values():
                    line_text = ' '.join(line_words)
                    
                    # Verificar padrões na linha
                    self._check_line_for_patterns(line_text)
                    
        except Exception as e:
            st.warning(f"Método alternativo falhou: {str(e)}")
    
    def _check_line_for_patterns(self, line_text: str):
        """Verifica padrões em uma linha de texto"""
        # Padrão simples: data seguida de valor
        date_pattern = r'(\d{2}/\d{2}/\d{4})'
        value_pattern = r'([\d\.]+,\d{2})'
        
        dates = re.findall(date_pattern, line_text)
        values = re.findall(value_pattern, line_text)
        
        if dates and values and len(dates) == len(values):
            for date_str, value_str in zip(dates, values):
                if self._is_valid_date(date_str):
                    valor = self._parse_currency(value_str)
                    if valor > 0:
                        self.parcelas.append({
                            'codigo': f"Parcela {len(self.parcelas) + 1}",
                            'data': date_str,
                            'valor': valor,
                            'padrao': 'linha'
                        })
    
    def _extract_cliente_info(self, text: str):
        """Extrai informações do cliente"""
        # Padrões comuns
        patterns = {
            'codigo': r'Cliente[:\s]+(\d+)',
            'nome': r'Cliente[:\s]+\d+\s*[-\s]*\s*([^\n]+)',
            'cpf_cnpj': r'(CPF|CNPJ)[:\s]+([\d\.\/-]+)',
            'venda': r'Venda[:\s]*(\d+)',
            'data_venda': r'Data\s+(?:Venda|Emissão)[:\s]*(\d{2}/\d{2}/\d{4})'
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                self.cliente[key] = match.group(1) if key != 'nome' else match.group(1).strip()
    
    def _is_valid_date(self, date_str: str) -> bool:
        """Valida se uma string é uma data válida"""
        try:
            datetime.strptime(date_str, "%d/%m/%Y")
            return True
        except:
            return False
    
    def _parse_currency(self, value_str: str) -> float:
        """Converte string monetária para float"""
        try:
            # Remover R$ e espaços
            clean = value_str.replace('R$', '').replace(' ', '')
            
            # Verificar se usa ponto como separador de milhar
            if '.' in clean and ',' in clean:
                # Formato: 1.500,00
                clean = clean.replace('.', '').replace(',', '.')
            elif ',' in clean:
                # Formato: 1500,00
                clean = clean.replace(',', '.')
            
            return float(clean)
        except:
            return 0.0
    
    def _create_dataframe(self) -> pd.DataFrame:
        """Cria DataFrame com as parcelas encontradas"""
        if not self.parcelas:
            return pd.DataFrame()
        
        # Ordenar por data
        sorted_parcelas = sorted(self.parcelas, 
                                key=lambda x: datetime.strptime(x['data'], "%d/%m/%Y"))
        
        # Criar DataFrame
        df = pd.DataFrame(sorted_parcelas)
        
        # Renomear colunas para padrão
        if 'codigo' in df.columns and 'data' in df.columns and 'valor' in df.columns:
            df = df.rename(columns={
                'codigo': 'Parcela',
                'data': 'Dt Vencim',
                'valor': 'Valor Parcela'
            })
        
        # Remover coluna de padrão se existir
        if 'padrao' in df.columns:
            df = df.drop(columns=['padrao'])
        
        return df


# Função principal para compatibilidade
def extract_payment_data(uploaded_file):
    """Função principal para extrair dados de pagamento"""
    parser = PDFParser()
    
    if uploaded_file.name.endswith('.pdf'):
        return parser.extract_from_pdf(uploaded_file)
    elif uploaded_file.name.endswith(('.xlsx', '.xls')):
        # Para Excel, manter a função original
        return extract_from_excel(uploaded_file)
    else:
        raise ValueError("Formato de arquivo não suportado")


def extract_from_excel(excel_file):
    """Extrai dados de arquivo Excel (mantido para compatibilidade)"""
    try:
        df = pd.read_excel(excel_file)
        
        # Mapear possíveis nomes de coluna
        column_map = {}
        for col in df.columns:
            col_lower = str(col).lower()
            
            if any(x in col_lower for x in ['parcela', 'prestação', 'numero']):
                column_map['Parcela'] = col
            elif any(x in col_lower for x in ['vencim', 'vencimento', 'data']):
                column_map['Dt Vencim'] = col
            elif any(x in col_lower for x in ['valor', 'vlr', 'montante']):
                column_map['Valor Parcela'] = col
        
        # Renomear colunas
        df = df.rename(columns=column_map)
        
        # Manter apenas colunas necessárias
        required_cols = ['Parcela', 'Dt Vencim', 'Valor Parcela']
        available_cols = [c for c in required_cols if c in df.columns]
        
        if len(available_cols) >= 2:  # Pelo menos data e valor
            result_df = df[available_cols].copy()
            
            # Converter datas
            if 'Dt Vencim' in result_df.columns:
                result_df['Dt Vencim'] = pd.to_datetime(
                    result_df['Dt Vencim'], 
                    errors='coerce'
                ).dt.strftime('%d/%m/%Y')
            
            return result_df
        else:
            st.error("Colunas necessárias não encontradas no Excel")
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Erro ao ler Excel: {str(e)}")
        return pd.DataFrame()
