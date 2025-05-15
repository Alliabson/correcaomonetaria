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
                # Verificação mais robusta do cabeçalho
                if all(term in line for term in ["Parcela", "Dt Vencim", "Valor Parc.", "Dt. Receb.", "Vlr da Parcela"]):
                    in_payment_table = True
                    header_found = True
                    continue
                
                if in_payment_table and line.strip():
                    # Padrão melhorado para capturar todos os tipos de parcelas
                    parcela_pattern = r'^([A-Z]{1,3}\.\d+/\d+)\s+(\d{2}/\d{2}/\d{4})\s+([\d.,]+)\s+(\d{2}/\d{2}/\d{4})?\s*([\d.,]+)?'
                    match = re.match(parcela_pattern, line.strip())
                    
                    if match:
                        try:
                            # Extrai os componentes da linha
                            parcela = match.group(1)
                            dt_vencim = datetime.strptime(match.group(2), '%d/%m/%Y').date()
                            valor_parcela = float(match.group(3).replace('.', '').replace(',', '.'))
                            
                            # Trata dados de recebimento (opcionais)
                            dt_receb = None
                            valor_recebido = 0.0
                            
                            if match.group(4):
                                dt_receb = datetime.strptime(match.group(4), '%d/%m/%Y').date()
                            
                            if match.group(5):
                                valor_recebido = float(match.group(5).replace('.', '').replace(',', '.'))
                            
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
                            
                        except Exception as e:
                            print(f"Erro ao processar linha: {line}. Erro: {str(e)}")
                            continue
                
                # Finaliza quando encontrar o total
                if in_payment_table and any(t in line for t in ["Total a pagar:", "TOTAL GERAL:"]):
                    break
    
    # Cria DataFrame mantendo todas as linhas
    df = pd.DataFrame(parcelas)
    
    if not df.empty:
        # Calcula campos adicionais
        df['Dias Atraso'] = df.apply(
            lambda x: (x['Dt Recebimento'] - x['Dt Vencim']).days 
            if pd.notnull(x['Dt Recebimento']) and x['Dt Recebimento'] > x['Dt Vencim'] 
            else 0,
            axis=1
        )
        df['Valor Pendente'] = df['Valor Parcela'] - df['Valor Recebido']
        
        # Formatação dos valores para exibição
        df['Valor Parcela'] = df['Valor Parcela'].apply(lambda x: f"R$ {x:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
        df['Valor Recebido'] = df['Valor Recebido'].apply(lambda x: f"R$ {x:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
        df['Valor Pendente'] = df['Valor Pendente'].apply(lambda x: f"R$ {x:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
    
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
