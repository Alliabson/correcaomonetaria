import pandas as pd
import pdfplumber
from datetime import datetime

def extract_payment_data(uploaded_file):
    """Extrai dados de parcelas de arquivos PDF ou Excel"""
    if uploaded_file.name.endswith('.pdf'):
        return extract_from_pdf(uploaded_file)
    elif uploaded_file.name.endswith(('.xlsx', '.xls')):
        return extract_from_excel(uploaded_file)
    else:
        raise ValueError("Formato de arquivo não suportado")

def extract_from_pdf(pdf_file):
    """Extrai dados específicos do PDF da Aquarela da Mata"""
    parcelas = []
    
    # Lista completa de prefixos com ponto
    prefixos_com_ponto = [
        'PR.', 'E.', 'P.', 'B.', 'BR.', 'SM.', '1.', 'ER.', '0.', '100.', '2.', '200.', '3.', 
        'A.', 'ACF.', 'ADV.', 'AM.', 'AOD.', 'APS.', 'AT.', 'C.', 'CH.', 'CON.', 'CRE.', 'CUS.', 
        'DE.', 'DEV.', 'DL.', 'DLT.', 'DVA.', 'EAP.', 'EMP.', 'GCC.', 'GRC.', 'IC.', 'ID.', 
        'ISS.', 'ITA.', 'OP.', 'P1.', 'PAC.', 'PBC.', 'PC.', 'PD1.', 'PDT.', 'PE.', 'PG.', 
        'PI.', 'PI1.', 'PM.', 'PM1.', 'PPS.', 'PQ.', 'PV.', 'R.', 'RC.', 'RCB.', 'RCC.', 
        'RCH.', 'RCI.', 'RCP.', 'RD.', 'RDC.', 'RDI.', 'RFT.', 'RP.', 'RPI.', 'RPJ.', 'RPR.', 
        'RRE.', 'RS.', 'RSD.', 'TC.', 'TR.', 'TRO.', 'UNM.', 'VA.', 'VME.', 'VTT.'
    ]
    
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            lines = text.split('\n')
            
            in_payment_table = False
            header_found = False
            
            for line in lines:
                # Verifica se é a linha de cabeçalho da tabela
                if ("Parcela" in line and "Dt Vencim" in line and "Valor Parc." in line and 
                    "Dt. Receb." in line and "Vlr da Parcela" in line):
                    in_payment_table = True
                    header_found = True
                    continue
                
                # Processa as linhas de dados que começam com qualquer prefixo válido
                if in_payment_table and any(line.strip().startswith(prefix) for prefix in prefixos_com_ponto):
                    # Remove múltiplos espaços e divide corretamente
                    cleaned_line = ' '.join(line.split())
                    parts = cleaned_line.split()
                    
                    try:
                        # Ajuste os índices conforme necessário para o seu PDF
                        parcela_info = {
                            'Parcela': parts[0],
                            'Dt Vencim': parse_date(parts[1]),
                            'Valor Parcela': parse_currency(parts[3]),
                            'Dt Recebimento': parse_date(parts[4]),
                            'Valor Recebido': parse_currency(parts[10] if len(parts) > 10 else '0'),
                            'Status Pagamento': 'Pago' if len(parts) > 10 and parse_currency(parts[10]) > 0 else 'Pendente',
                            'Arquivo Origem': pdf_file.name
                        }
                        parcelas.append(parcela_info)
                        
                    except (IndexError, ValueError) as e:
                        print(f"Erro ao processar linha: {line}. Erro: {str(e)}")
                        continue
                
                # Finaliza quando encontrar o total
                if in_payment_table and ("Total a pagar:" in line or "TOTAL GERAL:" in line):
                    break
    
    if not parcelas:
        print("Nenhuma parcela encontrada. Linhas disponíveis:")
        for i, line in enumerate(lines):
            print(f"{i}: {line}")
    
    # Cria DataFrame e adiciona colunas calculadas
    df = pd.DataFrame(parcelas)
    if not df.empty:
        df['Dias Atraso'] = (pd.to_datetime(df['Dt Recebimento']) - pd.to_datetime(df['Dt Vencim'])).dt.days
        df['Valor Pendente'] = df.apply(
            lambda x: x['Valor Parcela'] - x['Valor Recebido'] if x['Valor Recebido'] < x['Valor Parcela'] else 0,
            axis=1
        )
    
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
