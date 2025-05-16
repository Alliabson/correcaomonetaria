
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
#Extratos de info dos PDF's
def extract_from_pdf(pdf_file):
    """Extrai dados específicos do PDF da Aquarela da Mata com todos os tipos de parcelas"""
    # Lista completa de todos os tipos de parcelas informados
    TODOS_TIPOS_PARCELAS = {
        '0': 'Tipo Zero',
        '1': 'Tipo Um',
        '100': 'Tipo Cem',
        '2': 'Tipo Dois',
        '200': 'Tipo Duzentos',
        '3': 'Tipo Três',
        'A': 'Avulso',
        'ACF': 'Acerto Financeiro',
        'ADV': 'Adiantamento',
        'AM': 'Amortização',
        'AOD': 'Aporte de Dinheiro',
        'APS': 'Aporte de Serviços',
        'AT': 'Ativo',
        'B': 'Boleto',
        'BR': 'Boleto Registrado',
        'C': 'Crédito',
        'CH': 'Cheque',
        'CON': 'Consórcio',
        'CRE': 'Crédito',
        'CUS': 'Custas',
        'DE': 'Débito',
        'DEV': 'Devolução',
        'DL': 'Diluição',
        'DLT': 'Diluição Total',
        'DVA': 'Devolução de Ativo',
        'E': 'Entrada',
        'EAP': 'Empréstimo Aporte',
        'EMP': 'Empréstimo',
        'ER': 'Estorno',
        'GCC': 'Guia de Correção de Crédito',
        'GRC': 'Guia de Regularização de Crédito',
        'IC': 'Imposto de Consumo',
        'ID': 'Identificação',
        'ISS': 'Imposto sobre Serviços',
        'ITA': 'Item de Acerto',
        'OP': 'Operação',
        'P': 'Parcela',
        'P1': 'Parcela Tipo 1',
        'PAC': 'Parcela Acordo',
        'PBC': 'Parcela Boleto Cobrança',
        'PC': 'Parcela Crédito',
        'PD1': 'Parcela Débito Tipo 1',
        'PDT': 'Parcela Débito Total',
        'PE': 'Parcela Especial',
        'PG': 'Pagamento',
        'PI': 'Parcela Inicial',
        'PI1': 'Parcela Inicial Tipo 1',
        'PM': 'Parcela Mensal',
        'PM1': 'Parcela Mensal Tipo 1',
        'PPS': 'Parcela Programa Social',
        'PQ': 'Parcela Quadrimestral',
        'PR': 'Parcela Regular',
        'PV': 'Parcela Vencida',
        'R': 'Recibo',
        'RC': 'Recibo de Crédito',
        'RCB': 'Recibo de Cobrança',
        'RCC': 'Recibo de Concessão Crédito',
        'RCH': 'Recibo de Cheque',
        'RCI': 'Recibo de Cobertura',
        'RCP': 'Recibo de Compra',
        'RD': 'Recibo de Débito',
        'RDC': 'Recibo de Devolução Crédito',
        'RDI': 'Recibo de Diligência',
        'RFT': 'Recibo de Folha de Pagamento',
        'RP': 'Recibo de Pagamento',
        'RPI': 'Recibo de Pagamento Inicial',
        'RPJ': 'Recibo de Pagamento Judicial',
        'RPR': 'Recibo de Pagamento Regular',
        'RRE': 'Recibo de Regularização',
        'RS': 'Recibo de Serviço',
        'RSD': 'Recibo de Saldo Devedor',
        'SM': 'Saldo Mensal',
        'TC': 'Taxa de Condomínio',
        'TR': 'Transferência',
        'TRO': 'Troca',
        'UNM': 'Unidade Monetária',
        'VA': 'Vale',
        'VME': 'Vale Mensal',
        'VTT': 'Vale Transporte'
    }

    parcelas = []
    
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
                
                # Processa as linhas de dados
                if in_payment_table:
                    # Remove múltiplos espaços e divide corretamente
                    cleaned_line = ' '.join(line.split())
                    parts = cleaned_line.split()
                    
                    # Verifica se o primeiro elemento é um tipo de parcela conhecido
                    if parts and parts[0] in TODOS_TIPOS_PARCELAS:
                        try:
                            parcela_info = {
                                'Tipo': parts[0],
                                'DescricaoTipo': TODOS_TIPOS_PARCELAS[parts[0]],
                                'Parcela': parts[0],  # Mantém para compatibilidade
                                'Dt Vencim': parse_date(parts[1]),
                                'Valor Parcela': parse_currency(parts[3]),
                                'Dt Recebimento': parse_date(parts[4]),
                                'Valor Recebido': parse_currency(parts[10])
                            }
                            parcelas.append(parcela_info)
                            
                        except (IndexError, ValueError) as e:
                            print(f"Erro ao processar linha: {line}. Erro: {str(e)}")
                            continue
                
                # Finaliza quando encontrar o total
                if in_payment_table and "Total a pagar:" in line:
                    break
    
    return pd.DataFrame(parcelas)

def parse_date(date_str):
    """Converte string de data no formato DD/MM/YYYY para objeto date"""
    return datetime.strptime(date_str, '%d/%m/%Y').date()

def parse_currency(value_str):
    """Converte valores monetários com vírgula decimal para float"""
    return float(value_str.replace('.', '').replace(',', '.'))

def extract_from_excel(excel_file):
    """Extrai dados de arquivos Excel com estrutura similar"""
    df = pd.read_excel(excel_file)
    
    # Mapeamento de colunas alternativas
    col_mapping = {
        'parcela': 'Parcela',
        'dt vencim': 'Dt Vencim',
        'valor parc': 'Valor Parcela',
        'dt receb': 'Dt Recebimento',
        'vlr parcela': 'Valor Recebido'
    }
    
    # Normalizar nomes de colunas
    df.columns = [col_mapping.get(col.lower().strip(), col) for col in df.columns]
    
    # Verificar se todas as colunas necessárias estão presentes
    required_columns = ['Parcela', 'Dt Vencim', 'Valor Parcela', 'Dt Recebimento', 'Valor Recebido']
    if not all(col in df.columns for col in required_columns):
        raise ValueError("Não foi possível identificar as colunas necessárias no arquivo Excel")
    
    # Converter tipos de dados
    date_columns = ['Dt Vencim', 'Dt Recebimento']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True).dt.date
    
    currency_columns = ['Valor Parcela', 'Valor Recebido']
    for col in currency_columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: parse_currency(str(x)) if isinstance(x, str) else float(x))
    
    return df[required_columns]
