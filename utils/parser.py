import pandas as pd
import pdfplumber
import re
from datetime import datetime
from typing import Dict, List

def extract_payment_data(file) -> Dict:
    """Extrai todos os dados de pagamento do PDF"""
    result = {
        'client_info': {},
        'payments': [],
        'raw_text': '',
        'summary': {}
    }
    
    with pdfplumber.open(file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            result['raw_text'] += text + "\n\n"
            
            # Extrai informações do cliente
            if not result['client_info']:
                result['client_info'] = extract_client_info(text)
            
            # Extrai pagamentos
            payment_lines = extract_payment_lines(text)
            result['payments'].extend(payment_lines)
            
            # Extrai resumo financeiro
            if not result['summary']:
                result['summary'] = extract_summary_info(text)
    
    return result

def extract_client_info(text: str) -> Dict:
    """Extrai informações do cliente do texto do PDF"""
    client_info = {
        'name': extract_value(text, 'Cliente :', 'Venda:'),
        'contract_number': extract_value(text, 'Venda:', 'Dt. Venda:'),
        'contract_date': extract_value(text, 'Dt. Venda:', 'Empreend.:'),
        'project': extract_value(text, 'Empreend.:', 'End.:'),
        'address': extract_value(text, 'End.:', 'Bairro :'),
        'neighborhood': extract_value(text, 'Bairro :', 'Cidade :'),
        'city': extract_value(text, 'Cidade :', 'UF :'),
        'state': extract_value(text, 'UF :', 'CEP :'),
        'zip_code': extract_value(text, 'CEP :', 'Fones:'),
        'phone': extract_value(text, 'Residencial:', 'Valor da venda:'),
        'contract_value': extract_value(text, 'Valor da venda:', 'Status da venda:'),
        'status': extract_value(text, 'Status da venda:', 'Titular')
    }
    return {k: v.strip() if isinstance(v, str) else v for k, v in client_info.items()}

def extract_payment_lines(text: str) -> List[Dict]:
    """Extrai linhas de pagamento do texto do PDF"""
    payments = []
    lines = text.split('\n')
    
    for line in lines:
        # Identifica linhas de pagamento (P.1/144, E.1/1, B.1/12)
        if re.match(r'^[EPB]\.?\d+/\d+', line.strip()):
            payment = parse_payment_line(line)
            if payment:
                payments.append(payment)
    
    return payments

def parse_payment_line(line: str) -> Dict:
    """Analisa uma linha de pagamento individual"""
    try:
        # Remove múltiplos espaços
        line = ' '.join(line.split())
        parts = line.split()
        
        payment = {
            'installment': parts[0],
            'due_date': None,
            'payment_date': None,
            'installment_value': None,
            'correction': None,
            'fine': None,
            'interest': None,
            'discount': None,
            'late_correction': None,
            'days_late': 0,
            'other': None,
            'total_value': None
        }
        
        # Extrai datas (padrão DD/MM/AAAA)
        dates = [p for p in parts if re.match(r'\d{2}/\d{2}/\d{4}', p)]
        if len(dates) >= 1:
            payment['due_date'] = dates[0]
        if len(dates) >= 2:
            payment['payment_date'] = dates[1]
        
        # Extrai valores monetários (padrão 1.234,56)
        values = [p for p in parts if re.match(r'^\d{1,3}(?:\.\d{3})*,\d{2}$', p)]
        
        # Atribui valores com base na posição esperada
        if len(values) >= 1:
            payment['installment_value'] = values[0]
        if len(values) >= 2:
            payment['correction'] = values[1]
        if len(values) >= 3:
            payment['fine'] = values[2]
        if len(values) >= 4:
            payment['interest'] = values[3]
        if len(values) >= 5:
            payment['discount'] = values[4]
        if len(values) >= 6:
            payment['late_correction'] = values[5]
        if len(values) >= 7:
            payment['total_value'] = values[6]
        
        # Extrai dias em atraso (último número antes do valor final)
        numbers = [p for p in parts if p.isdigit()]
        if numbers:
            payment['days_late'] = int(numbers[-1])
        
        return payment
    
    except Exception as e:
        print(f"Erro ao processar linha: {line}. Erro: {str(e)}")
        return None

def extract_summary_info(text: str) -> Dict:
    """Extrai informações resumidas do contrato"""
    summary = {}
    
    # Extrai valores totais
    received_match = re.search(r'RECEBIDO\s*:\s*([\d.,]+)', text)
    if received_match:
        summary['total_received'] = received_match.group(1)
    
    receivable_match = re.search(r'RECEBER \+ RESID\.\s*:\s*([\d.,]+)', text)
    if receivable_match:
        summary['total_receivable'] = receivable_match.group(1)
    
    # Extrai percentuais
    paid_match = re.search(r'% Recebida\s*:\s*([\d.,]+%)', text)
    if paid_match:
        summary['paid_percentage'] = paid_match.group(1)
    
    receivable_pct_match = re.search(r'% A Receber\s*:\s*([\d.,]+%)', text)
    if receivable_pct_match:
        summary['receivable_percentage'] = receivable_pct_match.group(1)
    
    return summary

def extract_value(text: str, start_marker: str, end_marker: str) -> str:
    """Extrai valor entre dois marcadores"""
    try:
        start = text.index(start_marker) + len(start_marker)
        end = text.index(end_marker, start)
        return text[start:end].strip()
    except ValueError:
        return ''
