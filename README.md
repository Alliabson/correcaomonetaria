README - Aplicativo de Correção Monetária Completa
📌 Visão Geral
Este é um aplicativo Streamlit que realiza correção monetária de valores financeiros utilizando índices econômicos oficiais. Ele pode processar tanto valores manuais quanto extrair automaticamente informações de documentos PDF (como contratos ou extratos financeiros) e aplicar a correção monetária com base nos índices selecionados.

✨ Funcionalidades Principais
Correção monetária individual ou por média de índices

Processamento automático de PDFs (extrai informações de clientes, vendas e parcelas)

Suporte a múltiplos índices econômicos (IPCA, IGPM, SELIC, etc.)

Cálculo detalhado por parcela com histórico de correção

Exportação de resultados para CSV

Interface intuitiva com visualização de dados

🛠️ Como Usar
Selecione o modo de operação:

Corrigir Valores do PDF: Para processar documentos PDF com parcelas

Corrigir Valor Manual: Para corrigir um valor específico

Configure os parâmetros:

Escolha entre correção por índice único ou média de índices

Selecione o(s) índice(s) econômico(s) desejado(s)

Defina a data de referência para a correção

Para PDFs:

Carregue o arquivo PDF contendo as parcelas

O sistema extrairá automaticamente as informações

Clique em "Calcular Correção Monetária" para processar

Para valores manuais:

Informe o valor a ser corrigido

Defina a data original do valor

O resultado será exibido automaticamente

Exporte os resultados (para PDFs):

Visualize os dados corrigidos

Baixe os resultados em formato CSV

📊 Índices Suportados
O aplicativo utiliza índices econômicos oficiais obtidos através de API. Os índices disponíveis podem incluir (dependendo da configuração):

IPCA (Índice Nacional de Preços ao Consumidor Amplo)

IGPM (Índice Geral de Preços do Mercado)

INPC (Índice Nacional de Preços ao Consumidor)

SELIC (Taxa Básica de Juros)

Outros índices econômicos relevantes

⚙️ Requisitos Técnicos
Python 3.8+

Bibliotecas principais:

streamlit

pdfplumber

pandas

requests

pytz

python-dateutil

🚀 Instalação
Clone o repositório:

bash
git clone [URL_DO_REPOSITORIO]
cd [NOME_DO_DIRETORIO]
Crie um ambiente virtual (recomendado):

bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate  # Windows
Instale as dependências:

bash
pip install -r requirements.txt
Execute o aplicativo:

bash
streamlit run app.py
📝 Notas Importantes
O aplicativo requer conexão com a internet para acessar os índices econômicos atualizados

Para processamento de PDFs, o documento deve seguir um formato específico com informações claras de parcelas

Os cálculos são baseados em dados oficiais, mas devem ser validados para usos críticos

📧 Suporte
Para problemas ou dúvidas, entre em contato com o desenvolvedor.

Desenvolvido com ❤️ usando Streamlit e Python
