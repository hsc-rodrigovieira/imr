# Impacto MR

Aplicação desenvolvida para automatizar a coleta, tratamento e envio dos dados para o projeto Impacto MR do Hospital Albert Einstein.

## Tecnologias Utilizadas
- Python
- Streamlit
- SQL Oracle

## Funcionalidades

- Baixar arquivos EPIMED: lista os arquivos prontos das competências já consolidadas, em ordem decrescente.
- Consolidação de custos: permite rodar a consolidação dos custos para o mês selecionado. Caso o mês já tenha sido consolidado, exibe um aviso para o usuário. É possível rodar a consolidação novamente se for necessário.
- Incluir registros do EPIMED: permite importar a lista de atendimentos do EPIMED.

## Estrutura do Projeto
```plaintext
IMR/
│-- src/                               # Código-fonte principal do projeto
│   │-- .streamlit/                    # Configurações para aplicação Streamlitbanco de dados)
│   │-- data/                          # Diretório de dados
│   │   │-- csv/                       # Arquivos CSV organizados por status de processamento
│   │   │   │-- external/              # Dados brutos externos
│   │   │   │-- final/                 # Dados finais processados
│   │   │   │-- processed/             # Dados parcialmente processados
│   │   │   │-- raw/                   # Dados crus coletados do banco de dados
│   │   │   │-- downloads/             # Local para armazenar os arquivos para download
│   │   │   │-- scripts/               # Scripts auxiliares para processamento
│   │-- ETL/                           # Módulos do pipeline ETL
│   │   │-- dbConfig.py                # Funções de coleta no banco de dados
│   │   │-- Pipeline.py                # Pipeline principal de ETL
│   │   │-- pages/                     # Scripts individuais para extração e processamento
│   │       │-- baixarArquivos.py      # Baixa arquivos finalizados
│   │       │-- consolidacaoCustos.py  # Consolida dados de custos
│   │       │-- uploadListaEpimed.py   # Faz upload dos pacientes do EPIMED
│   │-- config.py                      # Arquivo de configuração do projeto
│   │-- imr.log                        # Logs de execução do sistema
│-- .gitignore                         # Arquivos ignorados pelo Git
│-- requirements.txt                   # Dependências do projeto
│-- README.md                          # Documentação do projeto
```

## Instalação e Uso

### Pré-requisitos

Antes de rodar a aplicação, certifique-se de ter instalado Python 3.12

### Passos para Rodar o Projeto

1. Clone este repositório:
   ```bash
   git clone https://github.com/hsc-rodrigovieira/imr.git
   ```

2. Crie um ambiente virtual:
   ```bash
   python -m venv venv
   source venv/bin/activate   # No Windows use: venv\Scripts\activate
   ```

3. Acesse o diretório do projeto:
   ```bash
   cd IMR/scr
   ```

4. Instale as dependências do projeto:
    ```bash
    pip install -r requirements.txt
    ```

5. Crie uma pasta .streamlit dentro de scr.

6. Na pasta .streamlit, crie um arquivo ```secrets.toml``` para configurar as credenciais de acesso ao banco de dados:
    ```toml
    [database]
    USERNAME = ""
    HOST = ""
    PASSWORD = ""
    ```

7. Execute a aplicação:
    ```bash
    streamlit run __init__.py
    ```

6. Acesse a aplicação pelo endereço gerado no terminal:
    ```bash
    http://localhost:<porta>
    ```