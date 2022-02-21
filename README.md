### Dados-Publicos-CNPJ-ETL

Coleta, processamento e armazenamento dos dados de empresas, socios e estabelecimento da [pagina de dados publicos da receita federal](https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj)


#### Requisitos
- Python 3.9
- PostgreSQL 14
- Airflow 2.0


#### Fluxo:
1. Os arquivos são baixados e armazenados na pasta RAW sem nenhum tratamento prévio.
2. Os arquivos são lidos e extraidos com correção de extensão para a pasta STANDARDIZED. 
3. Os arquivos da STANDARDIZED são lidos, é efetuado a inserção de nome de colunas, correção de tipos, conversão para parquet e por ultimo e são inseridos na pasta CONFORMED.
4. Os arquivos parquet da pasta CONFORMED são lidos e inseridos na tabela do banco de dados postgresql.


#### Parametros DAG
- Dag configurada para ser executada diariamente as 00:00 utc-0 
`schedule_interval="0 0 * * *"`
- Em caso de falha no serviço é executado 10 tentativas.
`"retries": 10`
- Cada tentiva ocorre a cada 1 hora.
`"retry_delay": timedelta(hours=1)`
