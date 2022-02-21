import os
import glob
import requests
import pandas as pd
from zipfile import ZipFile
from airflow.models import Variable
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain


urls_empr = ['http://200.152.38.155/CNPJ/K3241.K03200Y0.D20212.EMPRECSV.zip']
urls_esta = ['http://200.152.38.155/CNPJ/K3241.K03200Y0.D20212.ESTABELE.zip']
urls_soci = ['http://200.152.38.155/CNPJ/K3241.K03200Y0.D20212.SOCIOCSV.zip']

raw_path_empresas = 'raw/empresas/'
standardized_path_empresas = 'standardized/empresas/'
conformed_path_empresas = 'conformed/empresas/'

raw_path_estabelecimentos = 'raw/estabelecimentos/'
standardized_path_estabelecimentos = 'standardized/estabelecimentos/'
conformed_path_estabelecimentos = 'conformed/estabelecimentos/'

raw_path_socios = 'raw/socios/'
standardized_path_socios = 'standardized/socios/'
conformed_path_socios = 'conformed/socios/'


default_args = {
    'owner': 'Renan Nogueira',
    "depends_on_past": False,
    "start_date": datetime(2022, 2, 20),
    "retries": 10,
    "retry_delay": timedelta(hours=1),
}

@dag(default_args=default_args, schedule_interval="0 0 * * *", catchup=False)
def ETL():
   
   
    # Extracting raw data from the urls
    @task
    def extract_data(urls, path):
        print('Downloading files...')

        for url in urls:
            response = requests.get(url)

            if response.status_code == 200:
                file_name = url.split('/')[-1]

                with open(path + file_name, 'wb') as f:
                    f.write(response.content)
                    print('Download completed! ' + url)

            else:
                print('Error downloading the file: ' + url)

        print('Download process completed.')


    # Extract the raw data, add extension and save to standardized folder
    @task
    def process_raw_data(from_path, to_path):
        print('Starting raw process...')

        for index, raw_zip_file in enumerate(glob.glob(from_path + '*.zip')):
            zip = ZipFile(raw_zip_file, 'r')
            zip_data = zip.read(zip.namelist()[index])

            if not os.path.exists(to_path):
                os.makedirs(to_path)

            output = open(to_path + zip.namelist()[index] + ".csv", 'wb')
            output.write(zip_data)
            output.close()

        print('Raw process completed!')



    # Create a dataframe ``empresas`` with the standardized data and save to conformed folder
    @task
    def process_standardized_empresas(from_path, to_path):
        print('Reading all files in the path: ' + from_path)

        all_files = glob.glob(from_path + "*.csv")

        columns = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel',
                'capital_social', 'porte_empresa', 'ente_federativo_responsavel']

        df = pd.concat(pd.read_csv(x, delimiter=';', names=columns, encoding='latin-1',
                    dtype={'cnpj_basico': str, 'razao_social': str, 'natureza_juridica': str, 'qualificacao_responsavel': str,
                            'capital_social': str, 'porte_empresa': str, 'ente_federativo_responsavel': str}) for x in all_files)

        if not os.path.exists(to_path):
            os.makedirs(to_path)
        df.to_parquet(to_path + 'empresas.parquet', index=False)

        print('Transformation completed!', to_path)



    # Create a dataframe ``estabelecimentos`` with the standardized data and save to conformed folder
    @task
    def process_standardized_estabelecimentos(from_path, to_path):
        print('Reading all files in the path: ' + from_path)

        all_files = glob.glob(from_path + "*.csv")

        columns = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais', 'data_inicio_atividade', 'cnae_fiscal_principal',
                'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'ddd_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_situacao_especial']

        df = pd.concat(pd.read_csv(x, delimiter=';', names=columns, encoding='latin-1',
                    dtype={'cnpj_basico': str, 'cnpj_ordem': str, 'cnpj_dv': str, 'identificador_matriz_filial': str, 'nome_fantasia': str,
                            'situacao_cadastral': str, 'data_situacao_cadastral': str, 'motivo_situacao_cadastral': str, 'nome_cidade_exterior': str, 'pais': str, 'data_inicio_atividade': str, 'cnae_fiscal_principal': str,
                            'cnae_fiscal_secundaria': str, 'tipo_logradouro': str, 'logradouro': str, 'numero': str, 'complemento': str, 'bairro': str, 'cep': str, 'uf': str, 'municipio': str, 'ddd_1': str, 'telefone_1': str,
                            'ddd_2': str, 'telefone_2': str, 'ddd_fax': str, 'fax': str, 'correio_eletronico': str, 'situacao_especial': str, 'data_situacao_especial': str}) for x in all_files)

        df['data_situacao_cadastral'] = pd.to_datetime(
            df['data_situacao_cadastral'], errors='coerce').dt.strftime('%Y-%m-%d').astype('datetime64[ns]')
        df['data_inicio_atividade'] = pd.to_datetime(
            df['data_inicio_atividade'], errors='coerce').dt.strftime('%Y-%m-%d').astype('datetime64[ns]')
        df['data_situacao_especial'] = pd.to_datetime(
            df['data_situacao_especial'], errors='coerce').dt.strftime('%Y-%m-%d').astype('datetime64[ns]')
        df['motivo_situacao_cadastral'] = df['motivo_situacao_cadastral'].astype(
            int)
        df['identificador_matriz_filial'] = df['identificador_matriz_filial'].astype(
            int)

        if not os.path.exists(to_path):
            os.makedirs(to_path)
        df.to_parquet(to_path + 'estabelecimentos.parquet',  index=False)

        print('Transformation completed!', to_path)



    # Create a dataframe ``socios`` with the standardized data and save to conformed folder
    @task
    def process_standardized_socios(from_path, to_path):
        print('Reading all files in the path: ' + from_path)

        all_files = glob.glob(from_path + "*.csv")

        columns = ['cnpj_basico', 'identificador_socio', 'nome_socio_razao_social', 'cpf_cnpj_socio', 'qualificacao_socio',
                'data_entrada_sociedade', 'pais', 'representante_legal', 'nome_do_representante', 'qualificacao_representante_legal', 'faixa_etaria']

        df = pd.concat(pd.read_csv(x, delimiter=';', names=columns,
                    encoding='latin-1', dtype={'cnpj_basico': str, 'identificador_socio': str, 'cpf_cnpj_socio': str, 'qualificacao_socio': str,
                                                'data_entrada_sociedade': str, 'pais': str, 'representante_legal': str, 'nome_do_representante': str,
                                                'qualificacao_representante_legal': str, 'faixa_etaria': str}) for x in all_files)

        if not os.path.exists(to_path):
            os.makedirs(to_path)
        df.to_parquet(to_path + 'socios.parquet',  index=False)

        print('Transformation completed!', to_path)



    # Load the dataframe in the database
    @task
    def load_database(from_path, db_table):
        print('Loading database...')

        username = Variable.get("db_username")
        password = Variable.get("db_password")
        database = Variable.get("db_database")
        
        all_files = glob.glob(from_path + "*.parquet")
        df = pd.concat(pd.read_parquet(x) for x in all_files)

        engine = create_engine(f'postgresql://{username}:{password}@localhost:5432/{database}')
        df.to_sql(db_table, engine, schema='public', if_exists='replace')

        print('Database loaded!')
        
        
    extract_data(urls_empr, raw_path_empresas) >> process_raw_data(raw_path_empresas, standardized_path_empresas) >> process_standardized_empresas(standardized_path_empresas, conformed_path_empresas) >> load_database(conformed_path_empresas, 'empresas')
    extract_data(urls_esta, raw_path_estabelecimentos) >> process_raw_data(raw_path_estabelecimentos, standardized_path_estabelecimentos) >> process_standardized_estabelecimentos(standardized_path_estabelecimentos, conformed_path_estabelecimentos) >> load_database(conformed_path_estabelecimentos, 'estabelecimentos')
    extract_data(urls_soci, raw_path_socios) >> process_raw_data(raw_path_socios, standardized_path_socios) >> process_standardized_socios(standardized_path_socios, conformed_path_socios) >> load_database(conformed_path_socios, 'socios')
        
run = ETL()



    