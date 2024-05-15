import base64
import datetime
import json 
import pandas as pd
import logging
import google.cloud.logging
from google.cloud import storage
from google.cloud import bigquery

# Defina variáveis para Funções na Nuvem
print("Carregando valores do job")
project_name = 'comerc-datavis'
bucket_name = '804171967601-comerc-aws-destination-bucket'
dataset_name = 'ds_looker_embedded'
folder_name = 'dados_infomerc'  # Nome da pasta que contém os arquivos Parquet

def list_parquet_files_in_bucket(bucket_name, folder_name):
    """Listar todos os arquivos Parquet em uma pasta de um bucket do Google Cloud Storage."""
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=folder_name + '/', delimiter='/')

    parquet_files = []
    for blob in blobs:
        if blob.name.endswith('.parquet'):
            parquet_files.append(blob.name)

    return parquet_files

def generate_data():
    status = "OK"
    detail = ""
    print('Iniciando processo')
    try:
        schema_file = "schema_tabela.json"

        with open(schema_file) as json_file:
            schema = json.load(json_file)
        
        # Listar todos os arquivos Parquet na pasta
        parquet_files = list_parquet_files_in_bucket(bucket_name, folder_name)

        if not parquet_files:
            print(f"Nenhum arquivo Parquet encontrado na pasta ['{folder_name}'] do bucket: [{bucket_name}]")
            return {"status": "Failed", "detail": f"Nenhum arquivo Parquet encontrado na pasta ['{folder_name}'] do bucket: [{bucket_name}]"}

        print(f'Encontrados {len(parquet_files)} arquivos Parquet na pasta [{folder_name}]')

        # Inicializar uma lista para armazenar os DataFrames de cada arquivo Parquet
        dfs = []

        # Loop através de cada arquivo e carregá-lo em um DataFrame
        for file_name in parquet_files:
            uri = f"gs://{bucket_name}/{file_name}"
            print(f'Carregando arquivo Parquet: "{uri}"')
            df = pd.read_parquet(uri, engine='pyarrow')
            dfs.append(df)

        # Concatenar todos os DataFrames em um único DataFrame
        combined_df = pd.concat(dfs, ignore_index=True)
        print(f'Dataframe criado com sucesso para pasta [{folder_name}]')

        # Obtenha a hora atual
        today = datetime.datetime.now().strftime('%Y-%m-%d')

        # Configurando a exibição de floats no DataFrame
        pd.options.display.float_format = "{:.2f}".format

        # Adicionando timestamp ao DataFrame
        combined_df['timestamp_processamento_gcp'] = pd.to_datetime(pd.Timestamp.now())

        # Convertendo [mes_refencia] (formato YYYYMM) para ficar com padrao YYYY-MM-DD
        combined_df['mes_referencia_date'] = combined_df['mes_referencia'].str[:4] + '-' + combined_df['mes_referencia'].str[-2:] + '-01'

        # Convertendo para DATE
        combined_df['mes_referencia_date'] =  pd.to_datetime(combined_df['mes_referencia_date'])
        
        #print(combined_df.info()) #linha para debug -- printa o schema do DF nos logs
        # Carregue o arquivo Parquet do Cloud Storage para o BigQuery
        print('Carregando arquivo Parquet do Cloud Storage para o BigQuery')
        client = bigquery.Client()

        #table_id = "ds_looker_embedded.dados_infomerc" 
        table_id = dataset_name + '.' + folder_name
        print('Carregando dados na tabela: ', table_id)

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=schema,
            autodetect=False,
            source_format=bigquery.SourceFormat.PARQUET
        )

        load_job = client.load_table_from_dataframe(combined_df, table_id, job_config=job_config)
        load_job.result()  # Aguarde a conclusão do carregamento
        print("Dados carregados com sucesso!")

        # Faça uma solicitação de API e exiba o número de linhas carregadas
        destination_table = client.get_table(table_id)
        print("Carregadas {} linhas na tabela.".format(destination_table.num_rows))

    except Exception as e:
        # Adicionando tratamento de exceção para lidar com erros
        status = "Failed"
        detail = str(e)
        
    return {"status": status, "detail": detail}

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    generate_data()

if __name__ == "__main__":
    hello_pubsub('data', 'context')
