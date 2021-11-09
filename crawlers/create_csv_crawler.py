import boto3
import json

"""
O código abaixo utiliza um crawler para escanear e inferir o schema das tabelas da olist
guardadas em arquivos csv dentro do bucket s3://patrick-olist-data'.
O arquivo olist_orders_dataset.csv foi excluído visto que o crawler não consegue inferir suas colunas automaticamente,
sendo assim foi criado o script create_orders_table.py para fazer a criação da tabela manualmente.

"""

client = boto3.client('glue')

response = client.create_crawler(
    Name='OlistCSVCrawler',
    Role='MyDefaultGlueServiceRole',
    DatabaseName='olist_csv',
    Description='Crawler for generated Sales schema',
    Targets={
        'S3Targets': [
            {
                'Path': 's3://patrick-olist-data',
                'Exclusions': ["**olist_orders_dataset**"
                               ]
            },
        ]
    },
    SchemaChangePolicy={
        'UpdateBehavior': 'UPDATE_IN_DATABASE',
        'DeleteBehavior': 'DELETE_FROM_DATABASE'
    }

)

print(json.dumps(response, indent=4, sort_keys=True, default=str))