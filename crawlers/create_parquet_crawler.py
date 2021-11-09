import boto3
import json

"""
O código abaixo utiliza um crawler para escanear e inferir o schema das tabelas da olist
convertidas para parquet que foram transferidas pelo ETL job.

"""
database_name=""

DEV_OR_PROD = "DEV"

if DEV_OR_PROD == "DEV":
    database_name = "olist_parquet_dev"
if DEV_OR_PROD == "PROD":
    database_name = "olist_parquet_prod"


client = boto3.client('glue')

response = client.create_crawler(
    Name='OlistParquetCrawler',
    Role='MyDefaultGlueServiceRole',
    DatabaseName=database_name,
    Description='Crawler for the parquet olist files.',
    Targets={
        'S3Targets': [
            {
                'Path': 's3://patrick-olist-parquet-data',
                'Exclusions': [
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