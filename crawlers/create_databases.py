import boto3
import json

"""
O código abaixo cria os databases no glue catalog que irão ser utilizados para guardar as tabelas dos crawlers.

"""

client = boto3.client('glue')

response = client.create_database(
    DatabaseInput={'Name':'olist_csv'}
)
print(json.dumps(response, indent=4, sort_keys=True, default=str))

response = client.create_database(
    DatabaseInput={'Name':'olist_parquet_dev'}
)
print(json.dumps(response, indent=4, sort_keys=True, default=str))

response = client.create_database(
    DatabaseInput={'Name':'olist_parquet_prod'}
)

print(json.dumps(response, indent=4, sort_keys=True, default=str))
