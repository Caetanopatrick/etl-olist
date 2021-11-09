import boto3
import json

"""
Esse script roda o crawler que infere o schema das tabelas parquet.

"""

client = boto3.client('glue')

response = client.start_crawler(
    Name='OlistParquetCrawler'
)

print(json.dumps(response, indent=4, sort_keys=True, default=str))