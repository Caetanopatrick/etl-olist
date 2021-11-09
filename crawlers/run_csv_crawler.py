import boto3
import json

"""
Esse script roda o crawler que infere o schema das tabelas csv.

"""

client = boto3.client('glue')


response = client.start_crawler(
    Name='OlistCSVCrawler'
)

print(json.dumps(response, indent=4, sort_keys=True, default=str))