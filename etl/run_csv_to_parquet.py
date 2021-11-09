import boto3
import json


client = boto3.client('glue')

"""
Esse script roda o job ETL que converte as tabelas para parquet.

"""

response = client.start_job_run(
    JobName="ETLCsvToParquet"
)

print(json.dumps(response, indent=4, sort_keys=True, default=str))