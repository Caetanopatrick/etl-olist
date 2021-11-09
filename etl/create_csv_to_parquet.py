import boto3
import json

"""
Nesse script criamos o job ETL que irá converter nossas tabelas para parquet.
O job bookmark foi habilitado para fazer a transformação incremental.

"""

client = boto3.client('glue')

response = client.create_job(
    Name='ETLCsvToParquet',
    Role='MyDefaultGlueServiceRole',
    Command={
        'Name': 'glueetl',
        'ScriptLocation': 's3://aws-glue-scripts-520672085808-us-east-1/root/csv_to_parquet',
        'PythonVersion': '3'
    },
    DefaultArguments={
      '--TempDir': "s3://aws-glue-temporary-520672085808-us-east-1/root",
      '--job-bookmark-option': 'job-bookmark-enable'
    },
    MaxRetries=1,
    GlueVersion='2.0',
    NumberOfWorkers=2,
    WorkerType='Standard'
)

print(json.dumps(response, indent=4, sort_keys=True, default=str))