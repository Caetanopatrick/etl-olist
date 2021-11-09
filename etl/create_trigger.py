import boto3


client = boto3.client('glue')

"""
Script para executar o trigger e rodar o job ETL diariamente as 3 da manhã.

"""

trigger = dict(
    Name='ETLDiario',
    Description='Trigger para executar o ETL job as 3 da manhã',
    Type='SCHEDULED',
    Actions=[
        dict(JobName='ETLCsvToParquet'),
    ],
    Schedule='cron(0 3 * * ? *)'
)


client.create_trigger(**trigger)


client.start_trigger(Name=trigger['Name'])

