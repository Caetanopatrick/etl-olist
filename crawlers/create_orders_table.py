import boto3
import json

client = boto3.client('glue')

"""
Nesse script criamos a definição da tabela olist_orders_dataset_csv manualmente.
Definindo os nomes das suas colunas e parâmetro para o glue pular a primeira linha, pois se trata 
do nome das colunas.

"""

response = client.create_table(
    DatabaseName='olist_csv',
    TableInput={
        'Name': 'olist_orders_dataset_csv',
        'Description': '',
        'Owner': 'caetanopatrick',
        'StorageDescriptor': {
            'Columns': [

                {'Name': 'order_id', 'Type': 'string', 'Comment': ''},
                {'Name': 'customer_id', 'Type': 'string', 'Comment': ''},
                {'Name': 'order_status', 'Type': 'string', 'Comment': ''},
                {'Name': 'order_purchase_timestamp', 'Type': 'string', 'Comment': ''},
                {'Name': 'order_approved_at', 'Type': 'string', 'Comment': ''},
                {'Name': 'order_delivered_carrier_date', 'Type': 'string', 'Comment': ''},
                {'Name': 'order_delivered_customer_date', 'Type': 'string', 'Comment': ''},
                {'Name': 'order_estimated_delivery_date', 'Type': 'string', 'Comment': ''}

            ],
            'Location': 's3://patrick-olist-data/olist_orders_dataset.csv',
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'Compressed': False,
            'SerdeInfo': {'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                          },
            'Parameters': {'skip.header.line.count': '1'}

        },
        'Parameters': {'skip.header.line.count': '1'},
        'TableType': "EXTERNAL_TABLE"})

print(json.dumps(response, indent=4, sort_keys=True, default=str))