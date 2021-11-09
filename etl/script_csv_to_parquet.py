import sys

import boto3

from awsglue.transforms import *

from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext

from awsglue.context import GlueContext

from awsglue.job import Job

"""
Esse é o script que está no s3 e ele define o ETL job a ser rodado.
Aqui pegamos todas as tabelas do dataset e transformamos para parquet.
Também mudamos a extensão do seu nome para "_parquet" 
Os resultados são salvos no bucket s3://patrick-olist-parquet-data/

"""

DEV_OR_PROD = "DEV"

## @params: [JOB_NAME]

args = getResolvedOptions(sys.argv, ['JOB_NAME'])



sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(args['JOB_NAME'], args)



client = boto3.client('glue',region_name='us-east-1')



databaseName = 'olist_csv'

print('\ndatabaseName: ' + databaseName)



Tables = client.get_tables( DatabaseName = databaseName )

tableList = Tables ['TableList']


for table in tableList:

   tableName = table['Name']

   print('\n-- tableName: '+tableName)

   datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "olist_csv", table_name = tableName, transformation_ctx = "datasource0")
   if DEV_OR_PROD == "DEV":
      datasink4 = glueContext.write_dynamic_frame.from_options(frame = datasource0, connection_type = "s3", connection_options = {"path": "s3://patrick-olist-parquet-data/DEV/"+ tableName.replace("csv", "parquet") + "/"}, format = "parquet", transformation_ctx = "datasink4")
   if DEV_OR_PROD == "PROD":
      datasink4 = glueContext.write_dynamic_frame.from_options(frame = datasource0, connection_type = "s3", connection_options = {"path": "s3://patrick-olist-parquet-data/PROD/"+ tableName.replace("csv", "parquet") + "/"}, format = "parquet", transformation_ctx = "datasink4")



job.commit()
