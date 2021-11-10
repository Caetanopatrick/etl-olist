# etl-olist

etl_olist é um pipeline ETL com fluxo S3 -> Glue -> S3 a fim de criar um data warehouse com arquivos parquet, que são muito mais performáticos na análise do Athena e também econômicos por reduzir o tamanho dos arquivos.


## Crawlers

create_databases.py: cria o database olist_csv, olist_parquet_dev e olist_parquet_prod para uso nos crawlers do glue.

create_csv_crawler.py: cria o crawler para definir o schema das tabelas csv.

run_csv_crawler.py: executa o crawler para definir o schema das tabelas csv.

create_orders_table.py: cria manualmente a tabela orders que não é identificada corretamente pelos classifiers do crawler.

create_parquet_crawler.py: cria o crawler para definir o schema das tabelas parquet geradas após o job ETL.

run_parquet_crawler.py:  executa o crawler para definir o schema das tabelas parquet geradas após o job ETL.

## ETL job

create_csv_to_parquet.py: cria o job ETL que irá converter as tabelas para parquet, adicionando incrementalmente os dados.

script_csv_to_parquet.py: define o job que converte as tabelas para parquet, além disso, divide elas em desenvolvimento ou produção, como definido em variável do script. E move as tabelas para o bucket escolhido.

run_parquet_crawler.py:  executa o glue job que converte as tabelas para parquet.

create_trigger.py: Cria o trigger que executa o job diariamente as 03:00.

## Athena

athena_queries.py: responde as perguntas impostas no desafio através de queries no athena dos arquivos parquet, com conexão jdbc pelo python, podendo escolher se o database da query é o de desenvolvimento ou produção

## CI/CD

Implementado ciclo básico de CI/CD que altera o script ETL com cada novo commit.

```
