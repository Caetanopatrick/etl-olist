from pyathenajdbc import connect
import pandas as pd

"""
Nesse script utilizamos a biblioteca pyathenajdbc para realizar uma comunicação jdbc com o database dos arquivos parquet.
Com essa comunicação respondemos as perguntas abaixo que foram colocadas no desafio técnico:

Valor total das vendas e dos fretes por vendedor, produto e ordem de venda
Valor de venda por tipo de pagamento
Média das notas de cada produto e vendedor
Venda por cliente e por status de envio
Quantidade e valor das vendas aprovadas por dia, mês, ano...
Quantidade e valor das vendas emitidas por dia, mês, ano...
Tempo para aprovar um pedido
Tempo para envio de um review após a venda
Venda por produto e tipo do produto
Venda por vendedor e cidade do vendedor
Venda por cliente, cidade do cliente e estado

"""


conn = connect(S3OutputLocation='s3://athena-queries-patrick/',
               AwsRegion='us-east-1')
print("Valor total das vendas e dos fretes por vendedor")

query = """
SELECT seller_id,
    sum(order_item_id * price) as total_das_vendas,
	sum(freight_value) as total_frete
FROM "olist_parquet"."olist_order_items_dataset_parquet"
GROUP BY seller_id;
            """

df = pd.read_sql(query, conn)

print(df)

print("Valor total das vendas e dos fretes por produto")

query = """
SELECT product_id,
	sum(order_item_id * price) as total_das_vendas,
	sum(freight_value) as total_frete
FROM "olist_parquet"."olist_order_items_dataset_parquet"
GROUP BY product_id;
"""

df = pd.read_sql(query, conn)

print(df)

print("Valor total das vendas e dos fretes por ordem de venda")

query = """
SELECT order_id,
	sum(order_item_id * price) as total_das_vendas,
	sum(freight_value) as total_frete
FROM "olist_parquet"."olist_order_items_dataset_parquet"
GROUP BY order_id;
"""

df = pd.read_sql(query, conn)

print(df)

print("Valor de venda por tipo de pagamento")
query = """
SELECT payment_type,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet"."olist_order_payments_dataset_parquet" ON "olist_parquet"."olist_order_items_dataset_parquet".order_id = "olist_parquet"."olist_order_payments_dataset_parquet".order_id
GROUP BY payment_type;
"""
df = pd.read_sql(query, conn)

print(df)

print("Média das notas de cada produto")

query = """
SELECT product_id,
	avg(review_score) as media_reviews
FROM "olist_parquet"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet"."olist_order_reviews_dataset_parquet" ON "olist_parquet"."olist_order_items_dataset_parquet".order_id = "olist_parquet"."olist_order_reviews_dataset_parquet".order_id
GROUP BY product_id;
"""

df = pd.read_sql(query, conn)

print(df)

print("Média das notas de cada vendedor")

query = """
SELECT seller_id,
	avg(review_score) as media_reviews
FROM "olist_parquet"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet"."olist_order_reviews_dataset_parquet" ON "olist_parquet"."olist_order_items_dataset_parquet".order_id = "olist_parquet"."olist_order_reviews_dataset_parquet".order_id
GROUP BY seller_id;
"""
df = pd.read_sql(query, conn)

print(df)

print("Venda por cliente")

query = """
SELECT customer_id,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet"."olist_orders_dataset_parquet" ON "olist_parquet"."olist_order_items_dataset_parquet".order_id = "olist_parquet"."olist_orders_dataset_parquet".order_id
GROUP BY customer_id;
"""

df = pd.read_sql(query, conn)

print(df)

print("Venda por status de envio")

query = """
SELECT order_status,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet"."olist_orders_dataset_parquet" ON "olist_parquet"."olist_order_items_dataset_parquet".order_id = "olist_parquet"."olist_orders_dataset_parquet".order_id
GROUP BY order_status;
"""
df = pd.read_sql(query, conn)

print(df)

print("Quantidade e valor das vendas aprovadas por dia...")

query = """
SELECT DATE(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s')) as dia,
    sum(order_item_id) as quantidade,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet"."olist_orders_dataset_parquet" ON "olist_parquet"."olist_order_items_dataset_parquet".order_id = "olist_parquet"."olist_orders_dataset_parquet".order_id
WHERE order_status = 'approved'
GROUP BY DATE(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
ORDER BY DATE(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
"""
df = pd.read_sql(query, conn)

print(df)

print("Quantidade e valor das vendas aprovadas por mês..")

query = """
SELECT MONTH(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s')) as mes,
    sum(order_item_id) as quantidade,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet"."olist_orders_dataset_parquet" ON "olist_parquet"."olist_order_items_dataset_parquet".order_id = "olist_parquet"."olist_orders_dataset_parquet".order_id
WHERE order_status = 'approved'
GROUP BY MONTH(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
ORDER BY MONTH(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
"""
df = pd.read_sql(query, conn)

print(df)

print("Quantidade e valor das vendas aprovadas por ano..")

query = """
SELECT YEAR(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s')) as ano,
    sum(order_item_id) as quantidade,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet"."olist_orders_dataset_parquet" ON "olist_parquet"."olist_order_items_dataset_parquet".order_id = "olist_parquet"."olist_orders_dataset_parquet".order_id
WHERE order_status = 'approved'
GROUP BY YEAR(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
ORDER BY YEAR(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
"""
df = pd.read_sql(query, conn)

print(df)

print("Quantidade e valor das vendas emitidas por dia...")

query = """
SELECT DATE(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s')) as dia,
    sum(order_item_id) as quantidade,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet"."olist_orders_dataset_parquet" ON "olist_parquet"."olist_order_items_dataset_parquet".order_id = "olist_parquet"."olist_orders_dataset_parquet".order_id
WHERE order_status = 'invoiced'
GROUP BY DATE(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
ORDER BY DATE(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
"""
df = pd.read_sql(query, conn)

print(df)


print("Quantidade e valor das vendas emitidas por mês..")

query = """
SELECT MONTH(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s')) as mes,
    sum(order_item_id) as quantidade,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet"."olist_orders_dataset_parquet" ON "olist_parquet"."olist_order_items_dataset_parquet".order_id = "olist_parquet"."olist_orders_dataset_parquet".order_id
WHERE order_status = 'invoiced'
GROUP BY MONTH(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
ORDER BY MONTH(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
"""

df = pd.read_sql(query, conn)

print(df)


print("Quantidade e valor das vendas emitidas por ano..")

query = """
SELECT YEAR(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s')) as ano,
    sum(order_item_id) as quantidade,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet"."olist_orders_dataset_parquet" ON "olist_parquet"."olist_order_items_dataset_parquet".order_id = "olist_parquet"."olist_orders_dataset_parquet".order_id
WHERE order_status = 'invoiced'
GROUP BY YEAR(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
ORDER BY YEAR(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
"""
df = pd.read_sql(query, conn)

print(df)

print("Tempo para aprovar um pedido")

query = """
SELECT AVG(
		TRY(date_diff(
			'hour',
			date_parse(order_purchase_timestamp, '%Y-%m-%d %H:%i:%s'),
			date_parse(order_approved_at, '%Y-%m-%d %H:%i:%s')
		))
	) as tempo_aprovacao_horas
FROM "olist_parquet"."olist_orders_dataset_parquet" 
"""
df = pd.read_sql(query, conn)

print(df)

print("Tempo para envio de um review após a venda")

query = """
SELECT AVG(
		TRY(date_diff(
			'day',
			date_parse(order_purchase_timestamp, '%Y-%m-%d %H:%i:%s'),
			date_parse(review_creation_date, '%Y-%m-%d %H:%i:%s')
		))
	) as tempo_envio_review_dias
FROM "olist_parquet"."olist_orders_dataset_parquet"
	JOIN "olist_parquet"."olist_order_reviews_dataset_parquet" ON "olist_parquet"."olist_orders_dataset_parquet".order_id = "olist_parquet"."olist_order_reviews_dataset_parquet".order_id
"""
df = pd.read_sql(query, conn)

print(df)

print("Venda por produto")

query = """
SELECT "olist_parquet"."olist_order_items_dataset_parquet".product_id,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet"."olist_products_dataset_parquet" ON "olist_parquet"."olist_order_items_dataset_parquet".product_id = "olist_parquet"."olist_products_dataset_parquet".product_id
GROUP BY "olist_parquet"."olist_order_items_dataset_parquet".product_id;
"""
df = pd.read_sql(query, conn)

print(df)

print("Venda por tipo do produto")

query = """
SELECT "olist_parquet"."olist_products_dataset_parquet".product_category_name,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet"."olist_products_dataset_parquet" ON "olist_parquet"."olist_order_items_dataset_parquet".product_id = "olist_parquet"."olist_products_dataset_parquet".product_id
GROUP BY "olist_products_dataset_parquet".product_category_name;
"""
df = pd.read_sql(query, conn)

print(df)

print("Venda por vendedor")

query = """
SELECT seller_id,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet"."olist_order_items_dataset_parquet"
GROUP BY seller_id;
"""

df = pd.read_sql(query, conn)

print(df)

print("Venda por cidade do vendedor")

query = """
SELECT seller_city, sum(order_item_id * price) total_das_vendas
FROM "olist_parquet"."olist_order_items_dataset_parquet"
JOIN "olist_parquet"."olist_sellers_dataset_parquet"
ON "olist_parquet"."olist_order_items_dataset_parquet".seller_id = "olist_parquet"."olist_sellers_dataset_parquet".seller_id
GROUP BY seller_city; """

df = pd.read_sql(query, conn)

print(df)

print("Venda por cliente")
query = """
SELECT "olist_parquet"."olist_orders_dataset_parquet".customer_id,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet"."olist_orders_dataset_parquet"
	JOIN "olist_parquet"."olist_customers_dataset_parquet" ON "olist_parquet"."olist_orders_dataset_parquet".customer_id = "olist_parquet"."olist_customers_dataset_parquet".customer_id
	JOIN "olist_parquet"."olist_order_items_dataset_parquet" ON "olist_parquet"."olist_order_items_dataset_parquet".order_id = "olist_parquet"."olist_orders_dataset_parquet".order_id
GROUP BY "olist_parquet"."olist_orders_dataset_parquet".customer_id;
"""

df = pd.read_sql(query, conn)

print(df)

print("Venda por cidade do cliente")

query = """
SELECT customer_city,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet"."olist_orders_dataset_parquet"
	JOIN "olist_parquet"."olist_customers_dataset_parquet" ON "olist_parquet"."olist_orders_dataset_parquet".customer_id = "olist_parquet"."olist_customers_dataset_parquet".customer_id
	JOIN "olist_parquet"."olist_order_items_dataset_parquet" ON "olist_parquet"."olist_order_items_dataset_parquet".order_id = "olist_parquet"."olist_orders_dataset_parquet".order_id
GROUP BY customer_city;
"""
df = pd.read_sql(query, conn)

print(df)

print("Venda por estado do cliente")

query = """
SELECT customer_state,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet"."olist_orders_dataset_parquet"
	JOIN "olist_parquet"."olist_customers_dataset_parquet" ON "olist_parquet"."olist_orders_dataset_parquet".customer_id = "olist_parquet"."olist_customers_dataset_parquet".customer_id
	JOIN "olist_parquet"."olist_order_items_dataset_parquet" ON "olist_parquet"."olist_order_items_dataset_parquet".order_id = "olist_parquet"."olist_orders_dataset_parquet".order_id
GROUP BY customer_state;
"""

df = pd.read_sql(query, conn)

print(df)

conn.close()
