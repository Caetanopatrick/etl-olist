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

dev_or_prod = "dev"


conn = connect(S3OutputLocation='s3://athena-queries-patrick/',
               AwsRegion='us-east-1')



perguntas=["Valor total das vendas e dos fretes por vendedor", "Valor total das vendas e dos fretes por produto",
           "Valor total das vendas e dos fretes por ordem de venda", "Valor de venda por tipo de pagamento",
           "Média das notas de cada produto","Média das notas de cada vendedor", "Venda por cliente",
            "Venda por status de envio", "Quantidade e valor das vendas aprovadas por dia...",
            "Quantidade e valor das vendas aprovadas por mês..", "Quantidade e valor das vendas aprovadas por ano..",
            "Quantidade e valor das vendas emitidas por dia...", "Quantidade e valor das vendas emitidas por mês..",
            "Quantidade e valor das vendas emitidas por ano..", "Tempo para aprovar um pedido", "Tempo para envio de um review após a venda",
            "Venda por produto", "Venda por tipo do produto", "Venda por vendedor", "Venda por cidade do vendedor", "Venda por cliente",
            "Venda por cidade do cliente", "Venda por estado do cliente"]


query_0 = f"""
SELECT seller_id,
    sum(order_item_id * price) as total_das_vendas,
	sum(freight_value) as total_frete
FROM "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet"
GROUP BY seller_id;
            """


query_1 = f"""
SELECT product_id,
	sum(order_item_id * price) as total_das_vendas,
	sum(freight_value) as total_frete
FROM "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet"
GROUP BY product_id;
"""

query_2 = f"""
SELECT order_id,
	sum(order_item_id * price) as total_das_vendas,
	sum(freight_value) as total_frete
FROM "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet"
GROUP BY order_id;
"""

query_3 = f"""
SELECT payment_type,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet_{dev_or_prod}"."olist_order_payments_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".order_id = "olist_parquet_{dev_or_prod}"."olist_order_payments_dataset_parquet".order_id
GROUP BY payment_type;
"""

query_4 = f"""
SELECT product_id,
	avg(review_score) as media_reviews
FROM "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet_{dev_or_prod}"."olist_order_reviews_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".order_id = "olist_parquet_{dev_or_prod}"."olist_order_reviews_dataset_parquet".order_id
GROUP BY product_id;
"""

query_5 = f"""
SELECT seller_id,
	avg(review_score) as media_reviews
FROM "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet_{dev_or_prod}"."olist_order_reviews_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".order_id = "olist_parquet_{dev_or_prod}"."olist_order_reviews_dataset_parquet".order_id
GROUP BY seller_id;
"""

query_6 = f"""
SELECT customer_id,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".order_id = "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet".order_id
GROUP BY customer_id;
"""


query_7 = f"""
SELECT order_status,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".order_id = "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet".order_id
GROUP BY order_status;
"""

query_8 = f"""
SELECT DATE(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s')) as dia,
    sum(order_item_id) as quantidade,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".order_id = "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet".order_id
WHERE order_status = 'approved'
GROUP BY DATE(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
ORDER BY DATE(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
"""

query_9 = f"""
SELECT MONTH(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s')) as mes,
    sum(order_item_id) as quantidade,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".order_id = "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet".order_id
WHERE order_status = 'approved'
GROUP BY MONTH(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
ORDER BY MONTH(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
"""

query_10 = f"""
SELECT YEAR(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s')) as ano,
    sum(order_item_id) as quantidade,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".order_id = "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet".order_id
WHERE order_status = 'approved'
GROUP BY YEAR(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
ORDER BY YEAR(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
"""

query_11 = f"""
SELECT DATE(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s')) as dia,
    sum(order_item_id) as quantidade,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".order_id = "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet".order_id
WHERE order_status = 'invoiced'
GROUP BY DATE(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
ORDER BY DATE(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
"""

query_12 = f"""
SELECT MONTH(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s')) as mes,
    sum(order_item_id) as quantidade,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".order_id = "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet".order_id
WHERE order_status = 'invoiced'
GROUP BY MONTH(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
ORDER BY MONTH(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
"""

query_13 = f"""
SELECT YEAR(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s')) as ano,
    sum(order_item_id) as quantidade,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".order_id = "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet".order_id
WHERE order_status = 'invoiced'
GROUP BY YEAR(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
ORDER BY YEAR(date_parse(order_purchase_timestamp,'%Y-%m-%d %H:%i:%s'))
"""

query_14 = f"""
SELECT AVG(
		TRY(date_diff(
			'hour',
			date_parse(order_purchase_timestamp, '%Y-%m-%d %H:%i:%s'),
			date_parse(order_approved_at, '%Y-%m-%d %H:%i:%s')
		))
	) as tempo_aprovacao_horas
FROM "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet" 
"""

query_15 = f"""
SELECT AVG(
		TRY(date_diff(
			'day',
			date_parse(order_purchase_timestamp, '%Y-%m-%d %H:%i:%s'),
			date_parse(review_creation_date, '%Y-%m-%d %H:%i:%s')
		))
	) as tempo_envio_review_dias
FROM "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet"
	JOIN "olist_parquet_{dev_or_prod}"."olist_order_reviews_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet".order_id = "olist_parquet_{dev_or_prod}"."olist_order_reviews_dataset_parquet".order_id
"""
query_16 = f"""
SELECT "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".product_id,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet_{dev_or_prod}"."olist_products_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".product_id = "olist_parquet_{dev_or_prod}"."olist_products_dataset_parquet".product_id
GROUP BY "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".product_id;
"""

query_17 = f"""
SELECT "olist_parquet_{dev_or_prod}"."olist_products_dataset_parquet".product_category_name,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet"
	JOIN "olist_parquet_{dev_or_prod}"."olist_products_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".product_id = "olist_parquet_{dev_or_prod}"."olist_products_dataset_parquet".product_id
GROUP BY "olist_products_dataset_parquet".product_category_name;
"""

query_18 = f"""
SELECT seller_id,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet"
GROUP BY seller_id;
"""

query_19 = f"""
SELECT seller_city, sum(order_item_id * price) total_das_vendas
FROM "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet"
JOIN "olist_parquet_{dev_or_prod}"."olist_sellers_dataset_parquet"
ON "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".seller_id = "olist_parquet_{dev_or_prod}"."olist_sellers_dataset_parquet".seller_id
GROUP BY seller_city; """

query_20 = f"""
SELECT "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet".customer_id,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet"
	JOIN "olist_parquet_{dev_or_prod}"."olist_customers_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet".customer_id = "olist_parquet_{dev_or_prod}"."olist_customers_dataset_parquet".customer_id
	JOIN "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".order_id = "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet".order_id
GROUP BY "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet".customer_id;
"""

query_21 = f"""
SELECT customer_city,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet"
	JOIN "olist_parquet_{dev_or_prod}"."olist_customers_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet".customer_id = "olist_parquet_{dev_or_prod}"."olist_customers_dataset_parquet".customer_id
	JOIN "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".order_id = "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet".order_id
GROUP BY customer_city;
"""

query_22 = f"""
SELECT customer_state,
	sum(order_item_id * price) as total_das_vendas
FROM "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet"
	JOIN "olist_parquet_{dev_or_prod}"."olist_customers_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet".customer_id = "olist_parquet_{dev_or_prod}"."olist_customers_dataset_parquet".customer_id
	JOIN "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet" ON "olist_parquet_{dev_or_prod}"."olist_order_items_dataset_parquet".order_id = "olist_parquet_{dev_or_prod}"."olist_orders_dataset_parquet".order_id
GROUP BY customer_state;
"""

queries =[query_0, query_1, query_2, query_3, query_4, query_5, query_6, query_7, query_8, query_9,
          query_10, query_11, query_12, query_13, query_14, query_15, query_16, query_17, query_18, query_19,
          query_20, query_21, query_22]

for index, query in enumerate(queries):
    print(perguntas[index])
    df = pd.read_sql(query, conn)
    print(df)
conn.close()
