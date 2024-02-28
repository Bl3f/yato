SELECT product_name, count()
FROM source_orders
GROUP BY product_name