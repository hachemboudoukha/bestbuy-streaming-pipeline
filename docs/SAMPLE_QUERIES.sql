-- Sample SQL Queries for Apache Druid

-- 1. Recent Transactions (Last Hour)
SELECT 
    __time,
    customer_id,
    product_name,
    brand,
    price,
    quantity
FROM "ecommerce-topic-1"
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
LIMIT 100;

-- 2. Total Revenue by Product (Last 24 Hours)
SELECT 
    product_name,
    brand,
    SUM(price * quantity) as total_revenue,
    SUM(quantity) as units_sold
FROM "ecommerce-topic-1"
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
GROUP BY product_name, brand
ORDER BY total_revenue DESC
LIMIT 10;

-- 3. Transactions per Minute (Traffic)
SELECT 
    TIME_FLOOR(__time, 'PT1M') as minute,
    COUNT(*) as transaction_count
FROM "ecommerce-topic-1"
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
GROUP BY 1
ORDER BY 1 DESC;

-- 4. Average Order Value
SELECT 
    AVG(price * quantity) as avg_order_value
FROM "ecommerce-topic-1"
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY;

-- 5. Top Customers by Spend
SELECT 
    customer_id,
    SUM(price * quantity) as total_spend
FROM "ecommerce-topic-1"
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
GROUP BY customer_id
ORDER BY total_spend DESC
LIMIT 20;

-- 6. Revenue by Brand
SELECT 
    brand,
    SUM(price * quantity) as total_revenue,
    COUNT(*) as transactions
FROM "ecommerce-topic-1"
GROUP BY brand
ORDER BY total_revenue DESC;

-- 7. Sales by Screen Size
SELECT 
    screen_size,
    COUNT(*) as sales_count
FROM "ecommerce-topic-1"
GROUP BY screen_size
ORDER BY sales_count DESC;
