-- Daily sales overview
select
    date_day,
    order_count,
    item_value,
    freight_value,
    payment_value,
    average_review_score
from OLIST_DWH.GOLD.MART_DAILY_SALES
order by date_day;

-- Revenue and delivery quality by product category
select
    category.category_name_english,
    count(distinct items.order_id) as orders,
    sum(items.price) as item_value,
    avg(orders.delivery_days) as average_delivery_days,
    avg(orders.average_review_score) as average_review_score
from OLIST_DWH.GOLD.FACT_ORDER_ITEMS as items
join OLIST_DWH.GOLD.DIM_PRODUCT as product
    on items.product_key = product.product_key
left join OLIST_DWH.GOLD.DIM_PRODUCT_CATEGORY as category
    on product.product_category_key = category.product_category_key
join OLIST_DWH.GOLD.FACT_ORDERS as orders
    on items.order_id = orders.order_id
group by category.category_name_english
order by item_value desc;

-- Seller performance with geography normalized outside the seller dimension
select
    geography.state_code,
    geography.city,
    count(distinct items.seller_key) as sellers,
    count(distinct items.order_id) as orders,
    sum(items.gross_item_value) as gross_item_value
from OLIST_DWH.GOLD.FACT_ORDER_ITEMS as items
join OLIST_DWH.GOLD.DIM_SELLER as seller
    on items.seller_key = seller.seller_key
left join OLIST_DWH.GOLD.DIM_GEOGRAPHY as geography
    on seller.geography_key = geography.geography_key
group by geography.state_code, geography.city
order by gross_item_value desc;

