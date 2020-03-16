SELECT 
	S.shop_id as shop_id,
    C.item_category_id as item_category_id,
	sum(S.item_cnt_day) as yearly_sales_cnt
FROM airflow_db.daily_sales S
JOIN items I
	on S.item_id = I.item_id
JOIN airflow_db.item_categories C
	on I.item_category_id = C.item_category_id
WHERE S.sale_date between '2015-01-01' and '2015-12-31'
GROUP BY shop_id, item_category_id
ORDER BY yearly_sales_cnt desc;