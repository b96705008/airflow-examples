DROP TABLE IF EXISTS airflow_bi.monthly_item_sales;

CREATE TABLE airflow_bi.monthly_item_sales
AS (
	SELECT 
		S.item_id as item_id,
		month(S.sale_date) as sales_month,
		sum(S.item_cnt_day) as monthly_sales_cnt
	FROM airflow_db.daily_sales S
	WHERE S.sale_date between '2014-01-01' and '2014-12-31'
	GROUP BY item_id, sales_month
	ORDER BY item_id, sales_month
);

