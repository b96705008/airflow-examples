LOAD DATA INFILE '/Users/rogerkuo/Developer/data/sales/sales_train.csv'
INTO TABLE `airflow_bi`.`daily_sales`
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@sale_date,date_block_num,shop_id,item_id,item_price,item_cnt_day)
SET sale_date = STR_TO_DATE(@sale_date, '%d.%m.%Y');