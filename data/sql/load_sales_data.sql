LOAD DATA INFILE '/Users/rogerkuo/Developer/data/sales/sales_train.csv'
INTO TABLE `airflow_db`.`daily_sales`
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@sale_date,date_block_num,shop_id,item_id,item_price,item_cnt_day)
SET sale_date = STR_TO_DATE(@sale_date, '%d.%m.%Y');


LOAD DATA INFILE '/Users/rogerkuo/Developer/data/sales/items.csv'
INTO TABLE `airflow_db`.`items`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


LOAD DATA INFILE '/Users/rogerkuo/Developer/data/sales/item_categories.csv'
INTO TABLE `airflow_db`.`item_categories`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


LOAD DATA INFILE '/Users/rogerkuo/Developer/data/sales/shops.csv'
INTO TABLE `airflow_db`.`shops`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;