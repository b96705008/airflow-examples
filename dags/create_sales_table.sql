CREATE TABLE IF NOT EXISTS `airflow_bi`.`daily_sales` (
  `sale_date` date NOT NULL,
  `date_block_num` int(11) NOT NULL,
  `shop_id` int(11) NOT NULL,
  `item_id` int(11) NOT NULL,
  `item_price` float NOT NULL,
  `item_cnt_day` float NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;