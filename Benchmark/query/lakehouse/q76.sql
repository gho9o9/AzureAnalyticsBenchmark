--q76.sql--

SELECT TOP 100 channel,
       col_name,
       d_year,
       d_qoy,
       i_category,
       COUNT(*) sales_cnt,
       SUM(ext_sales_price) sales_amt
FROM
  (SELECT 'store' AS channel,
          ss_store_sk col_name,
          d_year,
          d_qoy,
          i_category,
          ss_ext_sales_price ext_sales_price
   FROM dbo.store_sales,
        dbo.item,
        dbo.date_dim
   WHERE ss_store_sk IS NULL
     AND ss_sold_date_sk=d_date_sk
     AND ss_item_sk=i_item_sk
   UNION ALL SELECT 'web' AS channel,
                    ws_ship_customer_sk col_name,
                    d_year,
                    d_qoy,
                    i_category,
                    ws_ext_sales_price ext_sales_price
   FROM dbo.web_sales,
        dbo.item,
        dbo.date_dim
   WHERE ws_ship_customer_sk IS NULL
     AND ws_sold_date_sk=d_date_sk
     AND ws_item_sk=i_item_sk
   UNION ALL SELECT 'catalog' AS channel,
                    cs_ship_addr_sk col_name,
                    d_year,
                    d_qoy,
                    i_category,
                    cs_ext_sales_price ext_sales_price
   FROM dbo.catalog_sales,
        dbo.item,
        dbo.date_dim
   WHERE cs_ship_addr_sk IS NULL
     AND cs_sold_date_sk=d_date_sk
     AND cs_item_sk=i_item_sk) foo
GROUP BY channel,
         col_name,
         d_year,
         d_qoy,
         i_category
ORDER BY channel,
         col_name,
         d_year,
         d_qoy,
         i_category
OPTION (LABEL = 'q76')
		 