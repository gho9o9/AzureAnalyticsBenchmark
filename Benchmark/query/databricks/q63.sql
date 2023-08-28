USE CATALOG o9o9uccatalog;
--q63.sql--

SELECT /*TOP 100*/ *
FROM
  (SELECT i_manager_id ,
          sum(ss_sales_price) sum_sales ,
          avg(sum(ss_sales_price)) OVER (PARTITION BY i_manager_id) avg_monthly_sales
   FROM TPCDS.item ,
        TPCDS.store_sales ,
        TPCDS.date_dim ,
        TPCDS.store
   WHERE ss_item_sk = i_item_sk
     AND ss_sold_date_sk = d_date_sk
     AND ss_store_sk = s_store_sk
     AND d_month_seq IN (1200,
                         1200+1,
                         1200+2,
                         1200+3,
                         1200+4,
                         1200+5,
                         1200+6,
                         1200+7,
                         1200+8,
                         1200+9,
                         1200+10,
                         1200+11)
     AND ((trim(i_category) IN ('Books',
                          'Children',
                          'Electronics')
           AND trim(i_class) IN ('personal',
                           'portable',
                           'reference',
                           'self-help')
           AND trim(i_brand) IN ('scholaramalgamalg #14',
                           'scholaramalgamalg #7',
                           'exportiunivamalg #9',
                           'scholaramalgamalg #9')) or(trim(i_category) IN ('Women', 'Music', 'Men')
                                                       AND trim(i_class) IN ('accessories', 'classical', 'fragrances', 'pants')
                                                       AND trim(i_brand) IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')))
   GROUP BY i_manager_id,
            d_moy) tmp1
WHERE CASE
          WHEN avg_monthly_sales > 0 THEN ABS (sum_sales - avg_monthly_sales) / avg_monthly_sales
          ELSE NULL
      END > 0.1
ORDER BY i_manager_id ,
         avg_monthly_sales ,
         sum_sales
LIMIT 100
-- OPTION (LABEL = 'q63')
