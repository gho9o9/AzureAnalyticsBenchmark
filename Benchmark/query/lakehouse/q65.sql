--q65.sql--

SELECT TOP 100 s_store_name,
       i_item_desc,
       sc.revenue,
       i_current_price,
       i_wholesale_cost,
       i_brand
FROM dbo.store,
     dbo.item,

  (SELECT ss_store_sk,
          avg(revenue) AS ave
   FROM
     (SELECT ss_store_sk,
             ss_item_sk,
             sum(ss_sales_price) AS revenue
      FROM dbo.store_sales,
           dbo.date_dim
      WHERE ss_sold_date_sk = d_date_sk
        AND d_month_seq BETWEEN 1176 AND 1176+11
      GROUP BY ss_store_sk,
               ss_item_sk) sa
   GROUP BY ss_store_sk) sb,

  (SELECT ss_store_sk,
          ss_item_sk,
          sum(ss_sales_price) AS revenue
   FROM dbo.store_sales,
        dbo.date_dim
   WHERE ss_sold_date_sk = d_date_sk
     AND d_month_seq BETWEEN 1176 AND 1176+11
   GROUP BY ss_store_sk,
            ss_item_sk) sc
WHERE sb.ss_store_sk = sc.ss_store_sk
  AND sc.revenue <= 0.1 * sb.ave
  AND s_store_sk = sc.ss_store_sk
  AND i_item_sk = sc.ss_item_sk
ORDER BY s_store_name,
         i_item_desc
OPTION (LABEL = 'q65')
		 