--q6.sql--

SELECT TOP 100 state,
       cnt
FROM
  (SELECT a.ca_state state,
          count(*) cnt
   FROM dbo.customer_address a,
        dbo.customer c,
        dbo.store_sales s,
        dbo.date_dim d,
        dbo.item i
   WHERE a.ca_address_sk = c.c_current_addr_sk
     AND c.c_customer_sk = s.ss_customer_sk
     AND s.ss_sold_date_sk = d.d_date_sk
     AND s.ss_item_sk = i.i_item_sk
     AND d.d_month_seq =
       (SELECT DISTINCT (d_month_seq)
        FROM dbo.date_dim
        WHERE d_year = 2001
          AND d_moy = 1)
     AND i.i_current_price > 1.2 *
       (SELECT avg(j.i_current_price)
        FROM dbo.item j
        WHERE j.i_category = i.i_category)
   GROUP BY a.ca_state) x
WHERE cnt >= 10
ORDER BY cnt
OPTION (LABEL = 'q6')
