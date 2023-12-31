USE CATALOG o9o9uccatalog;
--q50.sql--

SELECT /*TOP 100*/ s_store_name,
       s_company_id,
       s_street_number,
       s_street_name,
       s_street_type,
       s_suite_number,
       s_city,
       s_county,
       s_state,
       s_zip ,
       sum(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk <= 30) THEN 1
               ELSE 0
           END) AS 30_days ,
       sum(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk > 30)
                    AND (sr_returned_date_sk - ss_sold_date_sk <= 60) THEN 1
               ELSE 0
           END) AS 31_60_days ,
       sum(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk > 60)
                    AND (sr_returned_date_sk - ss_sold_date_sk <= 90) THEN 1
               ELSE 0
           END) AS 61_90_days ,
       sum(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk > 90)
                    AND (sr_returned_date_sk - ss_sold_date_sk <= 120) THEN 1
               ELSE 0
           END) AS 91_120_days ,
       sum(CASE
               WHEN (sr_returned_date_sk - ss_sold_date_sk > 120) THEN 1
               ELSE 0
           END) AS 120_over_days
FROM TPCDS.store_sales,
     TPCDS.store_returns,
     TPCDS.store,
     TPCDS.date_dim d1,
     TPCDS.date_dim d2
WHERE d2.d_year = 2001
  AND d2.d_moy = 8
  AND ss_ticket_number = sr_ticket_number
  AND ss_item_sk = sr_item_sk
  AND ss_sold_date_sk = d1.d_date_sk
  AND sr_returned_date_sk = d2.d_date_sk
  AND ss_customer_sk = sr_customer_sk
  AND ss_store_sk = s_store_sk
GROUP BY s_store_name,
         s_company_id,
         s_street_number,
         s_street_name,
         s_street_type,
         s_suite_number,
         s_city,
         s_county,
         s_state,
         s_zip
ORDER BY s_store_name,
         s_company_id,
         s_street_number,
         s_street_name,
         s_street_type,
         s_suite_number,
         s_city,
         s_county,
         s_state,
         s_zip
LIMIT 100
-- OPTION (LABEL = 'q50')
		 