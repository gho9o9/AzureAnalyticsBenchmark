--q70.sql--

SELECT *
FROM
(
SELECT TOP 100 sum(ss_net_profit) AS total_sum,
       s_state,
       s_county ,
       grouping(s_state)+grouping(s_county) AS lochierarchy ,
       rank() OVER (PARTITION BY grouping(s_state)+grouping(s_county),
                                 CASE
                                     WHEN grouping(s_county) = 0 THEN s_state
                                 END
                    ORDER BY sum(ss_net_profit) DESC) AS rank_within_parent
FROM dbo.store_sales,
     dbo.date_dim d1,
     dbo.store
WHERE d1.d_month_seq BETWEEN 1200 AND 1200+11
  AND d1.d_date_sk = ss_sold_date_sk
  AND s_store_sk = ss_store_sk
  AND s_state IN
    (SELECT s_state
     FROM
       (SELECT s_state AS s_state,
               rank() OVER (PARTITION BY s_state
                            ORDER BY sum(ss_net_profit) DESC) AS ranking
        FROM dbo.store_sales,
             dbo.store,
             dbo.date_dim
        WHERE d_month_seq BETWEEN 1200 AND 1200+11
          AND d_date_sk = ss_sold_date_sk
          AND s_store_sk = ss_store_sk
        GROUP BY s_state) tmp1
     WHERE ranking <= 5)
GROUP BY rollup(s_state, s_county)
) AS a
ORDER BY lochierarchy DESC ,
         CASE
             WHEN lochierarchy = 0 THEN s_state
         END ,
         rank_within_parent
OPTION (LABEL = 'q70')
		 