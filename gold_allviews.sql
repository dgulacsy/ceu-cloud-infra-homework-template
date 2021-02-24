-- ADD THE ATHENA SQL SCRIPT HERE WHICH CREATES THE `gold_allviews` TABLE
   CREATE TABLE dgulacsy_homework.gold_allviews
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://dominik-de4/de4/gold_allviews'
    ) AS 
    SELECT article, SUM(views) as total_top_views, MAX(rank) as top_rank, COUNT(article) as ranked_days 
    FROM "dgulacsy_homework"."silver_views" 
    GROUP BY article 
    ORDER BY total_top_views DESC;