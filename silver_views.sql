-- ADD THE ATHENA SQL SCRIPT HERE WHICH CREATES THE `silver_views` TABLE
   CREATE TABLE dgulacsy_homework.silver_views
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://dominik-de4/de4/views_silver'
    ) AS SELECT article, view, rank, date FROM dgulacsy_homework.bronze_edits