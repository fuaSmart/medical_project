-- models/marts/dim_dates.sql

--SELECT
   -- DISTINCT CAST(message_date AS DATE) AS date_id, 
   -- EXTRACT(YEAR FROM message_date) AS year,
   -- EXTRACT(MONTH FROM message_date) AS month,
  --  TO_CHAR(message_date, 'MMMM') AS month_name,
    --EXTRACT(DAY FROM message_date) AS day_of_month,
    --EXTRACT(DOW FROM message_date) AS day_of_week, 
    --TO_CHAR(message_date, 'Day') AS day_of_week_name,
--FROM {{ ref('stg_telegrammessages') }}
--WHERE message_date IS NOT NULL 

-- models/marts/dim_dates.sql

{{ config(materialized='table') }}

WITH dates AS (
    SELECT DISTINCT
        CAST(message_date AS DATE) AS date_day
    FROM
    {{ ref('stg_telegrammessages') }} 
)
SELECT
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    TO_CHAR(date_day, 'YYYY-MM') AS year_month,
    TO_CHAR(date_day, 'MM-DD') AS month_day,
    TO_CHAR(date_day, 'Day') AS day_name,
    EXTRACT(DOY FROM date_day) AS day_of_year,
    EXTRACT(WEEK FROM date_day) AS week_of_year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(ISOYEAR FROM date_day) AS iso_year,
    EXTRACT(WEEK FROM date_day) AS iso_week,
    EXTRACT(DOW FROM date_day) AS day_of_week_num, 
    (EXTRACT(DOW FROM date_day)::INTEGER + 6) % 7 + 1 AS day_of_week_iso, 
    TO_CHAR(date_day, 'DD/MM/YYYY') AS full_date_format
FROM dates 
ORDER BY date_day