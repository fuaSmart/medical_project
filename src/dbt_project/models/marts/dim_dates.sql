-- models/marts/dim_dates.sql

SELECT
    DISTINCT CAST(message_timestamp AS DATE) AS date_id,
    EXTRACT(YEAR FROM message_timestamp) AS year,
    EXTRACT(MONTH FROM message_timestamp) AS month,
    EXTRACT(DAY FROM message_timestamp) AS day,
    TO_CHAR(message_timestamp, 'Day') AS day_of_week_name
FROM {{ ref('stg_telegrammessages') }}