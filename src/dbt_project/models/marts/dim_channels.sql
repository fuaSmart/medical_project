
-- models/marts/dim_channels.sql

SELECT DISTINCT
    channel_id AS channel_id,
    CASE
        WHEN channel_id = 2398372400 THEN 'Lobelia Cosmetics Channel'
        WHEN channel_id = 2106543498 THEN 'Tikvah Pharma Channel'
        WHEN channel_id = 1271266957 THEN 'WHO News Channel'
        ELSE 'Unknown Channel' 
    END AS channel_name
FROM {{ ref('stg_telegrammessages') }}