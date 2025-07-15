-- models/marts/dim_channels.sql

SELECT DISTINCT
    chat_id AS channel_id,
    'Unknown' AS channel_name -- Placeholder, I want to add channel names later
FROM {{ ref('stg_telegrammessages') }}