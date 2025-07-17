-- models/marts/fct_messages.sql

SELECT
    stg.message_id,
    stg.channel_id AS channel_id, 
    TO_CHAR(stg.message_date, 'YYYYMMDD')::INTEGER AS date_key, 
    stg.message_text,
    stg.message_date AS message_timestamp, 
    stg.sender_id,
    stg.sender_username,
    stg.views_count,
    stg.forwards_count,
    stg.replies_count,
    stg.reactions_count,
    stg.link,
    stg.media_data,
    LENGTH(stg.message_text) AS message_length
FROM {{ ref('stg_telegrammessages') }} stg