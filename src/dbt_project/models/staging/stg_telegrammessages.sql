-- models/staging/stg_telegrammessages.sql

SELECT
    message_id,
    channel_id,
    channel_username,
    message_text,
    message_date,
    sender_id,
    sender_username,
    views_count,
    forwards_count,
    replies_count, 
    reactions_count, 
    link,
    media_data, 
    scraped_at
FROM {{ source('telegram', 'raw_telegram_messages') }}