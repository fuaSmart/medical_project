-- models/staging/stg_telegrammessages.sql

SELECT
    id AS message_id,
    chat_id,
    sender_id,
    message_date AS message_timestamp,
    message_text,
    views AS message_views,
    forwards AS message_forwards,
    replies_count AS message_replies_count,
    CASE WHEN media_type = 'photo' THEN TRUE ELSE FALSE END AS has_media_photo,
    CASE WHEN media_type = 'video' THEN TRUE ELSE FALSE END AS has_media_video,
    _extracted_at AS extracted_at,
    _file AS source_file_name
FROM {{ source('raw_data', 'telegram_messages') }} 