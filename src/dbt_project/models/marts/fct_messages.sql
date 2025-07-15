-- models/marts/fct_messages.sql

SELECT
    message_id,
    chat_id AS channel_id,
    sender_id,
    CAST(message_timestamp AS DATE) AS date_id,
    message_text,
    message_views,
    message_forwards,
    message_replies_count,
    has_media_photo,
    has_media_video
FROM {{ ref('stg_telegrammessages') }}