-- models/marts/fct_image_detections.sql
{{ config(materialized='table') }}

SELECT
    rid.id AS detection_id,
    rid.message_id,
    rid.image_path,
    obj.value ->> 'class_name' AS detected_object_class,
    (obj.value ->> 'confidence')::NUMERIC AS confidence_score,
    obj.value -> 'bbox' AS bounding_box, 
    rid.detection_timestamp
FROM
    {{ source('telegram', 'raw_image_detections') }} rid,
    jsonb_array_elements(rid.detected_objects) AS obj
WHERE
    rid.detected_objects IS NOT NULL
    AND jsonb_typeof(rid.detected_objects) = 'array'