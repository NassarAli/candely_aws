
CREATE OR REPLACE TABLE calendly_silver.webhook_flat
AS
WITH parsed_json AS (
  SELECT 
    from_json(raw_json, 'STRUCT<event: STRING, payload: STRUCT<scheduled_event: STRUCT<event_type: STRING>>>') as json_struct,
    source_file,
    file_size_bytes,
    s3_last_modified
  FROM aliworkspace.calendly_bronze.raw_webhooks
)
SELECT 
  get_json_object(raw_json, '$.created_at') as webhook_received_at,
  get_json_object(raw_json, '$.created_by') as webhook_created_by,
  get_json_object(raw_json, '$.event') as webhook_event_type,
  get_json_object(raw_json, '$.payload.email') as invitee_email,
  get_json_object(raw_json, '$.payload.first_name') as invitee_first_name,
  get_json_object(raw_json, '$.payload.last_name') as invitee_last_name,
  get_json_object(raw_json, '$.payload.name') as invitee_full_name,
  get_json_object(raw_json, '$.payload.status') as invitee_status,
  get_json_object(raw_json, '$.payload.timezone') as invitee_timezone,
  get_json_object(raw_json, '$.payload.rescheduled') as is_rescheduled,
  get_json_object(raw_json, '$.payload.scheduled_event.uri') as event_uri,
  get_json_object(raw_json, '$.payload.scheduled_event.name') as event_name,
  get_json_object(raw_json, '$.payload.scheduled_event.start_time') as event_start_time,
  get_json_object(raw_json, '$.payload.scheduled_event.end_time') as event_end_time,
  get_json_object(raw_json, '$.payload.scheduled_event.status') as event_status,
  get_json_object(raw_json, '$.payload.scheduled_event.event_type') as event_type_uri,
  CASE 
    WHEN get_json_object(raw_json, '$.payload.scheduled_event.event_type') = 'https://api.calendly.com/event_types/d639ecd3-8718-4068-955a-436b10d72c78' THEN 'facebook_paid_ads'
    WHEN get_json_object(raw_json, '$.payload.scheduled_event.event_type') = 'https://api.calendly.com/event_types/dbb4ec50-38cd-4bcd-bbff-efb7b5a6f098' THEN 'youtube_paid_ads'
    WHEN get_json_object(raw_json, '$.payload.scheduled_event.event_type') = 'https://api.calendly.com/event_types/bb339e98-7a67-4af2-b584-8dbf95564312' THEN 'tiktok_paid_ads'
    ELSE 'other'
  END as marketing_campaign,
  get_json_object(raw_json, '$.payload.questions_and_answers[0].answer') as phone_number,
  get_json_object(raw_json, '$.payload.tracking.utm_campaign') as utm_campaign,
  get_json_object(raw_json, '$.payload.tracking.utm_source') as utm_source,
  get_json_object(raw_json, '$.payload.tracking.utm_medium') as utm_medium,
  get_json_object(raw_json, '$.payload.tracking.utm_content') as utm_content,
  get_json_object(raw_json, '$.payload.tracking.utm_term') as utm_term,
  get_json_object(raw_json, '$.payload.cancel_url') as cancel_url,
  get_json_object(raw_json, '$.payload.reschedule_url') as reschedule_url,
  get_json_object(raw_json, '$.payload.uri') as invitee_uri,
  source_file,
  file_size_bytes,
  s3_last_modified
FROM aliworkspace.calendly_bronze.raw_webhooks
WHERE get_json_object(raw_json, '$.event') = 'invitee.created'
  AND get_json_object(raw_json, '$.payload.scheduled_event.event_type') IN (
    'https://api.calendly.com/event_types/d639ecd3-8718-4068-955a-436b10d72c78',
    'https://api.calendly.com/event_types/dbb4ec50-38cd-4bcd-bbff-efb7b5a6f098',
    'https://api.calendly.com/event_types/bb339e98-7a67-4af2-b584-8dbf95564312'
  );


  