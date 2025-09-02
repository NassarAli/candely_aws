%sql
INSERT INTO calendly_silver.campaign_spend_flat
WITH parsed_json AS (
  SELECT 
    from_json(
      raw_json, 
      'ARRAY<STRUCT<date: STRING, channel: STRING, spend: DOUBLE>>'
    ) AS json_array,
    source_file,
    file_url,
    file_size_bytes,
    download_timestamp
  FROM aliworkspace.calendly_bronze.raw_campaign_data
  WHERE download_timestamp > COALESCE(
    (SELECT MAX(download_timestamp) FROM calendly_silver.campaign_spend_flat),
    TIMESTAMP('1900-01-01')
  )
)
SELECT 
  exploded_data.date AS spend_date,
  exploded_data.channel AS marketing_channel,
  exploded_data.spend AS daily_spend,
  source_file,
  file_url,
  file_size_bytes,
  download_timestamp
FROM parsed_json
LATERAL VIEW explode(json_array) exploded_table AS exploded_data
