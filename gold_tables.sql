%sql
CREATE OR REPLACE TABLE calendly_gold.daily_calls_by_source
AS
SELECT 
  DATE(event_start_time) as booking_date,
  marketing_campaign as source,
  COUNT(*) as bookings_count
FROM calendly_silver.webhook_flat
WHERE event_start_time IS NOT NULL
  AND marketing_campaign != 'other'
GROUP BY DATE(event_start_time), marketing_campaign
ORDER BY booking_date DESC, source;

%sql
CREATE OR REPLACE TABLE calendly_gold.cost_per_booking_by_channel
WITH filtered_campaigns_spend AS (
  SELECT DISTINCT 
  spend_date,
  marketing_channel,
  daily_spend
FROM aliworkspace.calendly_silver.campaign_spend_flat
ORDER BY spend_date, marketing_channel
),
booked_calls AS (
    SELECT 
    marketing_campaign,
    COUNT(*) as total_booked_calls
  FROM aliworkspace.calendly_silver.webhook_flat
  -- WHERE invitee_status = 'active' -- Only count confirmed bookings
    -- AND marketing_campaign IS NOT NULL
  GROUP BY marketing_campaign
),
total_spend AS (
  SELECT 
    marketing_channel,
    SUM(daily_spend) as total_spend
  FROM filtered_campaigns_spend
  WHERE marketing_channel IS NOT NULL
  GROUP BY marketing_channel
)
SELECT 
  COALESCE(bc.marketing_campaign, ts.marketing_channel) as marketing_channel,
  bc.total_booked_calls,
  ts.total_spend,
  CASE 
    WHEN bc.total_booked_calls > 0 THEN ts.total_spend / bc.total_booked_calls
    ELSE NULL 
  END as cost_per_booking
FROM total_spend ts
FULL OUTER JOIN booked_calls bc 
  ON ts.marketing_channel = bc.marketing_campaign
ORDER BY cost_per_booking DESC;
--  doesn't perfectly track spends to bookings because we have more data on spending then we do on the bookings right now. 
-- not a table yet
