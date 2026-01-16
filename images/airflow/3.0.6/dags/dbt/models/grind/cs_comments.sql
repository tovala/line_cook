
SELECT DISTINCT
  id AS cs_comment_id
  , ticket_id 
  , RANK() OVER (PARTITION BY ticket_id ORDER BY created_at, id) AS comment_number
  , body AS comment 
  , created_at AS comment_time
FROM {{ source('zendesk_support_v3', 'ticket_comments') }}
