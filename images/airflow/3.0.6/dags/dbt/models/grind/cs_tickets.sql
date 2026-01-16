
WITH affected_terms AS (
  SELECT 
    ticket_id 
    , COUNT(DISTINCT F.value::INTEGER) > 1 AS affected_multiple_terms
    , MIN(F.value::INTEGER) AS affected_term_id
    , ARRAYAGG(F.value::INTEGER) AS all_affected_term_ids
  FROM {{ ref('cs_parsed_custom_fields') }}, Table(Flatten({{ ref('cs_parsed_custom_fields') }}.term_ids)) F
  GROUP BY 1
  )
SELECT
  t.id AS ticket_id 
  , t.created_at AS ticket_created_time
  , g.name AS agent_group 
  , t.is_public 
  , COALESCE(t.subject, t.raw_subject) AS subject 
  , t.requester_id 
  , t.assignee_id 
  , t.submitter_id 
  , t.status AS ticket_status
  , t.type AS ticket_type 
  , tc.comment AS first_comment
  , t.via:channel::STRING AS channel 
  -- Pre-calculated metrics from zendesk_support_v3.ticket_metrics
  , tm.initially_assigned_at AS initial_ticket_assigned_time
  , tm.assigned_at AS ticket_assigned_time
  , tm.solved_at AS solved_time
  , tm.latest_comment_added_at AS latest_comment_time
  , tm.requester_updated_at AS requester_update_time
  , tm.status_updated_at AS status_update_time
  -- Note: All "wait times" in calendar and business days are the same, so we're only grabbing business
  , tm.agent_wait_time_in_minutes:calendar::INT AS agent_wait_minutes
  , tm.requester_wait_time_in_minutes:calendar::INT AS requester_wait_minutes
  , tm.reply_time_in_minutes:calendar::INT AS reply_minutes
  , tm.on_hold_time_in_minutes:calendar::INT AS on_hold_minutes
  , tm.first_resolution_time_in_minutes:calendar::INT AS first_resolution_minutes
  , tm.full_resolution_time_in_minutes:calendar::INT AS full_resolution_minutes
  , COALESCE(aff.affected_multiple_terms, FALSE) AS affected_multiple_terms
  , aff.affected_term_id
  , aff.all_affected_term_ids
  , pcf.stella_score
FROM {{ source('zendesk_support_v3', 'tickets') }} t 
LEFT JOIN {{ source('zendesk_support_v3', 'groups') }} g 
  ON t.group_id = g.id 
LEFT JOIN {{ source('zendesk_support_v3', 'ticket_metrics') }} tm 
  ON t.id = tm.ticket_id 
LEFT JOIN {{ ref('cs_comments') }} tc
  ON t.id = tc.ticket_id 
  AND tc.comment_number = 1 
LEFT JOIN {{ ref('cs_parsed_custom_fields') }} pcf
  ON t.id = pcf.ticket_id 
LEFT JOIN affected_terms aff 
  ON t.id = aff.ticket_id
