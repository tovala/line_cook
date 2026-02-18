BROADCASTS_COLUMNS='''
  workspace_id INTEGER,
  broadcast_id INTEGER, 
  name STRING,
  created_at TIMESTAMP, 
  updated_at TIMESTAMP, 
  topic_names ARRAY
'''

CAMPAIGNS_COLUMNS='''
  workspace_id INTEGER,
  campaign_id INTEGER, 
  name STRING,
  created_at TIMESTAMP, 
  updated_at TIMESTAMP, 
  version INTEGER, 
  topic_names ARRAY
'''


DELIVERIES_COLUMNS='''
  workspace_id INTEGER,
  delivery_id VARCHAR, 
  internal_customer_id VARCHAR,
  subject_id VARCHAR, 
  event_id VARCHAR, 
  delivery_type VARCHAR, 
  campaign_id INTEGER, 
  action_id INTEGER, 
  newsletter_id INTEGER, 
  content_id INTEGER, 
  trigger_id INTEGER, 
  created_at TIMESTAMP, 
  transactional_message_id INTEGER,
  seq_num INTEGER
''' 


METRICS_COLUMNS='''
  event_id VARCHAR, 
  workspace_id INTEGER, 
  delivery_id VARCHAR, 
  metric VARCHAR, 
  reason VARCHAR, 
  link_id INTEGER, 
  link_url VARCHAR, 
  created_at TIMESTAMP, 
  seq_num INTEGER, 
  proxied BOOLEAN, 
  prefetched BOOLEAN,
  machine BOOLEAN, 
  user_agent VARCHAR, 
  email_client VARCHAR, 
  inbox_domain VARCHAR, 
  inbox_provider VARCHAR, 
  mx_host VARCHAR
'''

OUTPUTS_COLUMNS='''
  workspace_id INTEGER,
  output_id VARCHAR, 
  subject_name VARCHAR, 
  output_type VARCHAR,
  action_id INTEGER, 
  explanation VARCHAR, 
  delivery_id VARCHAR, 
  draft BOOLEAN, 
  link_tracked BOOLEAN, 
  manual_segment_id INTEGER, 
  add_to_manual_segment BOOLEAN, 
  split_test_index INTEGER,
  branch_index INTEGER,
  delay_ends_at TIMESTAMP, 
  created_at TIMESTAMP, 
  seq_num INTEGER
'''

PEOPLE_COLUMNS='''
  workspace_id INTEGER,
  customer_id VARCHAR, 
  internal_customer_id VARCHAR,
  deleted BOOLEAN, 
  suppressed BOOLEAN, 
  created_at TIMESTAMP, 
  updated_at TIMESTAMP, 
  email_addr VARCHAR 
'''

SUBJECTS_COLUMNS='''
    workspace_id INTEGER,
    subject_name VARCHAR, 
    internal_customer_id VARCHAR,
    campaign_id INTEGER, 
    campaign_type VARCHAR, 
    event_id VARCHAR, 
    trigger_id INTEGER, 
    created_at TIMESTAMP, 
    started_campaign_at TIMESTAMP, 
    seq_num INTEGER
'''