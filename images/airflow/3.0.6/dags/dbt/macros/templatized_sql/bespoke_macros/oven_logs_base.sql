{%- macro oven_logs_base(extra_fields=False) -%}
  oven_event_id
  , updated
  , agentid 
  , deviceid
  , key
  , sessionID
  , source
  , time_stamp
  , raw_data
  {%- if extra_fields -%}
  , serialNumber
  , deviceGroupID
  , deviceGroupName
  {%- endif -%}
{%- endmacro -%}