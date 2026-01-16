SELECT
  id
  , form_id
  , survey_title AS form_title
  , survey_link AS form_link
  , upload_time
  , created_at
  , last_updated_at
FROM {{ table_reference('typeform_surveys') }}
