-- Parse a choice-type question with an 'other' field from typeform
-- ta = dry.typeform_answers
{% macro typeform_mc_parser(question_id) %}
NULLIF(LISTAGG(CASE WHEN ta.question_id = '{{ question_id }}' 
                    THEN COALESCE(GET(ta.multiple_choice_answer, 0), ta.multiple_choice_other) 
                END, '|'), '')
{% endmacro %}
