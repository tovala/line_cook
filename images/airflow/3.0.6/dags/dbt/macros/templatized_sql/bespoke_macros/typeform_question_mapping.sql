-- Return multiple_choice_answer as a array base question_id on typeform, then boolean agg on if the array contains a input element.
-- Using for wash.skip_reasons
{% macro typeform_question_mapping(element,question_id) %}
    BOOLOR_AGG(ARRAY_CONTAINS('{{ element }}'::VARIANT,     
       CASE WHEN ta.question_id = '{{ question_id }}'
            THEN ta.multiple_choice_answer 
            END ))
{% endmacro %}
