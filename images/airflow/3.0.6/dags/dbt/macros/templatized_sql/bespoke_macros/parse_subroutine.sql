{% macro parse_subroutine(comment) %}

CASE 
    WHEN NULLIF(SUBSTR(REPLACE(SPLIT_PART({{ comment }}, '|', 4), '"}', ''), 1, 1), '') IN ('A', 'B', 'C')
    THEN NULLIF(SUBSTR(REPLACE(SPLIT_PART({{ comment }}, '|', 4), '"}', ''), 1, 1), '')
    ELSE NULL
END

{% endmacro %}
