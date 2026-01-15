{% macro array_string_to_variant(string) %}
-- Takes a string in the form ["a", "b", "c"] and converts to array containing a, b, c
SPLIT(REGEXP_REPLACE({{ string }}, '\\[|\\]|"', ''), ',')
{% endmacro %}