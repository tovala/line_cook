{% macro midline_samples(sample_indexes, channels) %}
  {% for index in sample_indexes %}
    {% for channel in channels %}
      , MAX(CASE WHEN sample_index = {{ index }} AND sample_values like '%{{ channel }}%' 
                 THEN sample_values:{{ channel }} 
            END) AS sample_{{ index + 1 }}_{{ channel }}
    {% endfor %}
  {% endfor %}
{% endmacro %}
