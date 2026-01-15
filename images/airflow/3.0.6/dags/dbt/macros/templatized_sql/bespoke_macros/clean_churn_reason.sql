--This macro cleans the reason codes for pause/cancel surveys
{% macro clean_churn_reason(reason) %}
    CASE WHEN {{ reason }} IN (
            'Budget/ financial reasons', 
            'Budget/financial reasons'
            ) 
         THEN 'Budget/ financial reasons'
         WHEN {{ reason }} IN (
            'I am not interested in the meals Tovala is offering',
            'I am not interested in the meals that Tovala is offering'
         )
         THEN 'I am not interested in the meals Tovala is offering'
         WHEN {{ reason }} IN (
            'I do not need meals right now',
            'Packaging & shipping-related issues'
            )
         THEN {{ reason }}
         WHEN {{ reason }} IS NULL 
         THEN NULL
         ELSE 'Other'
    END
{% endmacro %}