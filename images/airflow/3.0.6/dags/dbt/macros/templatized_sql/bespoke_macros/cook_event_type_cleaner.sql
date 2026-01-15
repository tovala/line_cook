{% macro cook_event_type_cleaner() %}
    (CASE WHEN cook_event_subtype IN ('tovala_assist_recipe', 'tovala_preset_recipe')
        THEN 'chefs_recipe'
     WHEN steadystate_type IN ('broil', 'bake', 'airfry', 'steam')
        THEN steadystate_type
     WHEN steadystate_type IN ('convection_steam')
        THEN 'steam'
     WHEN steadystate_type IN ('convection_bake')
        THEN 'bake'
     WHEN steadystate_type IN ('broil_low', 'broil_high')
        THEN 'broil'
    ELSE cook_event_subtype 
    END)
{% endmacro %}