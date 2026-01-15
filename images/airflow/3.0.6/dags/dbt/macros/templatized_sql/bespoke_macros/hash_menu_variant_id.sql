{% macro hash_menu_variant_id(subterm_id, variant_id='variant_id') %}
{{ hash_natural_key(subterm_id, 'COALESCE(' ~ variant_id~ ', \'\')')}}
{% endmacro %}