{% macro remove_soft_deletes(primary_key=config.get('unique_key')) %}

DELETE FROM {{ this }}
WHERE {{ primary_key }} IN (
  SELECT {{ primary_key }}
  FROM {{ this }}
  WHERE deleted IS NOT NULL
)

{% endmacro %}