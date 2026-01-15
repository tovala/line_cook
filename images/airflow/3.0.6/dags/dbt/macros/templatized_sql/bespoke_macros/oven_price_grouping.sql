--This macro designates an oven price grouping based on an oven price
--This macro will have to be changed if we want different groupings in the future ($1 customer is significantly different than a $49 customer)
{% macro oven_price_grouping(oven_price) %}
    CASE WHEN FLOOR({{ oven_price}}) < 0 THEN '<$0'
         WHEN FLOOR({{ oven_price}}) BETWEEN 0 AND 1 THEN '$0'
         WHEN FLOOR({{ oven_price}}) BETWEEN 1 AND 49 THEN '$1-$49'
         WHEN FLOOR({{ oven_price}}) BETWEEN 50 AND 99 THEN '$50-$99'
         WHEN FLOOR({{ oven_price}}) BETWEEN 100 AND 149 THEN '$100-$149'
         WHEN FLOOR({{ oven_price}}) BETWEEN 150 AND 199 THEN '$150-$199'
         WHEN FLOOR({{ oven_price}}) BETWEEN 200 AND 249 THEN '$200-$249'
         WHEN FLOOR({{ oven_price}}) BETWEEN 249 AND 299 THEN '$249-$299'
         WHEN FLOOR({{ oven_price}}) >= 300 THEN '$300+'
    END
{% endmacro %}