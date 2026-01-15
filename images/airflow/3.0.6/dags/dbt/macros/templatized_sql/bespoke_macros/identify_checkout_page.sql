
{% macro identify_checkout_page(url_field) %}
(REGEXP_LIKE({{ url_field }},'.*tovala.com/start.*|.*buy.tovala.com.*|.*tovala.com/checkout.*','i'))
{% endmacro %}
