--This macro designates a 3 letter facility network string for easier handling in downstream processes
--We designate CHI in the "else" to handle edge cases
--This macro will have to be changed when new facility networks are added to the operation
{% macro facility_network(facility_network) %}
    CASE WHEN {{ facility_network }} = 'slc' THEN 'SLC'
         ELSE 'CHI'
    END
{% endmacro %}