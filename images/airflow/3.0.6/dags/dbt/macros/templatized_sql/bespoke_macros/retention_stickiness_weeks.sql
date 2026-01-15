-- This is a macro to dynamically define the default "stickiness" included in retention evaluation. 
-- Use retention_stickiness_weeks against the masala.season.customer_term_summary.weeks_since_last_order to handle customers
-- by their last Tovala meal order date.  
-- For consistent results, use: 'customer_term_summary.weeks_since_last_order < {{ retention_stickiness_weeks() }}' in target table.
{% macro retention_stickiness_weeks() %}2{% endmacro %}