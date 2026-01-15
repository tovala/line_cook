-- This is a macro to dynamically define the default retention horizon (in number of terms) that will be evaluated by the Baklava model.
-- Use retention_horizon_weeks against the masala.season.customer_term_summary.cohort_week_without_holidays to handle customers
-- by their last Tovala meal order tenure.
-- For consistent results, use: 'customer_term_summary.cohort_week_without_holidays <= {{ retention_horizon_weeks() }}' in target table.
{% macro retention_horizon_weeks() %}78{% endmacro %}