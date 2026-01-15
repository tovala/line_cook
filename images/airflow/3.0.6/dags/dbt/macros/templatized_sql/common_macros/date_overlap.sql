-- Tests for overlap between two time periods
-- Caveat 1: NONE OF THESE TIMES CAN BE NULL
-- Caveat 2: If you are running this for a set of period 1 and a set of period 2, neither can have overlapping elements within the set
{%- macro date_overlap(period_1_start, period_1_end, period_2_start, period_2_end) -%}
(
    -- Case 1: period 1 entirely within period 2
    ({{ period_2_start }} < {{ period_1_start }} 
     AND {{ period_1_end }} < {{ period_2_end }}) OR 
    -- Case 2: period 2 entirely within period 1 
    ({{ period_1_start }} < {{ period_2_start }} 
     AND {{ period_2_end }} < {{ period_1_end }}) OR 
    -- Case 3: period 2 overlaps with period 1 at start of period 1 
    ({{ period_2_start }} < {{ period_1_start }} 
     AND {{ period_1_start }} < {{ period_2_end }} 
     AND {{ period_2_end }} < {{ period_1_end }}) OR 
    -- Case 4: period 2 overlaps with period 1 at end of period 1
    ({{ period_1_start }} < {{ period_2_start }}
     AND {{ period_2_start }} < {{ period_1_end }}
     AND {{ period_1_end }} < {{ period_2_end }})
)
{%- endmacro -%}
