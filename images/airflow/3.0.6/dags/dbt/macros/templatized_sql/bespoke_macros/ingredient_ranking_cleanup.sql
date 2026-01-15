--New Question Format Went Live As Of 11/16/2021 at 12:54:00PM Central Time
{% macro ingredient_ranking_cleanup(numeric_ranking, verbal_ranking) %}
    MAX(CASE WHEN ta.question_id = '{{ numeric_ranking }}' THEN ta.numeric_answer
             WHEN ta.question_id = '{{ verbal_ranking }}' THEN 
                  CASE WHEN TRIM(GET(multiple_choice_answer,0),'') = 'Avoid' THEN 1
                       WHEN TRIM(GET(multiple_choice_answer,0),'') = 'Neutral' THEN 3
                       WHEN TRIM(GET(multiple_choice_answer,0),'') = 'Enjoy' THEN 5
                  END
        END) 
{% endmacro %}