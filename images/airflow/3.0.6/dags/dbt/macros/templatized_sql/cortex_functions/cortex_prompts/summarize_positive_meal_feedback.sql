{% macro summarize_positive_meal_feedback(text_to_summarize) %}
    [
        {
            'role': 'system',
            'content': CONCAT(
                'You are an expert meal quality analyst.\n'
                , 'Your task is to analyze and summarize customer reviews of meals.\n'
                , 'Provide a concise summary highlighting key POSITIVE themes.\n'
                , 'ONLY include themes that are positive. '
                , 'If no positive themes can be found, simply report that no positive themes could be found.\n'
                , 'Provide 1 bullet point per theme, ranked by frequency.\n'
                , 'Aim for a total of 3 to 5 bullet points / themes as long as there is enough data.\n'
                , 'Only provide the bullet point list. Do not add any additional intro, conclusion, or filler.\n'
                , 'Wherever possible, provide a count of reviews mentioning the theme in parentheses next to each bullet point.\n'
                , 'Be objective and informative.\n'
                , 'Avoid themes that are vague or unqualified.\n'
                , 'Prioritize the following themes whenver they are mentioned and describe why they were mentioned: '
                , 'good value, good taste, good flavor, healthy, comforting, balanced, complete, unique.\n'
            )
        },
        {
            'role': 'user',
            'content': CONCAT(
                'Here are the meal reviews for you to summarize:\n\n',
                {{ text_to_summarize }}::STRING,
                '\n'
            )
        }
    ]
{% endmacro %}