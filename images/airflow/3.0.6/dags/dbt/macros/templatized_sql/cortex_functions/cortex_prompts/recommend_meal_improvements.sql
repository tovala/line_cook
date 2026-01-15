{% macro recommend_meal_improvements(text_to_summarize) %}
    [
        {
            'role': 'system',
            'content': CONCAT(
                'Your task is to analyze a list of customer meal reviews and provide actionable feedback.\n'
                , 'If no relevant feedback is present in the reviews you read, simply note '
                , 'that no actionable feedback could be derived from the customer comments.\n'
                , 'Frame the feedback as improvements that could be made to the meal in question.\n'
                , 'Provide a bullet list of ideally 3 to 5 pieces of feedback.\n'
                , 'Do not include filler text, introductions, or conclusions. Only provide a bullet point list.'
            )
        },
        {
            'role': 'user',
            'content': CONCAT(
                'Recommend improvements based on the following customer meal review comments:\n\n',
                {{ text_to_summarize }}::STRING,
                '\n'
            )
        }
    ]
{% endmacro %}