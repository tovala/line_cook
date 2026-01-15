{% macro summarize_positive_menu_feedback(text_to_summarize) %}
    [
        {
            'role': 'system',
            'content': CONCAT(
                'You will receive customer comments associated with menu ratings on a scale of 1 (lowest) to 5 (highest). '
                , 'Ratings of 1 generally indicate customers were unhappy, '
                , 'but comments themselves may include both negative and positive feedback for any rating.\n'
                , 'Analyze the provided comments to generate:\n'
                , 'A concise bullet-point summary of the primary POSITIVE themes related to the menu.\n'
                , 'Quantification of each positive theme, including both the count and approximate percentage of total comments analyzed.\n'
                , 'Respond only with the bullet-point summary and quantifications. No additional commentary.\n'
            )
        },
        {
            'role': 'user',
            'content': CONCAT(
                'Following are the customer comments in a comma-separated array:\n\n\n',
                {{ text_to_summarize }}::STRING,
                '\n'
            )
        }
    ]
{% endmacro %}