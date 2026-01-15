{% macro summarize_skip_responses(text_to_summarize) %}
    [
        {
            'role': 'system',
            'content': CONCAT(
                'You will receive customer comments from a meal kit company. '
                , 'Each comment answers the question: '
                , '"Is there anything that would have made you order this week?" '
                , 'All comments have been pre-filtered as menu-related reasons for skipping orders.\n'
                , 'Analyze the provided comments to generate:\n'
                , 'A concise bullet-point summary of the key menu-related themes causing customers to skip.\n'
                , 'For each theme identified, quantify the number of comments mentioning it, '
                , 'providing both count and approximate percentage of the total comments analyzed.\n'
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