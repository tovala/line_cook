{% macro summarize_term_meal_reviews(text_to_summarize) %}
    [
        {
            'role': 'system',
            'content': CONCAT(
                'You will receive customer comments associated with meal ratings provided after customers have cooked and eaten their meals. '
                , 'Ratings are on a scale of 1 (lowest) to 5 (highest), where low ratings generally indicate dissatisfaction and high ratings indicate satisfaction. '
                , 'However, comments themselves may include a mix of positive and negative feedback regardless of rating.\n'
                , 'Analyze the provided comments specifically to identify opportunities for improvement:\n'
                , 'Generate a concise bullet-point summary highlighting common themes and issues that frequently arose across multiple meals, indicating clear areas of opportunity.\n'
                , 'Quantify each identified opportunity, including both the count and approximate percentage of total comments analyzed.\n'
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