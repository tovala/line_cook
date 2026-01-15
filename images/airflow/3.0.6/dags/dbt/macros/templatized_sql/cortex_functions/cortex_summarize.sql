{% macro cortex_summarize(
    text_to_summarize,
    prompt_macro,
    llm='llama3-8b',
    temperature=0.01,
    max_output_tokens=250
) %}
    {%- set prompt_function = context.get(prompt_macro) -%}
    {%- set prompt_messages = prompt_function(text_to_summarize) -%}

    TRIM(snowflake.cortex.complete(
        '{{ llm }}',
        {{ prompt_messages }},
        {
            'temperature': {{ temperature }},
            'max_tokens': {{ max_output_tokens }},
            'guardrails': FALSE
        })['choices'][0]['messages']
    )
{% endmacro %}