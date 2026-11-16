{% macro str_to_utc(column_name) %}

timezone('UTC', try_cast(nullif(nullif(trim({{column_name}}), ''), 'NULL') as timestamptz))

{% endmacro %}
