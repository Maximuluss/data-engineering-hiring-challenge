{% macro initcap(column_name) %}

([upper(w[1]) || lower(w[2:]) for w in {{column_name}}.string_split(' ')]).list_aggr('string_agg', ' ')

{% endmacro %}