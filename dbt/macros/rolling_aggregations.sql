{% macro rolling_agg(column, partition_by, order_by, interval='30 days', agg_function='sum') %}
    {{ agg_function }}({{ column }}) over (
        partition by {{ partition_by }}
        order by {{ order_by }}
        range between interval '{{ interval }}' preceding and current row
    )
{% endmacro %}
