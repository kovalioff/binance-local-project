{% macro window_first(column, partition_by, order_by, direction='asc') %}
    first_value({{ column }}) over (
        partition by {{ partition_by }}
        order by {{ order_by }} {{ direction }}
        rows between unbounded preceding and unbounded following
    )
{% endmacro %}