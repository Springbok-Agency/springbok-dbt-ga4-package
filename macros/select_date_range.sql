{% macro select_date_range_base(start_date, end_date) %}
    where cast(left(replace(_table_suffix, 'intraday_', ''), 8) as int64) >= {{ start_date }}
    {% if end_date is not none %}
        and cast(left(replace(_table_suffix, 'intraday_', ''), 8) as int64) <= {{ end_date }}
    {% endif %}
{% endmacro %}


{% macro select_date_range(start_date, end_date, date_column) %}
    {% if end_date is not none %}
       and date_column >= {{ start_date }} and date_column <= {{ end_date }}
    {% else %}
       and date_column >= {{ start_date }}
    {% endif %}
{% endmacro %}
