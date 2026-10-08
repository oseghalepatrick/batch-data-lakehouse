{% macro create_schema_if_not_exists(schema_name, location) %}

    {% if execute %}

        {% set create_sql %}
            CREATE SCHEMA IF NOT EXISTS {{ target.database }}.{{ schema_name }}
            WITH (
                location = '{{ location }}'
            )
        {% endset %}

        {{ log(
            "Ensuring schema exists: "
            ~ target.database
            ~ "."
            ~ schema_name
            ~ " at "
            ~ location,
            info=True
        ) }}

        {% do run_query(create_sql) %}

    {% endif %}

{% endmacro %}