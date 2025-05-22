{# run this with 
    dbt run-operation grant_select
#} 
{% macro grant_select(schema=target.schema, role=target.role, dry_run=true) %}

    {% set sql %}
        grant usage on schema {{ schema }} to role {{ role }};
        grant select on all tables in schema {{ schema }} to role {{ role }};
        grant select on all views in schema {{ schema }} to role {{ role }};
    {% endset %}

    {{ log('Granting select on all tables and views in schema ' ~ target.schema ~ ' to role ' ~ role, info=True) }}
    {% if dry_run %}
        {{ log(sql, info=True) }}
    {% else %}
        {% do run_query(sql) %}
    {% endif %}

    {{ log('Privileges granted', info=True) }}

{% endmacro %}