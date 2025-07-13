{% macro get_tables_in_schema(schema) %}
  {% set sql %}
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = '{{ schema }}'
      AND table_type = 'BASE TABLE'
    ORDER BY table_name
  {% endset %}

  {% set results = run_query(sql) %}
  {% if execute %}
    {% for row in results %}
      {{ row[0] }}
    {% endfor %}
  {% endif %}
{% endmacro %}

