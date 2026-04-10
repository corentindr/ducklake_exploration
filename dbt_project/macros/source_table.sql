{% macro get_source_table(table_name) %}
  {#
    Returns the SQL expression for reading a source table in the current target format.
    Used in staging models so the same SQL works against DuckLake, Delta, and Iceberg.

    DuckLake : reads from the attached `lake` schema (SQL table reference)
    Delta    : uses DuckDB's delta_scan() function
    Iceberg  : uses DuckDB's iceberg_scan() function
  #}
  {% set fmt = env_var('DBT_FORMAT', 'ducklake') %}
  {% set storage = env_var('STORAGE_PATH', 'storage') %}

  {% if fmt == 'ducklake' %}
    lake.{{ table_name }}

  {% elif fmt == 'delta' %}
    delta_scan('{{ storage }}/delta/{{ table_name }}')

  {% elif fmt == 'iceberg' %}
    {#
      Iceberg requires the path to the latest metadata JSON.
      We use a glob to pick the most recently modified metadata file.
      DuckDB's iceberg_scan with allow_moved_paths handles relative paths.
    #}
    iceberg_scan(
      (
        SELECT file
        FROM   glob('{{ storage }}/iceberg/warehouse/benchmark/{{ table_name }}/metadata/*.metadata.json')
        ORDER  BY last_modified DESC
        LIMIT  1
      ),
      allow_moved_paths := true
    )

  {% else %}
    {{ exceptions.raise_compiler_error("Unknown format: " ~ fmt) }}
  {% endif %}
{% endmacro %}
