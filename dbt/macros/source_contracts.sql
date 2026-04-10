{% macro get_required_relation(relation, required_columns) %}
  {% if not execute %}
    {{ return(relation) }}
  {% endif %}

  {% set existing_relation = adapter.get_relation(
      database=relation.database,
      schema=relation.schema,
      identifier=relation.identifier
  ) %}

  {% if existing_relation is none %}
    {{ exceptions.raise_compiler_error(
        "Required relation `"
        ~ relation.database ~ "." ~ relation.schema ~ "." ~ relation.identifier
        ~ "` does not exist. Repair bronze ingestion or rerun the upstream merge before building dependent dbt models."
    ) }}
  {% endif %}

  {% set relation_columns = adapter.get_columns_in_relation(existing_relation) %}
  {% set ns = namespace(column_names=[], missing_columns=[]) %}
  {% for column in relation_columns %}
    {% set ns.column_names = ns.column_names + [column.name | lower] %}
  {% endfor %}

  {% for required_column in required_columns %}
    {% if required_column | lower not in ns.column_names %}
      {% set ns.missing_columns = ns.missing_columns + [required_column] %}
    {% endif %}
  {% endfor %}

  {% if ns.missing_columns %}
    {{ exceptions.raise_compiler_error(
        "Required relation `"
        ~ relation.database ~ "." ~ relation.schema ~ "." ~ relation.identifier
        ~ "` is missing required columns: "
        ~ (ns.missing_columns | join(", "))
        ~ ". The warehouse state is stale or incompatible with the current dbt project."
    ) }}
  {% endif %}

  {{ return(existing_relation) }}
{% endmacro %}
