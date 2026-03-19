{#- Cross-database compatibility macros for BigQuery and Redshift. -#}

{% macro countif(condition) %}
  {% if target.type == 'bigquery' %}
    countif({{ condition }})
  {% else %}
    sum(case when {{ condition }} then 1 else 0 end)
  {% endif %}
{% endmacro %}


{% macro safe_divide(numerator, denominator) %}
  {% if target.type == 'bigquery' %}
    safe_divide({{ numerator }}, {{ denominator }})
  {% else %}
    ({{ numerator }}) / nullif({{ denominator }}, 0)
  {% endif %}
{% endmacro %}



{% macro varchar_type() %}
  {% if target.type == 'bigquery' %}
    string
  {% else %}
    varchar
  {% endif %}
{% endmacro %}


{% macro int64_type() %}
  {% if target.type == 'bigquery' %}
    int64
  {% else %}
    bigint
  {% endif %}
{% endmacro %}


{% macro bool_type() %}
  {% if target.type == 'bigquery' %}
    bool
  {% else %}
    boolean
  {% endif %}
{% endmacro %}


{% macro float64_type() %}
  {% if target.type == 'bigquery' %}
    float64
  {% else %}
    double precision
  {% endif %}
{% endmacro %}
