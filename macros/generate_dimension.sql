{% macro generate_dimension(model_name, dimension_columns, key_name) %}

{% set source_model = ref(model_name) %}
{% set dimension_columns_list = dimension_columns if dimension_columns is iterable and dimension_columns is not string else [dimension_columns] %}

WITH source AS (
    SELECT DISTINCT
        {% for col in dimension_columns_list %}
        {{ col }}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM
        {{ source_model }}
    WHERE
        {% for col in dimension_columns_list %}
        {{ col }} IS NOT NULL{% if not loop.last %} AND {% endif %}
        {% endfor %}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(dimension_columns_list) }} AS {{ key_name }},
    {{ dimension_columns_list | join(', ') }}
FROM
    source

{% endmacro %}