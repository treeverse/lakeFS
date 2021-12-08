{% macro create_schama(name, location) %}
    create schema {{ name }} with ( location = '{{location}}')
{% endmacro %}
