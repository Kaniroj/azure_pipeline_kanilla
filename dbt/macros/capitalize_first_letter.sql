{% macro capitalize_first_letter(expression) %}
    (UPPER(SUBSTR({{ expression | safe }}, 1, 1)) || LOWER(SUBSTR({{ expression | safe }}, 2)))
{% endmacro %}
