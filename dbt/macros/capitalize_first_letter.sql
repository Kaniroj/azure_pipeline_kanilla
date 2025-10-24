{% macro capitalize_first_letter(string) %}
  {% if string is not none %}
    {{ return(string | capitalize) }}
  {% else %}
    {{ return('') }}
  {% endif %}
{% endmacro %}
