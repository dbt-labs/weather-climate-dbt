{% macro fahrenheit_to_celsius(fahrenheit_column) %}
    round(({{ fahrenheit_column }} - 32) * 5.0/9.0, 1)
{% endmacro %}

{% macro celsius_to_fahrenheit(celsius_column) %}
    round(({{ celsius_column }} * 9.0/5.0) + 32, 1)
{% endmacro %}