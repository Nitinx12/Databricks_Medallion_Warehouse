{% test accepted_range(model, column_name, min_value=none, max_value=none) %}

SELECT {{ column_name }}
FROM {{ model }}
WHERE
    {% if min_value is not none %} {{ column_name }} < {{ min_value }} {% endif %}
    {% if min_value is not none and max_value is not none %} OR {% endif %}
    {% if max_value is not none %} {{ column_name }} > {{ max_value }} {% endif %}

{% endtest %}