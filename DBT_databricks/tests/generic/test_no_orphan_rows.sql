{% test no_orphan_rows(model, column_name, to, field) %}

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND {{ column_name }} NOT IN (
      SELECT {{ field }}
      FROM {{ to }}
      WHERE {{ field }} IS NOT NULL
  )

{% endtest %}