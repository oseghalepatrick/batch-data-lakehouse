{% test one_current_record(model, column_name) %}

select
    {{ column_name }}

from {{ model }}

where is_current = true

group by {{ column_name }}

having count(*) > 1

{% endtest %}