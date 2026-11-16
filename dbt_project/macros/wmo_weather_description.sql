{# Maps a WMO weather interpretation code (as used by Open-Meteo) to a short description. #}
{% macro wmo_weather_description(column_name) %}

case
    when {{ column_name }} = 0 then 'Clear sky'
    when {{ column_name }} in (1, 2, 3) then 'Partly cloudy'
    when {{ column_name }} in (45, 48) then 'Fog'
    when {{ column_name }} in (51, 53, 55, 56, 57) then 'Drizzle'
    when {{ column_name }} in (61, 63, 65, 66, 67) then 'Rain'
    when {{ column_name }} in (71, 73, 75, 77) then 'Snow'
    when {{ column_name }} in (80, 81, 82) then 'Rain showers'
    when {{ column_name }} in (85, 86) then 'Snow showers'
    when {{ column_name }} in (95, 96, 99) then 'Thunderstorm'
end

{% endmacro %}
