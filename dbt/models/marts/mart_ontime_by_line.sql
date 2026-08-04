select
    line_name,
    service_date,
    count(*)                                                as trips,
    round(100.0 * sum(on_time_flag) / count(*), 1)          as on_time_pct,
    round(avg(delay_seconds) / 60.0, 2)                     as avg_delay_min
from {{ ref('fct_stop_performance') }}
group by line_name, service_date