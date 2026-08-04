-- Fails if any line-level on-time percentage falls outside 0-100.
select line_name, on_time_pct
from {{ ref('mart_ontime_by_line') }}
where on_time_pct < 0 or on_time_pct > 100