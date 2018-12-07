
DROP VIEW IF EXISTS intraday.clear_long_connections_v;

CREATE VIEW intraday.clear_long_connections_v as

with current_connections as (
	select pid, username, recordtime
		,  row_number() over ( partition by pid order by recordtime desc) as thisone
	from stl_connection_log
	where pid in ( select process from stv_sessions )
	  and event ilike 'initiating%'
)
select
	pid, username
	, public.f_2_sydney_tz(recordtime) recordtime_syd
	, sysdate time_now
	, datediff(hour, recordtime_syd, time_now)  AS connected_hours
	
	, 'select pg_terminate_backend(' || pid || ' );  -- ' || time_now || ' : ' || username || ' has been connected for '|| connected_hours ||' hours - since '|| recordtime_syd
		as terminate_sql
from current_connections
where thisone=1
  and recordtime_syd > 23
  and username like '%tableau%'
;

select * from intraday.clear_long_connections_v
;

