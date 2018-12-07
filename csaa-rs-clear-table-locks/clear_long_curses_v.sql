DROP VIEW IF EXISTS intraday.clear_long_curses_v;

CREATE OR REPLACE VIEW intraday.clear_long_curses_v AS 

 SELECT	cur.xid
 	,	cur.pid
	, usr.usename AS username
	, public.f_2_sydney_tz(cur.starttime) AS start_time
	, getdate() AS time_now
	, date_diff('second', start_time, time_now) AS run_time
	, 'select pg_terminate_backend(' || cur.pid || ' );  -- ' || time_now || ' : ' || username || ' query has been runing for ' || run_time / 60 || ' mins - it started at ' || start_time 
		AS terminate_sql
   FROM stv_active_cursors cur
   JOIN pg_user usr ON usr.usesysid = cur.userid
  WHERE usr.usename ilike '%tableau%' 
    AND start_time > 300
;

select * from clear_long_curses_v;
