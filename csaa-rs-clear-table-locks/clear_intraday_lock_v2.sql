
DROP VIEW IF EXISTS intraday.clear_intraday_lock_v2;

CREATE VIEW intraday.clear_intraday_lock_v2 as

with lox as ( -- intraday table locks
	SELECT 
		TRIM(n.nspname) schemaname,
		TRIM(c.relname) tablename,
		l.pid,
		a.usename,
		l.mode,
		l.granted
	FROM pg_catalog.pg_locks l
	JOIN pg_catalog.pg_class c ON c.oid = l.relation
	JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	JOIN pg_catalog.pg_stat_activity a ON a.procpid = l.pid
	where schemaname = 'intraday'
), etl_lox_waiting as (
	select * from lox
	where usename like '%etl'
	  and mode like '%ExclusiveLock'
	  and not granted
), tableau_lox_granted as (
	select * from lox
	where usename like '%tableau%'
	  and mode = 'AccessShareLock'
	  and granted
)
	select 
		current_timestamp current_ts,
	    etl.schemaname as etl_schemaname,
	    etl.tablename as etl_tablename,
	    etl.pid as etl_pid,
	    etl.usename as etl_usename,
	    etl.mode as etl_mode,
	    etl.granted as etl_granted,
	    tab.schemaname as tab_schemaname,
	    tab.tablename as tab_tablename,
	    tab.pid as tab_pid,
	    tab.usename as tab_usename,
	    tab.mode as tab_mode,
	    tab.granted as tab_granted,
		
		'select pg_terminate_backend(' || tab.pid || ' );  --' || current_ts::varchar(20) || ' : ' || tab_usename || '(' || tab_pid  || ') is locking ' || tab_tablename || ' wanted by ' || etl_usename || '(' || etl_pid || ') for ' || etl_mode
			as terminate_sql

	from etl_lox_waiting etl
	join tableau_lox_granted tab using(tablename)
;

select * from intraday.clear_intraday_lock_v2
;

