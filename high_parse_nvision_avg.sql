REM high_parse_nvision_avg.sql
clear screen
clear breaks
clear columns
alter session set current_schema=SYSADM;
Alter session set nls_date_Format='mm/dd/yy hh24.mi.ss';
Alter session set nls_timestamp_format = 'dd.mm.yyyy HH24.MI.SS';
column oprid format a10
column cursor_sharing heading 'Cursor|Sharing' format a7
column parmvalue heading 'Cursor|Sharing|Setting' format a7
column num_prcs heading 'Num|Procs' format 9999
Column avg_ash_secs heading 'Avg|ASH|Secs' format 99999
Column stddev_ash_secs heading 'StdDev|ASH|Secs' format 99999
Column avg_parse_secs heading 'Avg|Parse|Secs' format 99999
Column stddev_parse_secs heading 'StdDev|Parse|Secs' format 99999
Column avg_resmgr_secs heading 'Avg|ResMgr|Secs' format 99999
Column stddev_resmgr_secs heading 'StdDev|ResMgr|Secs' format 99999
Column avg_cpu_secs heading 'Avg|CPU|Secs' format 99999
Column stddev_cpu_secs heading 'StdDev|CPU|Secs' format 99999
--column parse_pct heading 'Parse|%' format 999
Column avg_elap_secs heading 'Avg|Elap|Secs' format 99999
Column stddev_elap_secs heading 'StdDev|Elap|Secs' format 99999
--column runstatus heading 'R|S' format a2
--Column min_sample_time format a20
--Column max_sample_time format a20
--Column report_id heading 'Report|ID' format a10
--column eff_para heading 'Eff.|Para' format 99.9
--column diff_secs heading 'Diff|Secs' format 99999
column avg_sql_ids heading 'Avg|SQL|IDs' format 9999
column avg_fms heading 'Avg|FMS' format 999
--column num_plans heading 'Exec|Plans' format 9999
--column num_execs heading 'Num|Execs' format 9999
--column num_samples heading 'ASH|Samp' format 9999
column runcntlid format a24
--column sql_fms_ratio heading 'S:F|Ratio' format 90.9
set pages 40 lines 200 trimspool on
clear screen
spool high_parse_nvision_avg.lst
break on oprid skip 1 on runcntlid skip 1

WITH FUNCTION tsdiff(p1 TIMESTAMP, p2 TIMESTAMP) RETURN number IS
BEGIN
 RETURN 86400*extract(day from (p2-p1)) + 3600*extract(hour from (p2-p1)) + 60*extract(minute from (p2-p1)) + extract(second from (p2-p1));
END tsdiff;
a as (
SELECT /*+MATERIALIZE*/ o.dbname, c.retention
--, p.prcstype, p.prcsname, p.oprid, p.runcntlid, p.parmvalue
from dba_hist_wr_control c, ps.psdbowner o --, PS_PRCS_SESS_PARM p
where c.con_id = 0
and o.ownerid = 'SYSADM'
--and p.param_name = 'cursor_sharing'
--and p.prcsname = 'RPTBOOK'
fetch first 1 rows only
), x as (
select /*+LEADING(A R X)*/ r.oprid, r.runcntlid, r.prcsinstance, r.runstatus, p.parmvalue
--, force_matching_signature
--, min(sample_time) min_sample_time
--, max(sample_time) max_sample_time
--, (CAST(r.enddttm AS DATE)-CAST(r.begindttm AS DATE))*86400 elap_secs
, tsdiff(MIN(r.begindttm),MAX(r.enddttm)) elap_secs
, sum(h.usecs_per_row)/1e6 ash_Secs
,	SUM(DECODE(event,'resmgr:cpu quantum',h.usecs_per_row))/1e6 resmgr_secs
,	SUM(DECODE(in_parse,'Y',h.usecs_per_row))/1e6 parse_secs
,	SUM(DECODE(event,null,h.usecs_per_row))/1e6 cpu_secs
, count(distinct h.sql_id) sql_ids
,	count(distinct h.force_matching_signature) fms
, count(*) num_samples
from a
 inner join psprcsrqst r on a.dbname = r.dbname AND r.prcstype = 'nVision-ReportBook' AND r.prcsname = 'RPTBOOK'
   and r.runstatus != '8'
 inner join dba_hist_Snapshot x ON x.end_interval_time >= r.begindttm and x.begin_interval_time <= NVL(r.enddttm,SYSDATE) 
 inner join dba_hist_Active_Sess_history h on h.dbid = x.dbid and h.instance_number = x.instance_number and h.snap_id = x.snap_id
 left outer join PS_PRCS_SESS_PARM p
   on p.param_name = 'cursor_sharing' and p.prcstype = r.prcstype AND p.prcsname = r.prcsname AND p.runcntlid = r.runcntlid and p.oprid = r.oprid    
where r.dbname = a.dbname
and h.module = r.prcsname
and r.begindttm >= SYSDATE-a.retention
and h.action like 'PI='||r.prcsinstance||':%'
and h.sample_time BETWEEN r.begindttm and NVL(r.enddttm,SYSDATE)
group by r.prcstype, r.prcsname, r.oprid, r.runcntlid, r.prcsinstance, r.runstatus, p.parmvalue
--, action
--, force_matching_signature
--, top_level_sql_id
--having count(*)>1
), y as (
select CASE WHEN sql_ids>fms THEN 'EXACT' ELSE 'FORCE' END as cursor_sharing
, x.*
from x
)
select oprid, runcntlid, cursor_sharing, parmvalue
, count(prcsinstance) num_prcs
, avg(elap_secs) avg_elap_secs
, stddev(elap_secs) stddev_elap_secs
, avg(ash_secs) avg_ash_secs
, stddev(ash_secs) stddev_ash_secs
, avg(resmgr_secs) avg_resmgr_secs
, stddev(resmgr_secs) stddev_resmgr_secs
, avg(parse_secs) avg_parse_secs
, stddev(parse_secs) stddev_parse_secs
, avg(cpu_secs) avg_cpu_secs
, stddev(cpu_secs) stddev_cpu_secs
, avg(sql_ids) avg_sql_ids
, avg(fms) avg_fms
from y
group by oprid, runcntlid, cursor_sharing, parmvalue
order by 1,2
/
spool off

