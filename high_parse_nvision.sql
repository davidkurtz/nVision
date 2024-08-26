REM high_parse_nvision.sql
clear screen
clear breaks
alter session set current_schema=SYSADM;
Alter session set nls_date_Format='mm/dd/yy hh24.mi.ss';
Alter session set nls_timestamp_format = 'dd.mm.yyyy HH24.MI.SS';
Column prcsinstance heading 'Process|Instance' format 99999999
column oprid format a10
Column ash_secs heading 'ASH|Secs' format 99999
Column parse_secs heading 'Parse|Secs' format 99999
Column resmgr_secs heading 'ResMgr|Secs' format 99999
Column cpu_secs heading 'CPU|Secs' format 99999
column parse_pct heading 'Parse|%' format 999
Column elap_secs heading 'Elap|Secs' format 99999
column runstatus heading 'R|S' format a2
Column min_sample_time format a20
Column max_sample_time format a20
Column report_id heading 'Report|ID' format a10
column eff_para heading 'Eff.|Para' format 99.9
column diff_secs heading 'Diff|Secs' format 99999
column sql_ids heading 'SQL|IDs' format 9999
column fms heading 'FMS' format 999
column num_plans heading 'Exec|Plans' format 9999
column num_execs heading 'Num|Execs' format 9999
column num_samples heading 'ASH|Samp' format 9999
column runcntlid format a24
column sql_fms_ratio heading 'S:F|Ratio' format 90.9
column cursor_sharing heading 'Cursor|Sharing' format a7
column parmvalue heading 'Cursor|Sharing|Setting' format a7
set pages 40 lines 200 trimspool on
break on oprid skip 1
clear screen
spool high_parse_nvision.lst
WITH FUNCTION tsround(p_in IN TIMESTAMP, p_len INTEGER) RETURN timestamp IS
 l_date VARCHAR2(20);
  l_secs NUMBER;
 l_date_fmt VARCHAR2(20) := 'J';
 l_secs_fmt VARCHAR2(20) := 'SSSSS.FF9';
 BEGIN
  l_date := TO_CHAR(p_in,l_date_fmt);
  --l_secs := ROUND(TO_NUMBER(TO_CHAR(p_in,l_secs_fmt)),p_len);
  l_secs := FLOOR(TO_NUMBER(TO_CHAR(p_in,l_secs_fmt))/p_len)*p_len;
  IF l_secs >= 86400 THEN
   l_secs := l_secs - 86400;
   l_date := l_date + 1;
  END IF;
  RETURN TO_TIMESTAMP(l_date||l_secs,l_date_fmt||l_secs_fmt);
 END tsround;
 FUNCTION tsdiff(p1 TIMESTAMP, p2 TIMESTAMP) RETURN number IS
 BEGIN
   RETURN 86400*extract(day from (p2-p1)) + 3600*extract(hour from (p2-p1)) + 60*extract(minute from (p2-p1)) + extract(second from (p2-p1));
 END tsdiff;
t as (
select TRUNC(SYSDATE)-5/24-2 begindttm
,      TRUNC(SYSDATE)+19/24-0 enddttm
--select TO_DATE('210505-1800','YYMMDD-HH24MI') begindttm
--,      TO_DATE('210506-0600','YYMMDD-HH24MI') enddttm
,      o.dbname
from ps.psdbowner o
WHERE o.ownerid = 'SYSADM'
), r as (
Select    /*+LEADING(t o)*/ r.*
--,         q.sessionidnum
--,         (CAST(r.enddttm AS DATE)-CAST(r.begindttm AS DATE))*86400 elap_secs
,         tsdiff(r.begindttm,r.enddttm) elap_secs
From      t
,         psprcsrqst r
--,         psprcsque q
WHERE     r.prcsname = 'RPTBOOK'
and       t.dbname = r.dbname
And       r.enddttm >= t.begindttm
And       r.begindttm <= t.enddttm
--and       q.prcsinstance = r.prcsinstance
--and       r.runstatus = '9'
), x AS (
SELECT	/*+LEADING(r x) USE_NL(r)*/ 
          r.prcsinstance, r.oprid, r.runcntlid, r.prcstype, r.prcsname, r.elap_secs, r.runstatus
--,         REGEXP_REPLACE(h.action,':logins[[:digit:]]{1,2}','') action
,	SUM(usecs_per_row)/1e6 ash_secs
,	SUM(DECODE(event,'resmgr:cpu quantum',usecs_per_row))/1e6 resmgr_secs
,	SUM(DECODE(in_parse,'Y',usecs_per_row))/1e6 parse_secs
,	SUM(DECODE(event,null,usecs_per_row))/1e6 cpu_secs
,         AVG(usecs_per_row)/1e6*COUNT(distinct tsround(h.sample_time,-1)) elap_ash_secs
--,	min(sample_time) min_sample_time
--,	max(sample_time) max_sample_time
--,         count(distinct program) num_progs
,	count(distinct sql_id) sql_ids
,	count(distinct force_matching_signature) fms
,	count(distinct sql_plan_hash_value) num_plans
,         count(distinct sql_id||sql_plan_hash_value||sql_exec_id) num_execs
--,         tsdiff(MIN(h.sample_time),MAX(h.sample_time)) diff_secs
FROM      r
,         dba_hist_snapshot x
,         dba_hist_Active_Sess_history h
WHERE     x.dbid = h.dbid
And       x.instance_number = h.instance_number
and       x.snap_id = h.snap_id
and       x.end_interval_time >= r.begindttm
and       x.begin_interval_time <= NVL(r.enddttm,SYSDATE)
and       h.sample_time BETWEEN r.begindttm AND NVL(r.enddttm,SYSDATE)
--And       h.module = 'PSAE.'||r.prcsname||'.'||q.sessionidnum
And       h.action like 'PI='||r.prcsinstance||':%'
--and      (h.event is null or h.event != 'resmgr:cpu quantum')
And       r.prcsinstance = REGEXP_SUBSTR(h.action,'[[:digit:]]+',4,1)
--And       x.begin_interval_time >= t.begindttm
--And       x.end_interval_time <= t.enddttm
GROUP BY r.prcstype, r.prcsname, r.prcsinstance, r.oprid, r.runcntlid, r.elap_Secs, r.runstatus
--,         REGEXP_REPLACE(h.action,':logins[[:digit:]]{1,2}','')
--, session_id, session_serial#
), y as (
Select --x.prcsname,
       x.prcsinstance, x.oprid, x.runcntlid, x.runstatus
,      CASE WHEN sql_ids>fms THEN 'EXACT' ELSE 'FORCE' END as cursor_sharing
,      p.parmvalue
--,      substr(regexp_substr(x.action,':([A-Za-z0-9_-])+',1,1),2) report_id
--,      substr(regexp_substr(x.action,':([A-Za-z0-9_-])+',1,2),2) business_unit
,      x.elap_secs
,      x.ash_secs
--,      x.elap_ash_secs
--,      x.ash_Secs/x.elap_ash_secs eff_para
,      x.resmgr_secs
,      x.parse_secs
,      x.cpu_secs
--,      x.num_progs
--,      x.diff_secs, x.min_sample_time, x.max_sample_time
,      x.sql_ids, x.fms, x.num_plans, x.num_execs
,      x.parse_secs/NULLIF(x.ash_secs,0)*100 parse_pct
,      x.sql_ids/NULLIF(x.fms,0) sql_fms_ratio
From   x
 left outer join PS_PRCS_SESS_PARM p
 on p.oprid = x.oprid
 and p.runcntlid = x.runcntlid
 and p.prcstype = x.prcstype
 and p.prcsname = x.prcsname
 and p.param_name = 'cursor_sharing'
)
Select *
From y
Where 1=1
and elap_secs >= 300 
and parse_secs>=60
and (parse_secs/ash_secs >= .5
or parse_secs >= 600)
Order by 2,3 --parse_secs desc, elap_secs desc, prcsinstance, max_sample_time desc --, elap_ash_secs  --max_sample_time, min_sample_time
Fetch first 100 rows only
/


break on oprid skip 1 on runcntlid skip 1

WITH FUNCTION tsdiff(p1 TIMESTAMP, p2 TIMESTAMP) RETURN number IS
BEGIN
 RETURN 86400*extract(day from (p2-p1)) + 3600*extract(hour from (p2-p1)) + 60*extract(minute from (p2-p1)) + extract(second from (p2-p1));
END tsdiff;
a as (
SELECT /*+MATERIALIZE*/ o.dbname, c.retention --, p.prcstype, p.prcsname, p.oprid, p.runcntlid, p.parmvalue
from dba_hist_wr_control c, ps.psdbowner o --, PS_PRCS_SESS_PARM p
where c.con_id = 0
and o.ownerid = 'SYSADM'
--and p.param_name = 'cursor_sharing'
--and p.prcsname = 'RPTBOOK'
), x as (
select /*+LEADING(A R X)*/ r.oprid, r.runcntlid, r.prcsinstance, r.runstatus
--, force_matching_signature
, min(sample_time) min_sample_time
, max(sample_time) max_sample_time
--, (CAST(r.enddttm AS DATE)-CAST(r.begindttm AS DATE))*86400 elap_secs
, tsdiff(MIN(r.begindttm),MAX(r.enddttm)) elap_secs
, sum(h.usecs_per_row)/1e6 ash_Secs
,	SUM(DECODE(event,'resmgr:cpu quantum',h.usecs_per_row))/1e6 resmgr_secs
,	SUM(DECODE(in_parse,'Y',h.usecs_per_row))/1e6 parse_secs
,	SUM(DECODE(event,null,h.usecs_per_row))/1e6 cpu_secs
, count(distinct h.sql_id) sql_ids
,	count(distinct h.force_matching_signature) fms
, count(*) num_samples
, p.parmvalue
from a
 inner join psprcsrqst r on a.dbname = r.dbname 
   --AND a.prcstype = r.prcstype AND a.prcsname = r.prcsname AND a.runcntlid = r.runcntlid and a.oprid = r.oprid 
   and r.runstatus != '8'
 left outer join PS_PRCS_SESS_PARM p
   on p.param_name = 'cursor_sharing' and p.prcstype = r.prcstype AND p.prcsname = r.prcsname AND p.runcntlid = r.runcntlid and p.oprid = r.oprid    
 inner join dba_hist_Snapshot x ON x.end_interval_time >= r.begindttm and x.begin_interval_time <= NVL(r.enddttm,SYSDATE) 
 inner join dba_hist_Active_Sess_history h on h.dbid = x.dbid and h.instance_number = x.instance_number and h.snap_id = x.snap_id
where r.dbname = a.dbname
and r.prcsname = 'RPTBOOK'
and h.module = r.prcsname
and r.begindttm >= SYSDATE-a.retention
and h.action like 'PI='||r.prcsinstance||':%'
and h.sample_time BETWEEN r.begindttm and NVL(r.enddttm,SYSDATE)
group by r.prcstype, r.prcsname, r.oprid, r.runcntlid, r.prcsinstance, r.runstatus, p.parmvalue
--, action
--, force_matching_signature
--, top_level_sql_id
--having count(*)>1
)
select x.*
, CASE WHEN sql_ids>fms THEN 'EXACT' ELSE 'FORCE' END as cursor_sharing
, parse_Secs/ash_Secs*100 parse_pct
, sql_ids/fms sql_fms_ratio
from x
order by 1,2,3,4
/
spool off

