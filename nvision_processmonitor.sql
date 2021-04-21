REM nvision_processmonitor.sql

alter session set nls_date_Format = 'dd-mon-yy hh24:mi:ss';
set pages 99 lines 200 trimspool on echo off termout on
break on report
column prcsinstance heading 'Process|Instance' format 99999999
column jobinstance heading 'Job Prcs|Instance' format 99999999
column prcsname heading 'Process|Name' format a10
column begindttm format a28
column prcsjobname    heading 'Process|Job Name' format a8
column servernamerun  heading 'Server|Run'
column servernamerqst heading 'Server|Request'
column serverassign   heading 'Server|Assign'
column prcscategory   heading 'Process|Category' format a14
column secs           heading 'Elap|Secs' format 9999
column runcntlid format a25
column rundttm format a28
column excel_layout_ids format a27
spool nvision_processmonitor
with r as (
select prcsinstance, jobinstance, rundttm, begindttm, prcsjobname, prcsname, prcscategory, servernamerqst, servernamerun, runcntlid, runstatus
, 3600*extract(hour   from (enddttm-begindttm)) 
   +60*extract(minute from (enddttm-begindttm)) 
      +extract(second from (enddttm-begindttm)) secs
, CASE WHEN substr(prcsname,-1)  = 'E' AND servernamerun LIKE 'PSNT_E%' THEN 'OK'
       WHEN substr(prcsname,-1) != 'E' AND servernamerun LIKE 'PSNT_X%' THEN 'OK'
       ELSE 'Err' END as status
, (SELECT LISTAGG(e.layout_id,', ') WITHIN GROUP (ORDER BY e.layout_id) 
  FROM   psnvsbookrequst b
  ,      ps_nvs_report n
  ,      ps_nvs_redir_excel e
  WHERE  b.oprid = r.oprid
  AND    b.run_cntl_id = r.runcntlid
  AND    b.eff_status = 'A'
  AND    n.business_unit = b.business_unit
  AND    n.report_id = b.report_id
  AND    n.layout_id = e.layout_id
  AND    e.eff_status = 'A') excel_layout_ids
from   psprcsrqst r
where  r.prcstype like 'nVision%'
and    r.rundttm > sysdate-6/24
)
select * from r
union
select prcsinstance, jobinstance, rundttm, begindttm, prcsjobname, prcsname, prcscategory, servernamerqst, servernamerun, runcntlid, runstatus
, 3600*extract(hour   from (enddttm-begindttm)) 
   +60*extract(minute from (enddttm-begindttm)) 
      +extract(second from (enddttm-begindttm)) secs
, '',''
from psprcsrqst
where prcsinstance IN (Select jobinstance from r)
/

column excel_layout_ids format a56
with r as (
select prcsinstance, jobinstance, rundttm, prcsjobname, prcsname, prcscategory, servernamerqst, servernamerun, serverassign, runcntlid
, CASE WHEN substr(prcsname,-1)  = 'E' AND servernamerun LIKE 'PSNT_E%' THEN 'OK'
       WHEN substr(prcsname,-1) != 'E' AND servernamerun LIKE 'PSNT_X%' THEN 'OK'
       ELSE 'Err' END as status
, (SELECT LISTAGG(e.layout_id,', ') WITHIN GROUP (ORDER BY e.layout_id)
  FROM   psnvsbookrequst b
  ,      ps_nvs_report n
  ,      ps_nvs_redir_excel e
  WHERE  b.oprid = r.oprid
  AND    b.run_cntl_id = r.runcntlid
  AND    b.eff_status = 'A'
  AND    n.business_unit = b.business_unit
  AND    n.report_id = b.report_id
  AND    n.layout_id = e.layout_id
  AND    e.eff_status = 'A') excel_layout_ids
from   psprcsque r
where  r.prcstype like 'nVision%'
and    r.rundttm > sysdate-6/24
)
select * from r
union
select prcsinstance, jobinstance, rundttm, prcsjobname, prcsname, prcscategory, servernamerqst, servernamerun, serverassign, runcntlid, '',''
from psprcsque 
where prcsinstance IN (Select jobinstance from r)
/

spool off
