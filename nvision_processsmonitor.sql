REM nvision_processsmonitor.sql
REM (c)Go-Faster Consultancy 2021
REM Report on nVision processes, which schedulers they are assigned to, and which layouts require Excel nVision
REM assumes PSNT_X runs OpenXML, PSNT_E runs Excel

alter session set nls_date_Format = 'dd-mon-yy hh24:mi:ss';
set pages 99 lines 200 trimspool on echo off
break on report
column begindttm format a28
column servernamerun heading 'Server|Run'
column servernamerqst heading 'Server|Request'
column serverassign heading 'Server|Assign'
column prcscategory heading 'Process|Category' format a15
column secs heading 'Elap|Secs' format 99999
column rundttm format a28
column excel_layout_ids format a30
spool nvision_processmonitor
select prcsinstance, rundttm, begindttm, prcsname, prcscategory, servernamerqst, servernamerun, runcntlid, runstatus
, 3600*extract(hour   from (enddttm-begindttm)) 
   +60*extract(minute from (enddttm-begindttm)) 
      +extract(second from (enddttm-begindttm)) secs
, CASE WHEN substr(prcsname,-1)  = 'E' AND servernamerun LIKE 'PSNT_X%' THEN 'OK'
       WHEN substr(prcsname,-1) != 'E' AND servernamerun LIKE 'PSNT_S%' THEN 'OK'
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
from psprcsrqst r
where r.prcstype like 'nVision%'
and r.rundttm > sysdate-1
order by 2,1
/
select prcsinstance, rundttm, prcsname, prcscategory, servernamerqst, servernamerun, serverassign, runcntlid
, CASE WHEN substr(prcsname,-1)  = 'E' AND servernamerun LIKE 'PSNT_X%' THEN 'OK'
       WHEN substr(prcsname,-1) != 'E' AND servernamerun LIKE 'PSNT_S%' THEN 'OK'
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
from psprcsque r
where r.prcstype like 'nVision%'
and r.rundttm > sysdate-1
order by 2,1
/

spool off
