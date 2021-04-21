REM gfc_nvsion_excel_redirect_triggers.sql
REM (c)Go-Faster Consultancy 2021
REM Triggers to change process category to nVisionExcel for reports and reportbooks that have to be run on Excel

REM see also https://blogs.oracle.com/oraclemagazine/on-conditional-compilatio
set echo on
spool gfc_nvsion_excel_redirect_triggers
rollback;
ALTER SESSION SET PLSQL_CCFLAGS = 'mydebug:FALSE';

----------------------------------------------------------------------------------------------------
CREATE TABLE sysadm.ps_nvs_redir_excel
(layout_id VARCHAR2(50) NOT NULL
,eff_status VARCHAR2(1) not null
);

ALTER TABLE sysadm.ps_nvs_redir_excel add eff_status VARCHAR2(1); 
UPDATE sysadm.ps_nvs_redir_excel SET eff_status = 'A';
ALTER TABLE sysadm.ps_nvs_redir_excel MODIFY eff_status not null;

CREATE UNIQUE INDEX sysadm.ps_nvs_redir_excel ON sysadm.ps_nvs_redir_excel (layout_id);
----------------------------------------------------------------------------------------------------
REM load metadata of layouts that have to run on Excel rather than OpenXML
@@gfc_nvsion_excel_redirect_metadata
----------------------------------------------------------------------------------------------------
spool gfc_nvsion_excel_redirect_triggers app

CREATE OR REPLACE TRIGGER sysadm.gfc_nvision_excel_redirect_rqst 
BEFORE INSERT ON sysadm.psprcsrqst
FOR EACH ROW
WHEN (new.prcstype IN('nVision-Report','nVision-ReportBook'))
DECLARE
  l_excel INTEGER := 0;  
  l_maxconcurrent INTEGER := 0;
  k_prcscategory CONSTANT VARCHAR2(15) := 'nVisionExcel';
  $IF $$mydebug $THEN
  l_errc NUMBER;
  l_errm VARCHAR2(200);
  $END
BEGIN
  $IF $$mydebug $THEN dbms_output.put_line('Entering Trigger sysadm.gfc_nvision_excel_redirect_rqst'); $END

  IF :new.prcstype = 'nVision-ReportBook' THEN
    --check for reportbook running report that uses layout on Excel list
    SELECT 1
    INTO   l_excel
    FROM   psnvsbookrequst b
    ,      ps_nvs_report n
    ,      ps_nvs_redir_excel e
    WHERE  b.oprid = :new.oprid
    AND    b.run_cntl_id = :new.runcntlid
    AND    b.eff_status = 'A'
    AND    n.business_unit = b.business_unit
    AND    n.report_id = b.report_id
    AND    n.layout_id = e.layout_id
    AND    e.eff_status = 'A'
    AND    rownum=1;
  ELSE
    --look in command line for report running layout on Excel list
    SELECT 1
    INTO   l_excel
    FROM   psprcsparms p
    ,      ps_nvs_report n
    ,      ps_nvs_redir_excel e
    WHERE  p.prcsinstance = :new.prcsinstance
    AND    n.report_id = substr(regexp_substr(p.parmlist,'-NRN[^ ]+'),5)
    AND    n.layout_id = e.layout_id
    AND    e.eff_status = 'A'
    AND    rownum=1;
  END IF;

  $IF $$mydebug $THEN dbms_output.put_line('found Excel nVision layout for oprid='||:new.oprid||', runcntlid='||:new.runcntlid); $END
  --set category of request
  :new.prcscategory := k_prcscategory;

  SELECT maxconcurrent
  INTO   l_maxconcurrent
  FROM   ps_servercategory
  WHERE  prcscategory = :new.prcscategory
  AND    servername = :new.servernamerqst;

  --if request assigned to server where it cannot run blank out server assignment and allow load balancing to determine it
  IF l_maxconcurrent = 0 THEN
    :new.servernamerqst := ' ';
  END IF;

  $IF $$mydebug $THEN dbms_output.put_line('set process name:'||:new.prcsname||', category:'||:new.prcscategory); $END
EXCEPTION
  WHEN no_data_found THEN 
    $IF $$mydebug $THEN dbms_output.put_line('No excel redirect found'); $ELSE NULL; $END
  WHEN others THEN 
    $IF $$mydebug $THEN 
    l_errc := sqlcode;
    l_errm := SUBSTR(sqlerrm,1,200);
    dbms_output.put_line('Other Error: ORA-'||l_errc||':'||l_errm); 
    $ELSE NULL; $END
END gfc_nvision_excel_redirect_rqst ;
/
show errors
----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER sysadm.gfc_nvision_excel_redirect_que
BEFORE INSERT ON sysadm.psprcsque
FOR EACH ROW
WHEN (new.prcstype IN('nVision-Report','nVision-ReportBook'))
DECLARE
  l_excel         INTEGER := 0;  
  l_maxconcurrent INTEGER := 0;
  k_prcscategory  CONSTANT VARCHAR2(15) := 'nVisionExcel';
BEGIN

  IF :new.prcstype = 'nVision-ReportBook' THEN
    SELECT 1
    INTO   l_excel
    FROM   psnvsbookrequst b
    ,      ps_nvs_report n
    ,      ps_nvs_redir_excel e
    WHERE  b.oprid = :new.oprid
    AND    b.run_cntl_id = :new.runcntlid
    AND    b.eff_status = 'A'
    AND    n.business_unit = b.business_unit
    AND    n.report_id = b.report_id
    AND    n.layout_id = e.layout_id
    AND    e.eff_status = 'A'
    AND    rownum=1;
  ELSE
    SELECT 1
    INTO   l_excel
    FROM   psprcsparms p
    ,      ps_nvs_report n
    ,      ps_nvs_redir_excel e
    WHERE  p.prcsinstance = :new.prcsinstance
    AND    n.report_id = substr(regexp_substr(p.parmlist,'-NRN[^ ]+'),5)
    AND    n.layout_id = e.layout_id
    AND    e.eff_status = 'A'
    AND    rownum=1;
  END IF;

  --set category of request
  :new.prcscategory := k_prcscategory;

  SELECT maxconcurrent
  INTO   l_maxconcurrent
  FROM   ps_servercategory
  WHERE  prcscategory = :new.prcscategory
  AND    servername = :new.servernamerqst;

  --if request assigned to server where it cannot run blank out server assignment and allow load balancing to determine it
  IF l_maxconcurrent = 0 THEN
    :new.servernamerqst := ' ';
    :new.serverassign := ' ';
  END IF;

EXCEPTION
  WHEN no_data_found THEN NULL;
  WHEN others THEN NULL;
END gfc_nvision_excel_redirect_que;
/
show errors
----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER sysadm.gfc_nvision_excel_redirect_jobrqst 
BEFORE INSERT ON sysadm.psprcsrqst
FOR EACH ROW
WHEN (new.prcstype IN('PSJob'))
DECLARE
  l_excel INTEGER := 0;  
  l_maxconcurrent INTEGER := 0;
  k_prcscategory CONSTANT VARCHAR2(15) := 'nVisionExcel';
  $IF $$mydebug $THEN
  l_errc NUMBER;
  l_errm VARCHAR2(200);
  $END
BEGIN
  $IF $$mydebug $THEN dbms_output.put_line('Entering Trigger sysadm.gfc_nvision_excel_redirect_jobrqst'); $END

  SELECT 1
  INTO   l_excel
  FROM   ps_schdlitem i
  ,      psnvsbookrequst b
  ,      ps_nvs_report n
  ,      ps_nvs_redir_excel e
  WHERE  b.oprid = :new.oprid
  AND    i.jobnamesrc = :new.prcsjobname
  AND    i.prcstype IN('nVision-ReportBook')
  AND    b.run_cntl_id = i.run_cntl_id
  AND    b.eff_status = 'A'
  AND    n.business_unit = b.business_unit
  AND    n.report_id = b.report_id
  AND    n.layout_id = e.layout_id
  AND    e.eff_status = 'A'
  AND    rownum=1;

  --set category of request
  :new.prcscategory := k_prcscategory;

  SELECT maxconcurrent
  INTO   l_maxconcurrent
  FROM   ps_servercategory
  WHERE  prcscategory = :new.prcscategory
  AND    servername = :new.servernamerqst;

  --if request assigned to server where it cannot run blank out server assignment and allow load balancing to determine it
  IF l_maxconcurrent = 0 THEN
    :new.servernamerqst := ' ';
  END IF;

EXCEPTION
  WHEN no_data_found THEN 
    $IF $$mydebug $THEN dbms_output.put_line('No excel redirect found'); $ELSE NULL; $END
  WHEN others THEN 
    $IF $$mydebug $THEN 
    l_errc := sqlcode;
    l_errm := SUBSTR(sqlerrm,1,200);
    dbms_output.put_line('Other Error: ORA-'||l_errc||':'||l_errm); 
    $ELSE NULL; $END
END gfc_nvision_excel_redirect_jobrqst ;
/
show errors
----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER sysadm.gfc_nvision_excel_redirect_jobque
BEFORE INSERT ON sysadm.psprcsque
FOR EACH ROW
WHEN (new.prcstype IN('PSJob'))
DECLARE
  l_excel INTEGER := 0;  
  l_maxconcurrent INTEGER := 0;
  k_prcscategory CONSTANT VARCHAR2(15) := 'nVisionExcel';
  $IF $$mydebug $THEN
  l_errc NUMBER;
  l_errm VARCHAR2(200);
  $END
BEGIN
  SELECT 1
  INTO   l_excel
  FROM   ps_schdlitem i
  ,      psnvsbookrequst b
  ,      ps_nvs_report n
  ,      ps_nvs_redir_excel e
  WHERE  b.oprid = :new.oprid
  AND    i.jobnamesrc = :new.prcsjobname
  AND    i.prcstype IN('nVision-ReportBook')
  AND    b.run_cntl_id = i.run_cntl_id
  AND    b.eff_status = 'A'
  AND    n.business_unit = b.business_unit
  AND    n.report_id = b.report_id
  AND    n.layout_id = e.layout_id
  AND    e.eff_status = 'A'
  AND    rownum=1;

  --set category of request
  :new.prcscategory := k_prcscategory;

  SELECT maxconcurrent
  INTO   l_maxconcurrent
  FROM   ps_servercategory
  WHERE  prcscategory = :new.prcscategory
  AND    servername = :new.servernamerqst;

  --if request assigned to server where it cannot run blank out server assignment and allow load balancing to determine it
  IF l_maxconcurrent = 0 THEN
    :new.servernamerqst := ' ';
    :new.serverassign := ' ';
  END IF;

EXCEPTION
  WHEN no_data_found THEN NULL;
  WHEN others THEN NULL;
END gfc_nvision_excel_redirect_jobque;
/
show errors
----------------------------------------------------------------------------------------------------
column trigger_name format a40
column table_name format a30
column triggering_event format a20
select trigger_name, table_name, triggering_event, status
from   user_triggers
where  table_name IN('PSPRCSRQST','PSPRCSQUE')
and    trigger_name like 'GFC_NVISION_EXCEL_REDIRECT%'
;
spool off
----------------------------------------------------------------------------------------------------
--drop TRIGGER sysadm.gfc_nvision_excel_redirect_rqst;
--drop TRIGGER sysadm.gfc_nvision_excel_redirect_que;
--drop TRIGGER sysadm.gfc_nvision_excel_redirect_jobrqst;
--drop TRIGGER sysadm.gfc_nvision_excel_redirect_jobque;
set termout off

/****************************************************************************************************
 * Test Script
 ****************************************************************************************************
REM test

set serveroutput on termout on
delete from psprcsrqst where prcsinstance IN(41,42);
delete from psprcsque where prcsinstance IN(41,42);

insert into psprcsrqst
(PRCSINSTANCE, JOBINSTANCE, MAINJOBINSTANCE, PRCSJOBSEQ, PRCSJOBNAME, PRCSTYPE, PRCSNAME, PRCSITEMLEVEL, MAINJOBNAME, MAINJOBSEQ
, RUNLOCATION, OPSYS, DBTYPE, DBNAME, SERVERNAMERQST, SERVERNAMERUN, RUNDTTM, RECURNAME, OPRID, PRCSVERSION, RUNSTATUS, RQSTDTTM
, LASTUPDDTTM, BEGINDTTM, ENDDTTM, RUNCNTLID, PRCSRTNCD, CONTINUEJOB, USERNOTIFIED, INITIATEDNEXT, OUTDESTTYPE, OUTDESTFORMAT
, ORIGPRCSINSTANCE, GENPRCSTYPE, RESTARTENABLED, TIMEZONE, PSRF_FOLDER_NAME, SCHEDULENAME, RETRYCOUNT, RECURORIGPRCSINST
, P_PRCSINSTANCE, DISTSTATUS, PRCSCATEGORY, PRCSCURREXPIREDTTM, RUNSERVEROPTION, PT_RETENTIONDAYS, CONTENTID, PTNONUNPRCSID)
WITH 
o AS (select distinct dbname from ps.psdbowner
where ownerid = 'SYSADM'
), r as (
SELECT DISTINCT b.oprid, b.run_cntl_id runcntlid, i.jobnamesrc
FROM   ps_schdlitem i
,      PSNVSBOOKREQUST b
,      PS_NVS_REPORT n
WHERE  b.eff_status = 'A'
and    n.business_unit = b.business_unit
and    n.report_id = b.report_id
and    n.layout_id IN('ZBUVBS64')
and    b.oprid = 'BATCH'
and    i.prcstype = 'nVision-ReportBook'
and    i.prcsname = 'RPTBOOK'
and    i.run_cntl_id = b.run_cntl_id
and rownum = 1
)
SELECT 41, 41, 0, 0, jobnamesrc, 'PSJob', jobnamesrc, 0, ' ', 0
, '2', '2', '2', o.DBNAME, ' ', ' ', null, ' ', r.oprid, 0, 5, sysdate
, sysdate, null, null, r.RUNCNTLID, 0, 0, 0, 0, 0, 0
, 0, '7', ' ', 'GMT', 'NVISION', ' ', 0, 0
, 0, ' ', 'Default', sysdate+42, ' ', 42, 0, 0
FROM o, r
/

insert into psprcsrqst
(PRCSINSTANCE, JOBINSTANCE, MAINJOBINSTANCE, PRCSJOBSEQ, PRCSJOBNAME, PRCSTYPE, PRCSNAME, PRCSITEMLEVEL, MAINJOBNAME, MAINJOBSEQ
, RUNLOCATION, OPSYS, DBTYPE, DBNAME, SERVERNAMERQST, SERVERNAMERUN, RUNDTTM, RECURNAME, OPRID, PRCSVERSION, RUNSTATUS, RQSTDTTM
, LASTUPDDTTM, BEGINDTTM, ENDDTTM, RUNCNTLID, PRCSRTNCD, CONTINUEJOB, USERNOTIFIED, INITIATEDNEXT, OUTDESTTYPE, OUTDESTFORMAT
, ORIGPRCSINSTANCE, GENPRCSTYPE, RESTARTENABLED, TIMEZONE, PSRF_FOLDER_NAME, SCHEDULENAME, RETRYCOUNT, RECURORIGPRCSINST
, P_PRCSINSTANCE, DISTSTATUS, PRCSCATEGORY, PRCSCURREXPIREDTTM, RUNSERVEROPTION, PT_RETENTIONDAYS, CONTENTID, PTNONUNPRCSID)
WITH 
o AS (select distinct dbname from ps.psdbowner
where ownerid = 'SYSADM'
), r as (
SELECT b.oprid, b.run_cntl_id runcntlid
FROM   PSNVSBOOKREQUST b
,      PS_NVS_REPORT n
WHERE  b.eff_status = 'A'
and    n.business_unit = b.business_unit
and    n.report_id = b.report_id
and    n.layout_id IN('ZBUVBS64')
and    b.oprid = 'BATCH'
and rownum = 1
)
SELECT 42, 41, 0, 0, ' ', 'nVision-ReportBook', 'RPTBOOK', 0, ' ', 0
, '2', '2', '2', o.DBNAME, ' ', ' ', null, ' ', r.oprid, 0, 5, sysdate
, sysdate, null, null, r.RUNCNTLID, 0, 0, 0, 0, 0, 0
, 0, '7', ' ', 'GMT', 'NVISION', ' ', 0, 0
, 0, ' ', 'Default', sysdate+42, ' ', 42, 0, 0
FROM o, r
/

insert into psprcsque
(PRCSINSTANCE, JOBINSTANCE, PRCSJOBSEQ, PRCSJOBNAME, MAINJOBINSTANCE, PRCSTYPE, PRCSNAME, MAINJOBNAME, MAINJOBSEQ, PRCSITEMLEVEL
, RUNLOCATION, OPSYS, SERVERNAMERQST, SERVERNAMERUN, SERVERASSIGN, RUNDTTM, RECURNAME, OPRID, PRCSPRTY, SESSIONIDNUM, RUNSTATUS
, RQSTDTTM, RECURDTTM, LASTUPDDTTM, RUNCNTLID, PRCSRTNCD, CONTINUEJOB, USERNOTIFIED, INITIATEDNEXT, OUTDESTTYPE, OUTDESTFORMAT
, ORIGPRCSINSTANCE, GENPRCSTYPE, RESTARTENABLED, TIMEZONE, EMAIL_WEB_RPT, EMAIL_LOG_FLAG, PTEMAILRPTURLTYPE, PSRF_FOLDER_NAME
, SCHEDULENAME, PRCSWINPOP, MCFREN_URL_ID, RETRYCOUNT, RECURORIGPRCSINST, P_PRCSINSTANCE, PRCSCATEGORY, PRCSCURREXPIREDTTM
, DISTSTATUS, PRCSSTARTDTTM, RUNSERVEROPTION, TUXSVCID, PT_RETENTIONDAYS, PRCSRUNNOTIFY, PTNONUNPRCSID, QRYXFORMNAME
, MSGNODENAME, CDM_APPROVAL_FLAG, PT_OVRDFROMEMAILID)
WITH 
o AS (select distinct dbname from ps.psdbowner
where ownerid = 'SYSADM'
), r as (
SELECT DISTINCT b.oprid, b.run_cntl_id runcntlid, i.jobnamesrc
FROM   ps_schdlitem i
,      PSNVSBOOKREQUST b
,      PS_NVS_REPORT n
WHERE  b.eff_status = 'A'
and    n.business_unit = b.business_unit
and    n.report_id = b.report_id
and    n.layout_id IN('ZBUVBS64')
and    b.oprid = 'BATCH'
and    i.prcstype = 'nVision-ReportBook'
and    i.prcsname = 'RPTBOOK'
and    i.run_cntl_id = b.run_cntl_id
and rownum = 1
)
SELECT 41, 41, 0, jobnamesrc, 0, 'PSJob', jobnamesrc, ' ', 0, 0
, '2', '2', ' ', ' ', ' ', sysdate, ' ', r.OPRID, 5, 0, 5
, sysdate, null, sysdate, r.RUNCNTLID, 0, 0, 0, 0, '6', '8'
, 0, '7', '1', 'PST', ' ', ' ', 0, ' '
, ' ', ' ', ' ', 0, 0, 0, 'nVisionOpenXML', null
, ' ', null, '1', 0, 0, 0, ' ', ' '
, ' ', ' ', ' '
FROM o, r
/

insert into psprcsque
(PRCSINSTANCE, JOBINSTANCE, PRCSJOBSEQ, PRCSJOBNAME, MAINJOBINSTANCE, PRCSTYPE, PRCSNAME, MAINJOBNAME, MAINJOBSEQ, PRCSITEMLEVEL
, RUNLOCATION, OPSYS, SERVERNAMERQST, SERVERNAMERUN, SERVERASSIGN, RUNDTTM, RECURNAME, OPRID, PRCSPRTY, SESSIONIDNUM, RUNSTATUS
, RQSTDTTM, RECURDTTM, LASTUPDDTTM, RUNCNTLID, PRCSRTNCD, CONTINUEJOB, USERNOTIFIED, INITIATEDNEXT, OUTDESTTYPE, OUTDESTFORMAT
, ORIGPRCSINSTANCE, GENPRCSTYPE, RESTARTENABLED, TIMEZONE, EMAIL_WEB_RPT, EMAIL_LOG_FLAG, PTEMAILRPTURLTYPE, PSRF_FOLDER_NAME
, SCHEDULENAME, PRCSWINPOP, MCFREN_URL_ID, RETRYCOUNT, RECURORIGPRCSINST, P_PRCSINSTANCE, PRCSCATEGORY, PRCSCURREXPIREDTTM
, DISTSTATUS, PRCSSTARTDTTM, RUNSERVEROPTION, TUXSVCID, PT_RETENTIONDAYS, PRCSRUNNOTIFY, PTNONUNPRCSID, QRYXFORMNAME
, MSGNODENAME, CDM_APPROVAL_FLAG, PT_OVRDFROMEMAILID)
WITH o AS (
select distinct dbname from ps.psdbowner
where ownerid = 'SYSADM'
), r as (
SELECT b.oprid, b.run_cntl_id runcntlid
FROM   PSNVSBOOKREQUST b
,      PS_NVS_REPORT n
WHERE  b.eff_status = 'A'
and    n.business_unit = b.business_unit
and    n.report_id = b.report_id
and    n.layout_id IN('ZBUVBS64')
and    b.oprid = 'BATCH'
and rownum = 1
)
SELECT 42, 41, 0, ' ', 0, 'nVision-ReportBook', 'RPTBOOK', ' ', 0, 0
, '2', '2', ' ', ' ', ' ', sysdate, ' ', r.oprid, 5, 0, 5
, sysdate, null, sysdate, r.RUNCNTLID, 0, 0, 0, 0, '6', '8'
, 0, '7', '1', 'PST', ' ', ' ', 0, ' '
, ' ', ' ', ' ', 0, 0, 0, 'nVisionOpenXML', null
, ' ', null, '1', 0, 0, 0, ' ', ' '
, ' ', ' ', ' '
FROM o, r
/

select prcsinstance, jobinstance, prcstype, prcsname, prcscategory, oprid, runcntlid
from psprcsrqst where prcsinstance IN(41,42);
select prcsinstance, prcstype, prcsname, prcscategory, oprid, runcntlid
from psprcsque where prcsinstance IN(41,42);

set lines 200 trimspool on 
column text format a180
--select line, text from user_source where name like 'GFC_NVISION_EXCEL_REDIRECT_RQST' order by line;

delete from psprcsrqst where prcsinstance IN(41,42);
delete from psprcsque where prcsinstance IN(41,42);
drop TRIGGER sysadm.gfc_nvision_excel_redirect_rqst;
drop TRIGGER sysadm.gfc_nvision_excel_redirect_que;
drop TRIGGER sysadm.gfc_nvision_excel_redirect_jobrqst;
drop TRIGGER sysadm.gfc_nvision_excel_redirect_jobque;

--exec dbms_preprocessor.print_post_processed_source('TRIGGER',user,'GFC_NVISION_EXCEL_REDIRECT_QUE');
--exec dbms_preprocessor.print_post_processed_source('TRIGGER',user,'GFC_NVISION_EXCEL_REDIRECT_RQST');

****************************************************************************************************/
set termout on
