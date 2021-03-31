REM gfc_nvsion_excel_redirect_triggers.sql
REM (c)Go-Faster Consultancy 2021
REM Triggers to switch process name from RPTBOOK to RPRBOOKE for reportbooks that have to be run on Excel

REM see also https://blogs.oracle.com/oraclemagazine/on-conditional-compilatio
set echo on
spool gfc_nvsion_excel_redirect_triggers
rollback;
ALTER SESSION SET PLSQL_CCFLAGS = 'mydebug:FALSE';

----------------------------------------------------------------------------------------------------
CREATE TABLE psoft.ps_nvs_redir_excel
(layout_id VARCHAR2(50) NOT NULL
,eff_status VARCHAR2(1) not null
);

ALTER TABLE psoft.ps_nvs_redir_excel add eff_status VARCHAR2(1); 
UPDATE psoft.ps_nvs_redir_excel SET eff_status = 'A';
ALTER TABLE psoft.ps_nvs_redir_excel MODIFY eff_status not null;

CREATE UNIQUE INDEX psoft.ps_nvs_redir_excel ON psoft.ps_nvs_redir_excel (layout_id);
----------------------------------------------------------------------------------------------------
REM load metadata of layouts that have to run on Excel rather than OpenXML
@@gfc_nvsion_excel_redirect_metadata
----------------------------------------------------------------------------------------------------
spool gfc_nvsion_excel_redirect_triggers app

CREATE OR REPLACE TRIGGER psoft.gfc_nvision_excel_redirect_rqst 
BEFORE INSERT ON psoft.psprcsrqst
FOR EACH ROW
WHEN (new.prcstype IN('nVision-Report','nVision-ReportBook')
AND   new.prcsname IN('RPTBOOK')
     )
DECLARE
  l_excel INTEGER := 0;  
  l_maxconcurrent INTEGER := 0;
BEGIN
  $IF $$mydebug $THEN dbms_output.put_line('Entering Trigger psoft.gfc_nvision_excel_redirect_rqst'); $END

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

  $IF $$mydebug $THEN dbms_output.put_line('found Excel nVision layout for oprid='||:new.oprid||', runcntlid='||:new.runcntlid); $END
  IF :new.prcsname IN('RPTBOOK') THEN
    :new.prcsname := 'RPTBOOKE';
  ELSE
    :new.prcsname := :new.prcsname||'E';
  END IF;

  SELECT d.prcscategory
  INTO   :new.prcscategory
  FROM   ps_prcsdefn d
  WHERE  d.prcstype = :new.prcstype
  AND    d.prcsname = :new.prcsname;

  SELECT maxconcurrent
  INTO   l_maxconcurrent
  FROM   ps_servercategory
  WHERE  prcscategory = :new.prcscategory
  AND    servername = :new.servernamerqst;

  --if request assigned to server where it cannot run blank out server assignment and allow load balancing to determine it
  IF l_maxconcurrent = 0 THEN
    :new.servernamerqst := ' ';
  END IF;

  $IF $$mydebug $THEN dbms_output.put_line('set process name:'||:new.prcsname||', category:'||:new.category); $END
EXCEPTION
  WHEN no_data_found THEN 
    $IF $$mydebug $THEN dbms_output.put_line('No excel redirect found'); $ELSE NULL; $END
  WHEN others THEN 
    $IF $$mydebug $THEN dbms_output.put_line('Other Error'); $ELSE NULL; $END
END;
/
show errors

CREATE OR REPLACE TRIGGER psoft.gfc_nvision_excel_redirect_que
BEFORE INSERT ON psoft.psprcsque
FOR EACH ROW
WHEN (new.prcstype IN('nVision-Report','nVision-ReportBook')
AND   new.prcsname IN('RPTBOOK')
     )
DECLARE
  l_excel         INTEGER := 0;  
  l_maxconcurrent INTEGER := 0;
BEGIN
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

  IF :new.prcsname IN('RPTBOOK') THEN
    :new.prcsname := 'RPTBOOKE';
  ELSE
    :new.prcsname := :new.prcsname||'E';
  END IF;

  SELECT d.prcscategory
  INTO   :new.prcscategory
  FROM   ps_prcsdefn d
  WHERE  d.prcstype = :new.prcstype
  AND    d.prcsname = :new.prcsname;

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
END;
/
show errors
spool off

/****************************************************************************************************
 * Test Script
 ****************************************************************************************************
REM test

set serveroutput on
delete from psprcsrqst where prcsinstance = 42;
delete from psprcsque where prcsinstance = 42;

INSERT INTO ps_nvs_redir_excel VALUES ('GLNVIHC2','A');

insert into psprcsrqst
(PRCSINSTANCE, JOBINSTANCE, MAINJOBINSTANCE, PRCSJOBSEQ, PRCSJOBNAME, PRCSTYPE, PRCSNAME, PRCSITEMLEVEL, MAINJOBNAME, MAINJOBSEQ
, RUNLOCATION, OPSYS, DBTYPE, DBNAME, SERVERNAMERQST, SERVERNAMERUN, RUNDTTM, RECURNAME, OPRID, PRCSVERSION, RUNSTATUS, RQSTDTTM
, LASTUPDDTTM, BEGINDTTM, ENDDTTM, RUNCNTLID, PRCSRTNCD, CONTINUEJOB, USERNOTIFIED, INITIATEDNEXT, OUTDESTTYPE, OUTDESTFORMAT
, ORIGPRCSINSTANCE, GENPRCSTYPE, RESTARTENABLED, TIMEZONE, PSRF_FOLDER_NAME, SCHEDULENAME, RETRYCOUNT, RECURORIGPRCSINST
, P_PRCSINSTANCE, DISTSTATUS, PRCSCATEGORY, PRCSCURREXPIREDTTM, RUNSERVEROPTION, PT_RETENTIONDAYS, CONTENTID, PTNONUNPRCSID)
WITH 
o AS (select distinct dbname from ps.psdbowner
), r as (
SELECT b.oprid, b.run_cntl_id runcntlid
FROM   PSNVSBOOKREQUST b
,      PS_NVS_REPORT n
WHERE  b.eff_status = 'A'
and    n.business_unit = b.business_unit
and    n.report_id = b.report_id
and    n.layout_id IN('EXCELNVS')
and    b.oprid = 'BATCH'
)
SELECT 42, 0, 0, 0, ' ', 'nVision-ReportBook', 'RPTBOOK', 0, ' ', 0
, '2', '2', '2', o.DBNAME, ' ', ' ', null, ' ', r.oprid, 0, 5, sysdate
, sysdate, null, null, r.RUNCNTLID, 0, 0, 0, 0, 0, 0
, 0, '7', ' ', 'GMT', 'NVISION', ' ', 0, 0
, 0, ' ', 'Default', sysdate+42, ' ', 42, 0, 0
FROM o, r
/

select prcsinstance, prcstype, prcsname, prcscategory, oprid, runcntlid
from psprcsrqst
where prcsinstance = 42;

select prcsinstance, prcstype, prcsname, prcscategory, oprid, runcntlid
from psprcsque
where prcsinstance = 42;

set lines 200 trimspool on 
column text format a180
select line, text from user_source
where name like 'GFC_NVISION_EXCEL_REDIRECT_RQST'
order by line;

delete from psprcsrqst where prcsinstance = 42;
delete from psprcsque where prcsinstance = 42;
drop TRIGGER psoft.gfc_nvision_excel_redirect_rqst;
drop TRIGGER psoft.gfc_nvision_excel_redirect_que;
****************************************************************************************************/
