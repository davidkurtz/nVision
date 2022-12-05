REM nvision_current_schema_trigger.sql
set serveroutput on echo on
spool nvision_current_schema_trigger
--------------------------------------------------------------------------------
--nvision set current schema 
--------------------------------------------------------------------------------
rollback;
alter session set current_schema = sysadm;

CREATE OR REPLACE TRIGGER sysadm.nvision_current_schema
BEFORE UPDATE OF runstatus ON sysadm.psprcsrqst
FOR EACH ROW
WHEN (new.runstatus IN('7') AND new.prcsname IN('RPTBOOK','NVSRUN') AND new.prcstype like 'nVision%')
BEGIN
--EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema = NVEXEC'||LTRIM(TO_CHAR(dbms_utility.get_hash_value(:new.prcsinstance,1,16),'00'));
  EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema = NVEXEC'||LTRIM(TO_CHAR(MOD(:new.prcsinstance,64),'00'));
EXCEPTION WHEN OTHERS THEN NULL; --exception deliberately coded to suppress all exceptions
END;
/
show errors
spool off
DECLARE 
  l_prcsinstance INTEGER := 0;
  l_current_schema VARCHAR2(30);
BEGIN  
  select sys_context('userenv','current_schema') 
  into l_current_schema
  from dual;
  dbms_output.put_line('PI='||l_prcsinstance||', current_schema='||l_current_schema);

  UPDATE psprcsrqst
  SET runstatus = 7
  WHERE prcsname = 'RPTBOOK'
  and prcstype like 'nVision%'
  and rownum = 1
  RETURN prcsinstance INTO l_prcsinstance;
  
  select sys_context('userenv','current_schema') 
  into l_current_schema
  from dual;
  dbms_output.put_line('PI='||l_prcsinstance||', current_schema='||l_current_schema);
END;
/

rollback;
spool off
