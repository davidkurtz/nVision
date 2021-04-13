REM nvision_current_schema_trigger.sql
spool nvision_current_schema_trigger
--------------------------------------------------------------------------------
--nvision set current schema 
--------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER sysadm.nvision_current_schema
BEFORE UPDATE OF runstatus ON sysadm.psprcsrqst
FOR EACH ROW
WHEN (new.runstatus IN('7') AND new.prcsname IN('RPTBOOK','NVSRUN') AND new.prcstype like 'nVision%')
BEGIN
--EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema = NVEXEC'||LTRIM(TO_CHAR(dbms_utility.get_hash_value(:new.prcsinstance,1,16),'00'));
  EXECUTE IMMEDIATE 'ALTER SESSION SET current_schema = NVEXEC'||LTRIM(TO_CHAR(MOD(:new.prcsinstance,16),'00'));
EXCEPTION WHEN OTHERS THEN NULL; --exception deliberately coded to suppress all exceptions
END;
/
show errors
spool off