REM nvision_current_schema_trigger.sql
set serveroutput on echo on lines 120 pages 99
clear screen
spool nvision_current_schema_trigger
--------------------------------------------------------------------------------
--nvision set current schema - the number of schemas that will be used in the trigger should be around 1.8 * CPU_COUNT
--This is the number of NVEXEC schemas in use.  We currently pre-create the necessary partitions in NVEXEC01-22, so this is now constant at 22.
COLUMN num_nvexec_schemas NEW_VALUE num_nvexec_schemas
select 30 num_nvexec_schemas from dual;
--------------------------------------------------------------------------------
rollback;
alter session set current_schema = sysadm;
--------------------------------------------------------------------------------
DROP TRIGGER sysadm.nvision_current_schema2;
CREATE OR REPLACE TRIGGER sysadm.nvision_current_schema
BEFORE UPDATE OF runstatus ON sysadm.psprcsrqst
FOR EACH ROW
FOLLOWS sysadm.psftapi_store_prcsinstance, sysadm.set_prcs_sess_parm
WHEN (new.runstatus = '7' AND new.prcsname IN('RPTBOOK','NVSRUN') AND new.prcstype like 'nVision%')
DECLARE 
  l_sql VARCHAR2(100);
BEGIN
  l_sql := 'ALTER SESSION SET current_schema = NVEXEC'||LTRIM(TO_CHAR(MOD(:new.prcsinstance,&&num_nvexec_schemas)+1,'00'));
  dbms_output.put_line('nvision_current_schema:'||l_sql);
  EXECUTE IMMEDIATE l_sql;
EXCEPTION WHEN OTHERS THEN NULL; --exception deliberately coded to suppress all exceptions
END;
/
show errors
--------------------------------------------------------------------------------
ALTER TRIGGER sysadm.xx_nvision_end DISABLE;
clear screen 
set lines 120 pages 99
column owner format a10
column table_owner heading 'Table|Owner' format a10
column base_object_name heading 'Base|Object|Type' format a10
column trigger_name format a40
column triggering_event format a20
column when_clause format a120
column description format a90
column table_name format a20
column column_name format a20
column referencing_names format a35
column trigger_body format a120
select * from all_triggers
where table_name = 'PSPRCSRQST'
and status = 'ENABLED'
ORDER BY 1,2;
--------------------------------------------------------------------------------
set serveroutput on 
UPDATE psprcsrqst
SET runstatus = 7
WHERE prcsname = 'RPTBOOK'
AND begindttm >= sysdate-1
and rownum <= 2;
rollback;
--------------------------------------------------------------------------------
-- test script to show trigger working
--------------------------------------------------------------------------------
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
  and MOD(prcsinstance,&&num_nvexec_schemas)=0
  and rownum = 1
  RETURN prcsinstance INTO l_prcsinstance;
  
  select sys_context('userenv','current_schema') 
  into l_current_schema
  from dual;
  dbms_output.put_line('PI='||l_prcsinstance||', current_schema='||l_current_schema);

  UPDATE psprcsrqst
  SET runstatus = 7
  WHERE prcsname = 'RPTBOOK'
  and prcstype like 'nVision%'
  and MOD(prcsinstance,&&num_nvexec_schemas)=&&num_nvexec_schemas-1
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