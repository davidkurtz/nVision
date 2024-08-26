REM fga_handler.sql
REM 16.6.17 increase length of parm variables to 64 chart
spool fga_handler

REM requires psftapi 
REM requires explicit select privs
ALTER SESSION SET current_schema=SYSADM;
GRANT SELECT ON sys.fga_log$ TO SYSADM;

CREATE OR REPLACE PROCEDURE sysadm.gfc_fga_nvision_handler
(object_schema VARCHAR2
,object_name   VARCHAR2
,policy_name   VARCHAR2)
AS
  l_sqlbind VARCHAR2(4000);
  l_parm1   VARCHAR2(30);
  l_parm2   VARCHAR2(30);
  l_parm3   VARCHAR2(30);
  l_parm4   VARCHAR2(30);
BEGIN
  BEGIN
    SELECT x.lsqlbind
    ,      SUBSTR(x.lsqlbind,x.start1,LEAST(30,NVL(x.end1,x.lensqlbind+1)-x.start1)) parm1
    ,      SUBSTR(x.lsqlbind,x.start2,LEAST(30,NVL(x.end2,x.lensqlbind+1)-x.start2)) parm2
    ,      SUBSTR(x.lsqlbind,x.start3,LEAST(30,NVL(x.end3,x.lensqlbind+1)-x.start3)) parm3
    ,      SUBSTR(x.lsqlbind,x.start4,LEAST(30,NVL(x.end4,x.lensqlbind+1)-x.start4)) parm4
    INTO   l_sqlbind, l_parm1, l_parm2, l_parm3, l_parm4
    FROM   (
      SELECT l.*
      ,      NULLIF(REGEXP_INSTR(lsqlbind,' #[0-9]+\([0-9]+\)\:',1,1,1,'i'),0) start1
      ,      NULLIF(REGEXP_INSTR(lsqlbind,' #[0-9]+\([0-9]+\)\:',1,2,0,'i'),0) end1
      ,      NULLIF(REGEXP_INSTR(lsqlbind,' #[0-9]+\([0-9]+\)\:',1,2,1,'i'),0) start2
      ,      NULLIF(REGEXP_INSTR(lsqlbind,' #[0-9]+\([0-9]+\)\:',1,3,0,'i'),0) end2
      ,      NULLIF(REGEXP_INSTR(lsqlbind,' #[0-9]+\([0-9]+\)\:',1,3,1,'i'),0) start3
      ,      NULLIF(REGEXP_INSTR(lsqlbind,' #[0-9]+\([0-9]+\)\:',1,4,0,'i'),0) end3
      ,      NULLIF(REGEXP_INSTR(lsqlbind,' #[0-9]+\([0-9]+\)\:',1,4,1,'i'),0) start4
      ,      NULLIF(REGEXP_INSTR(lsqlbind,' #[0-9]+\([0-9]+\)\:',1,5,1,'i'),0) end4
      ,      LENGTH(lsqlbind) lensqlbind
      FROM   sys.fga_log$ l
    ) x
    WHERE  x.sessionid = USERENV('SESSIONID')
    AND    x.entryid   = USERENV('ENTRYID')
    AND    x.obj$name  = 'PS_NVS_REPORT';
  EXCEPTION
    WHEN no_data_found THEN
      RAISE_APPLICATION_ERROR(-20000,'GFC_FGA_NVISION_HANDER: No Audit Row');
  END;

  IF l_parm4 IS NULL THEN
    l_parm4 := l_parm3;
    l_parm3 := l_parm2;
    l_parm2 := l_parm1;
  END IF;

  IF l_parm4 IS NULL THEN
    l_parm4 := l_parm3;
    l_parm3 := l_parm2;
  END IF;

  IF l_parm4 IS NULL THEN
    l_parm4 := l_parm3;
  END IF;

  dbms_output.put_line(l_sqlbind);
  dbms_output.put_line(l_parm1);
  dbms_output.put_line(l_parm2);
  dbms_output.put_line(l_parm3);
  dbms_output.put_line(l_parm4);

  dbms_application_info.set_action(SUBSTR('PI='||psftapi.get_prcsinstance()||':'||l_parm4||':'||l_parm3,1,64));
--EXECUTE IMMEDIATE 'ALTER SESSION SET TRACEFILE_IDENTIFIER=''PI='||psftapi.get_prcsinstance()||':'||l_parm4||':'||l_parm3||'''';
END;
/
show errors
/**********************************************************/
rem this is a test - expected output
rem ERROR at line 1:
rem ORA-20000: GFC_FGA_NVISION_HANDER: No Audit Row
rem ORA-06512: at "SYSADM.GFC_FGA_NVISION_HANDLER", line 28
rem ORA-06512: at line 1
exec gfc_fga_nvision_handler('SYSADM','PS_NVS_REPORT','PS_NVS_REPORT_SEL');
select * from ps_nvs_report where rownum <= 1;
exec gfc_fga_nvision_handler('SYSADM','PS_NVS_REPORT','PS_NVS_REPORT_SEL');
rem but if not error it is because an audit has run in this session

set serveroutput on 
declare
  l_module VARCHAR2(64);
  l_action VARCHAR2(64);
begin
  dbms_application_info.read_module(l_module, l_action);
  dbms_output.put_line('Module:'||l_module);
  dbms_output.put_line('Action:'||l_action);
end;
/

/**********************************************************
 * This policy logs query on the control table from which nVision parameters are read
 * The error handle is used to name the oracle trace file.  NB creates 0 length file even if trace not invoked
/**********************************************************/
BEGIN
  DBMS_FGA.DROP_POLICY(
   object_schema      => 'SYSADM',
   object_name        => 'PS_NVS_REPORT',
   policy_name        => 'PS_NVS_REPORT_SEL');
END;
/

BEGIN
  DBMS_FGA.ADD_POLICY(
   object_schema      => 'SYSADM',
   object_name        => 'PS_NVS_REPORT',
   policy_name        => 'PS_NVS_REPORT_SEL',
   handler_schema     => 'SYSADM',
   handler_module     => 'GFC_FGA_NVISION_HANDLER',
   enable             =>  TRUE,
   statement_types    => 'SELECT',
   audit_trail        =>  DBMS_FGA.DB + DBMS_FGA.EXTENDED);
END;
/
select * from dba_audit_policies;
/**********************************************************/
spool off
