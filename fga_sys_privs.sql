REM fga_sys_privs.sql

set echo on
spool fga_sys_privs

GRANT SELECT ON sys.fga_log$ TO sysadm
/
GRANT EXECUTE ON sys.dbms_fga TO sysadm
/

GRANT SELECT ON sys.v_$sql TO sysadm
/
GRANT SELECT ON sys.gv_$sql TO sysadm
/


CREATE INDEX sys.fga_log$_obj$name
ON sys.fga_log$ (obj$name, sessionid, entryid)
TABLESPACE sysaux PCTFREE 1 COMPRESS 1 
/

spool off