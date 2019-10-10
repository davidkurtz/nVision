REM fga_sys_privs.sql

set echo on
spool fga_sys_privs

GRANT SELECT ON sys.fga_log$ TO SYSADM
/

CREATE INDEX sys.fga_log$_obj$name
ON sys.fga_log$ (obj$name, sessionid, entryid)
TABLESPACE sysaux PCTFREE 1 COMPRESS 1 
/

spool off