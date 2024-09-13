REM pstreeindexalter.sql
set echo on
spool pstreeindexalter.lst

exec psft_ddl_lock.set_ddl_permitted(TRUE);

DROP INDEX SYSADM.PS_NVS_TREESLCTLOG;
DROP INDEX SYSADM.PSBNVS_TREESLCTLOG;

CREATE UNIQUE INDEX SYSADM.PS_NVS_TREESLCTLOG ON SYSADM.PS_NVS_TREESLCTLOG (OWNERID, SELECTOR_NUM) TABLESPACE PSINDEX;
CREATE INDEX SYSADM.PSBNVS_TREESLCTLOG ON SYSADM.PS_NVS_TREESLCTLOG (LENGTH, OWNERID, PARTITION_NAME) TABLESPACE PSINDEX;

exec psft_ddl_lock.set_ddl_permitted(FALSE);
spool off