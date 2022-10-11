REM rebuild_interval_treeselector.sql
set pages 0 head off feedback off echo off
spool pstreeselect_interval_partitioning
spool C:\Users\DAKURT0\Documents\sql\rebuild_interval_treeselector1.sql
with u as (
      SELECT username
      FROM dba_users
      WHERE (username = 'SYSADM' OR username like 'NVEXEC%')
), r as (
      SELECT recname, DECODE(sqltablename,' ','PS_'||recname,sqltablename) sqltablename
      , SUBSTR(recname,-2) n
      FROM psrecdefn
      WHERE recname like 'PSTREESELECT__'
)
SELECT '@@C:\Users\DAKURT0\Documents\sql\pstreeselect_interval_partitioning.sql', r.n, u.username
FROM u, r
--WHERE r.n = 1
ORDER BY r.n, u.username
--FETCH FIRST 10 ROWS ONLY
/
spool off
set pages 99 head on feedback on echo on verify on 
spool rebuild_interval_treeselector
clear screen
alter session set current_schema = SYSADM;
EXEC sysadm.psft_Ddl_lock.set_Ddl_permitted(true);
@@C:\Users\DAKURT0\Documents\sql\rebuild_interval_treeselector1
EXEC sysadm.psft_Ddl_lock.set_Ddl_permitted(false);
