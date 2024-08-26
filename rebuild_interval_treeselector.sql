REM rebuild_interval_treeselector.sql
REM 22.02.23 added additional query for PSTREESELNUM in NVEXEC schemas
clear screen 
column username format a12
set pages 0 head off feedback off echo off
spool rebuild_interval_treeselector1.sql
alter session set current_schema=SYSADM;
with u as (
      SELECT username
      FROM dba_users
      WHERE (username = 'SYSADM' OR username like 'NVEXEC%')
      --AND username = 'NVEXEC30'
), r as (
      SELECT recname, DECODE(sqltablename,' ','PS_'||recname,sqltablename) sqltablename
      , SUBSTR(recname,-2) n
      FROM psrecdefn
      WHERE recname like 'PSTREESELECT__'
)
SELECT DISTINCT '@@pstreeselect_interval_partitioning.sql', u.username, r.n
FROM u, r
WHERE NOT EXISTS(
  SELECT 'x'
  FROM dba_part_Tables p
  WHERE p.owner = u.username
  and p.table_name = r.sqltablename
  and p.partitioning_type = 'RANGE' and p.interval = '1'
  and not p.table_name IN('PSTREESELECT05','PSTREESELECT06','PSTREESELECT10') /*force recreate these tables*/)
--and 1=2
UNION ALL
SELECT DISTINCT '@@pstreeselnum.sql', u.username, NULL --added 22.2.2023 to put selectornum sequence generator into NVEXEC schemas
FROM u
WHERE NOT EXISTS(
  SELECT 'x'
  FROM dba_tables p
  WHERE p.owner = u.username
  and p.table_name = 'PSTREESELNUM' AND 1=2)
ORDER BY n nulls first, username
--FETCH FIRST 10 ROWS ONLY
/
spool off

set pages 99 head on feedback on echo on verify on 
spool rebuild_interval_treeselector append
clear screen
alter session set current_schema = SYSADM;
EXEC sysadm.psft_Ddl_lock.set_Ddl_permitted(true);
REM @@rebuild_interval_treeselector1.sql
EXEC sysadm.psft_Ddl_lock.set_Ddl_permitted(false);
