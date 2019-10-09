REM lockselectorstats.sql

Set serveroutput on
BEGIN
  FOR i IN(
    SELECT owner, table_name
    FROM   dba_tables
    WHERE  table_name like 'PSTREESELECT__'
    AND    owner = 'SYSADM'
    ORDER BY 1,2
  ) LOOP
    dbms_output.put_line('UnLocking stats on '||i.owner||'.'||i.table_name);
    dbms_stats.unlock_table_stats(i.owner, i.table_name);
    dbms_stats.gather_table_stats(i.owner, i.table_name, FORCE=>TRUE);
  END LOOP;
END;
/
