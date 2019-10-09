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
    dbms_output.put_line('Deteting and Locking stats on '||i.owner||'.'||i.table_name);
    dbms_stats.delete_table_stats(i.owner, i.table_name, FORCE=>TRUE);
    dbms_stats.lock_table_stats(i.owner, i.table_name);
  END LOOP;
END;
/
