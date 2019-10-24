REM treeselectorclup2.sql
set termout on head off echo off feedback off timi off trimspool on trimout on pages 0 serveroutput on lines 200
spool treeselectorclup2

alter session set current_schema=SYSADM;
DECLARE
  l_sqlc CLOB;
  l_sqld CLOB;
  l_numrows INTEGER;
  l_numsels INTEGER;
BEGIN
  FOR i IN (
    SELECT table_name
    FROM   user_tables
    where table_name LIKE 'PSTREESELECT__'
    ORDER BY 1
  ) LOOP
    l_sqlc := 'SELECT COUNT(DISTINCT selector_Num), COUNT(*) FROM '||i.table_name;
    l_sqld := 'DELETE FROM '||i.table_name||' t WHERE NOT t.selector_num IN(SELECT DISTINCT selector_num FROM pstreeselctl)';

    EXECUTE IMMEDIATE l_sqlc INTO l_numsels, l_numrows;
    dbms_output.put_line('Table '||i.table_name||':'||l_numsels||' selectors,'||l_numrows||' rows');

    IF l_numrows > 0 THEN
      dbms_output.put_line('SQL:'||l_sqld);
      EXECUTE IMMEDIATE l_sqld;
      dbms_output.put_line(SQL%ROWCOUNT||' rows deleted.');

      EXECUTE IMMEDIATE l_sqlc INTO l_numsels, l_numrows;
      dbms_output.put_line('Table '||i.table_name||':'||l_numsels||' selectors,'||l_numrows||' rows');

      COMMIT;
    END IF;
  END LOOP;
END;
/
spool off
show errors
