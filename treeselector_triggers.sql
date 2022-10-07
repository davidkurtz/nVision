REM treeselector_triggers.sql
set serveroutput on echo on
clear screen
spool treeselector_triggers
--------------------------------------------------------------------------------
--log selector population - dynamically create insert/delete triggers
--------------------------------------------------------------------------------
DECLARE
  l_cmd CLOB;
  l_updstats BOOLEAN := TRUE; /*TRUE=maintain tree selector statistic, FALSE=monitor only*/
BEGIN
  FOR i IN (
    WITH x AS (
      SELECT 'INSERT' action, 'logins' logproc, 'rowins' rowproc, 'AFTER' beforeafter, 'new' newold
      FROM DUAL
      UNION ALL
      SELECT 'DELETE',        'logdel',         'rowdel',         'BEFORE',            'old'
      FROM DUAL
    )
    SELECT r.recname, t.owner
    ,      SUBSTR(recname,-2) length
    ,      x.action, x.logproc, x.rowproc, x.beforeafter, x.newold
    FROM   psrecdefn r
    ,      all_tables t
    ,      x
    WHERE  r.recname = r.sqltablename
    AND    r.recname like 'PSTREESELECT__'
    AND    t.table_name = r.sqltablename
    AND    (t.owner = 'SYSADM' or t.owner LIKE 'NVEXEC%')
  ) LOOP
  l_cmd := 'CREATE OR REPLACE TRIGGER '||i.owner||'.'||LOWER(i.recname||'_'||i.action)||' FOR '||i.action||' ON '||i.owner||'.'||i.recname||' compound trigger
  l_err_msg VARCHAR2(100 CHAR);
AFTER EACH ROW IS 
BEGIN
  sysadm.xx_nvision_selectors.'||i.rowproc||'(:'||i.newold||'.selector_num';

  IF i.rowproc = 'rowins' THEN
    l_cmd := l_cmd||',:'||i.newold||'.range_from_'||i.length||',:'||i.newold||'.range_to_'||i.length;
  END IF;

  l_cmd := l_cmd||');
EXCEPTION WHEN OTHERS THEN NULL;
END after each row;
AFTER STATEMENT IS
BEGIN
  sysadm.xx_nvision_selectors.'||i.logproc||'('||i.length;
  IF i.logproc = 'logins' THEN
    l_cmd := l_cmd||','''||i.owner||'''';
    l_cmd := l_cmd||','||CASE WHEN l_updstats THEN 'TRUE' ELSE 'FALSE' END;
  END IF;
  l_cmd := l_cmd||');
EXCEPTION WHEN OTHERS THEN 
  l_err_msg := SUBSTR(SQLERRM,1,100);
  dbms_output.put_line(''Error:''||l_err_msg);
END after statement;
END;';
    dbms_output.put_line(l_cmd);
    EXECUTE IMMEDIATE l_cmd;
  END LOOP;
END;
/
show errors

column owner format a8
column table_name format a18
column trigger_name format a30
select owner, table_name, trigger_name, status
from all_Triggers
where table_name like 'PSTREESEL%'
order by 1,2,3
/

EXEC DBMS_UTILITY.compile_schema(schema => 'SYSADM');

spool off
