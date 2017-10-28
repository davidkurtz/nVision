rem pstreeselector_stats.sql
rem trigger to update stats on selector table as pstreeselctl is populated.
rem NB stats job is only fired on commit.
set echo on feedback on verify on termout on lines 200 trimspool on
column table_name format a18

spool pstreeselector_stats
ROLLBACK;

CREATE OR REPLACE TRIGGER sysadm.pstreeselector_stats
BEFORE INSERT OR UPDATE ON sysadm.pstreeselctl
FOR EACH ROW
DECLARE
  l_jobno      NUMBER;
  l_cmd        VARCHAR2(1000);
  l_table_name VARCHAR2(18);
  l_suffix     VARCHAR2(2);
BEGIN
  l_table_name := 'PSTREESELECT'||LTRIM(TO_CHAR(:new.length,'00'));
  l_suffix     := SUBSTR(l_table_name,-2);
  l_cmd := 'dbms_stats.gather_table_stats(ownname=>user,tabname=>'''||l_table_name||''',force=>TRUE);'
--       ||'dbms_stats.set_column_stats(ownname=>user,tabname=>'''||l_table_name||''',colname=>''RANGE_FROM_'||l_suffix||''',density=>1,force=>TRUE);'
--       ||'dbms_stats.set_column_stats(ownname=>user,tabname=>'''||l_table_name||''',colname=>''RANGE_TO_'||l_suffix||''',density=>1,force=>TRUE);'
         ;
  dbms_output.put_line(l_cmd);
  dbms_job.submit(l_jobno,l_cmd);
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

set serveroutput on
show errors

rollback; 
alter session set nls_date_format='hh24:mi:ss dd.mm.yyyy';
INSERT INTO sysadm.pstreeselctl VALUES('GFC',' ','GFC_TEST',sysdate,42,42,sysdate,'R',1);
commit;
delete from sysadm.pstreeselctl where setid = 'GFC';
commit;
pause
select table_name, num_rows, last_analyzed from user_tables where table_name like 'PSTREESELECT__' order by 1,2,3
/
select table_name, column_name, num_distinct, density from user_tab_columns where table_name = 'PSTREESELECT01'
/
spool off
