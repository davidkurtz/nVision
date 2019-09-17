REM nvision_dynamic_selectors.sql

ROLLBACK;

ALTER SESSION SET current_schema=SYSADM;
GRANT SELECT on SYS.V_$SQL TO sysadm;

set echo on pages 99 lines 180 trimspool on
spool nvision_dynamic_selectors
--------------------------------------------------------------------------------
--selector logging table
--------------------------------------------------------------------------------
--Status Flag:
--I: Inserted
--D: Deleted
--S: Static Select
--X: Partition Dropped
--------------------------------------------------------------------------------
create table ps_nvs_treeslctlog
(selector_num       number not null
,process_instance   number not null
,length             number not null
,num_rows           number not null
,timestamp          timestamp not null
,module             VARCHAR2(64 CHAR) not null
,appinfo_action     VARCHAR2(64 CHAR) not null
,client_info        VARCHAR2(64 CHAR) not null
,status_flag        VARCHAR2(1 CHAR) not null
,tree_name          VARCHAR2(18 CHAR) default ' ' NOT NULL
,ownerid            varchar2(8 char) not null
,partition_name     varchar2(128 char) default ' ' not null
,job_no             integer not null
);
--alter table ps_nvs_treeslctlog add (tree_name varchar2(18 char) default ' ' not null );
--alter table ps_nvs_treeslctlog add (partition_name varchar2(128 char) default ' ' not null );

alter table ps_nvs_treeslctlog add num_rows number DEFAULT 0;
update ps_nvs_treeslctlog SET num_rows = 0 where num_rows IS NULL;
alter table ps_nvs_treeslctlog modify num_rows default 0 not null;

alter table ps_nvs_treeslctlog add ownerid varchar2(8);
alter table ps_nvs_treeslctlog modify ownerid default 'SYSADM';
update ps_nvs_treeslctlog SET ownerid = 'SYSADM' where ownerid IS NULL;
alter table ps_nvs_treeslctlog modify ownerid not null;

--alter table ps_nvs_treeslctlog modify status_flag not null;
--alter table ps_nvs_treeslctlog modify timestamp not null;
--alter table ps_nvs_treeslctlog modify module default ' ' not null;
--alter table ps_nvs_treeslctlog modify action default ' ' not null;
--alter table ps_nvs_treeslctlog modify client_info default ' ' not null;

alter table ps_nvs_treeslctlog add job_no integer;
update ps_nvs_treeslctlog set job_no = 0 where job_no is null;
alter table ps_nvs_treeslctlog modify job_no not null;

alter table ps_nvs_treeslctlog rename column action to appinfo_action;


CREATE UNIQUE INDEX ps_nvs_treeslctlog ON ps_nvs_treeslctlog (selector_num) TABLESPACE psindex
/
CREATE INDEX psanvs_treeslctlog ON ps_nvs_treeslctlog (process_instance, selector_num) TABLESPACE psindex
/
CREATE INDEX psbnvs_treeslctlog ON ps_nvs_treeslctlog (length, partition_name) TABLESPACE psindex
/

@@xx_nvision_selectors

spool nvision_dynamic_selectors append
--------------------------------------------------------------------------------
--purge selectors on termination of nVision process
--------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER sysadm.xx_nvision_end
AFTER UPDATE ON sysadm.psprcsrqst
FOR EACH ROW
WHEN (new.runstatus != '7' AND old.runstatus = '7' AND new.prcstype like 'nVision%')
BEGIN
  xx_nvision_selectors.purge_selectors(:old.prcsinstance);
EXCEPTION WHEN OTHERS THEN NULL; --exception deliberately coded to suppress all exceptions
END;
/
show errors
pause
--------------------------------------------------------------------------------
--mark/unmark static selectors
--------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER sysadm.xx_pstreeselctl_inc
AFTER INSERT OR UPDATE OR DELETE ON sysadm.pstreeselctl
FOR EACH ROW
BEGIN
  IF deleting THEN
    UPDATE ps_nvs_treeslctlog
    SET    status_flag = 'D'
    ,      tree_name = :old.tree_name
    WHERE  selector_num = :old.selector_num
    AND    status_flag = 'S';
  ELSE
    UPDATE ps_nvs_treeslctlog
    SET    status_flag = 'S'
    ,      tree_name = :new.tree_name
    WHERE  selector_num = :new.selector_num;
  END IF;
EXCEPTION WHEN OTHERS THEN NULL; --exception deliberately coded to suppress all exceptions
END;
/
--DROP TRIGGER sysadm.xx_pstreeselctl_inc;
show errors
pause

--------------------------------------------------------------------------------
--one-time fix to populate selector log with static selectors
--------------------------------------------------------------------------------
INSERT INTO ps_nvs_treeslctlog
(selector_num, process_instance, length, num_rows, timestamp, module, appinfo_action, client_info, status_flag, ownerid, tree_name, partition_name, job_no)
SELECT DISTINCT selector_num, 0, length, 0, selector_dt, ' ', ' ', ' ', 'S', sys_context( 'userenv', 'current_schema' ), tree_name, ' ', 0
FROM pstreeselctl c
WHERE NOT EXISTS(SELECt 'x' FROM ps_nvs_treeslctlog l WHERE l.selector_Num = c.selector_num)
/
COMMIT
/



--------------------------------------------------------------------------------
--one-time fix to populate selector log with contents of tree selector
--------------------------------------------------------------------------------
set serveroutput on
DECLARE
  l_sql CLOB;
BEGIN
  FOR i IN (
    SELECT owner, table_name, SUBSTR(table_name,-2) length
    FROM   all_tables t
    WHERE  table_name LIKE 'PSTREESELECT__' 
  ) LOOP
    l_sql := 'INSERT INTO ps_nvs_treeslctlog (selector_num, process_instance, length, num_rows, timestamp, module, appinfo_action, client_info, status_flag, tree_name, ownerid, partition_name, job_no) SELECT s.selector_num, 0, '||i.length||', COUNT(*), SYSDATE, '' '', '' '', '' '', ''I'', '' '', '''||i.owner||''', '' '', 0 
FROM '||i.owner||'.'||i.table_name||' s WHERE NOT EXISTS(SELECT 1 FROM ps_nvs_treeslctlog l WHERE l.selector_num = s.selector_num) GROUP BY s.selector_num';
    dbms_output.put_line(l_sql);
    EXECUTE IMMEDIATE l_sql;
    dbms_output.put_line(SQL%ROWCOUNT||' rows inserted');
  END LOOP;
END;
/

--------------------------------------------------------------------------------
--one-time fix to populate selector log according to partitions
--------------------------------------------------------------------------------
set serveroutput on
DECLARE
  l_sql CLOB;
  l_high_value VARCHAR2(30);
  l_selector_num INTEGER;
  l_counter INTEGER := 0;
BEGIN
  FOR i IN (  
    SELECT t.table_owner, substr(t.table_name,-2) length, t.high_value, NVL(t.num_rows,0) num_rows, o.created, t.partition_name
    FROM   all_tab_partitions t
    ,      all_objects o
    WHERE  t.table_name LIKE 'PSTREESELECT__' 
    AND    t.table_name = o.object_name
    AND    t.partition_name = o.subobject_name
    AND    o.owner = t.table_owner
    AND    o.object_Type = 'TABLE PARTITION'
  ) LOOP
    l_high_Value := i.high_Value;
    l_selector_Num := TO_NUMBER(l_high_value)-1;
    l_sql := 'INSERT INTO ps_nvs_treeslctlog (selector_num, process_instance, length, num_rows, timestamp, module, appinfo_action, client_info, status_flag, tree_name, ownerid, partition_name, job_no) 
              VALUES (:1, 0, :2, :3, :4, '' '', '' '', '' '', ''I'', '' '', :5, :6, 0 )';
    BEGIN
      EXECUTE IMMEDIATE l_sql USING l_selector_num, i.length, i.num_rows, i.created, i.table_owner, i.partition_name;
      l_counter := l_counter+1;
    EXCEPTION WHEN dup_Val_on_index THEN NULL;
    END;
  END LOOP;
  dbms_output.put_line(l_counter||' rows inserted');
END;
/

--------------------------------------------------------------------------------
--one time fixes to put tree names onto log
--------------------------------------------------------------------------------
MERGE INTO ps_nvs_treeslctlog u
USING (
  SELECT DISTINCT l.selector_num, l.length
  ,      substr(regexp_substr(s.SQL_TEXT,'TREE_NAME=\''[^'']+'),12) tree_name
  FROM   ps_nvs_treeslctlog l
  ,      v$sql s
  where	 l.tree_name IS NULL
  and	 l.module = s.module
  and	 l.appinfo_action = s.action
  and    s.sql_text like 'INSERT%PSTREESELECT%SELECT%'
  and	 s.sql_text like 'INSERT%PSTREESELECT'||LTRIM(TO_CHAR(l.length,'00'))||'%SELECT% '||l.selector_num||'%'
  and    l.tree_name = ' '
) S
ON (s.selector_num = u.selector_num)
WHEN MATCHED THEN UPDATE
SET u.tree_name = s.tree_name
/

MERGE INTO ps_nvs_treeslctlog u
USING (
  SELECT DISTINCT l.selector_num, l.length
  ,      substr(CAST(regexp_substr(s.SQL_TEXT,'TREE_NAME=\''[^'']+') AS VARCHAR2(100)),12) tree_name
  FROM   ps_nvs_treeslctlog l
  ,      v$database d
  ,      dba_hist_sqltext s
  ,      dba_hist_sqlstat t
  where	 1=1 --l.tree_name IS NULL
  and	 t.dbid = d.dbid
  and    s.dbid = d.dbid
  and    s.sql_id = t.sql_id
  and	 t.module = l.module
  and	 t.action = l.appinfo_action
  and    s.sql_text like 'INSERT%PSTREESELECT%SELECT%'
  and	 s.sql_text like 'INSERT%PSTREESELECT'||LTRIM(TO_CHAR(l.length,'00'))||'%SELECT% '||l.selector_num||'%'
  and    l.tree_name = ' '
) S
ON (s.selector_num = u.selector_num)
WHEN MATCHED THEN UPDATE
SET u.tree_name = s.tree_name
/

DECLARE
  l_sql CLOB;
BEGIN
  FOR i IN (SELECT DISTINCT LTRIM(TO_CHAR(length,'00')) length FROM ps_nvs_treeslctlog WHERE tree_name = ' ') LOOP
  l_sql := 'MERGE INTO ps_nvs_treeslctlog u
USING (
WITH X AS (
SELECT	l.selector_num, MIN(s.tree_node_num) tree_node_num
FROM	ps_nvs_treeslctlog l
,	pstreeselect'||i.length||' s
WHERE	l.length = '||i.length||'
AND	l.selector_num = s.selector_num
AND	l.tree_name = '' ''
GROUP BY l.selector_num
)
SELECT DISTINCT x.selector_num, MAX(t.tree_name) tree_name
FROM	x
,	pstreenode t
WHERE 	t.tree_node_num = x.tree_node_num
GROUP BY x.selector_num
HAVING COUNT(distinct tree_name)=1
ORDER BY 1,2
) s 
ON (s.selector_num = u.selector_num)
WHEN MATCHED THEN UPDATE
SET u.tree_name = s.treE_name';
  execute immediate l_sql;
  END LOOP;
END;
/

--------------------------------------------------------------------------------
--one-time fix to set static selector flag 
--------------------------------------------------------------------------------
MERGE INTO ps_nvs_treeslctlog u
USING (
  SELECT c.selector_num, c.tree_name
  FROM   ps_nvs_treeslctlog l
  ,      pstreeselctl c
  WHERE  c.selector_num = l.selector_num
  AND    (l.tree_name = ' ' OR l.status_flag != 'S')
  AND    l.ownerid = sys_context( 'userenv', 'current_schema' )
) S
ON (s.selector_num = u.selector_num)
WHEN MATCHED THEN UPDATE
SET u.tree_name = s.tree_name
, u.status_flag = 'S'
/
UPDATE ps_nvs_treeslctlog l
SET    l.status_flag = 'D'
WHERE  l.status_flag = 'S'
AND    l.ownerid = sys_context( 'userenv', 'current_schema' )
AND NOT EXISTS(
	SELECT 'x'
	FROM   pstreeselctl c
	WHERE  c.selector_num = l.selector_num)
/
commit
/

--------------------------------------------------------------------------------
--one-time fix to add partition name to selector log
--------------------------------------------------------------------------------
set serveroutput on 
DECLARE
  l_selector_num INTEGER;
BEGIN
  FOR i IN (
    SELECT table_owner, table_name, partition_position, partition_name, high_value, high_value_length
    FROM   all_tab_partitions p
    WHERE  table_name LIKE 'PSTREESELECT__' 
    ORDER BY table_name, partition_position desc
  ) LOOP
    l_selector_num := SUBSTR(i.high_value,1,i.high_value_length) - 1;
    UPDATE ps_nvs_treeslctlog
    SET    ownerid = i.table_owner
    ,      partition_name = i.partition_name
    WHERE  selector_num = l_selector_num
    AND    partition_name = ' ';
    dbms_output.put_line(i.table_owner||'.'||i.table_name||':'||l_selector_num||':'||i.partition_name);
  END LOOP;
END;
/

UPDATE ps_nvs_treeslctlog
SET    status_flag = 'X'
WHERE  partition_name = ' '
and    status_flag IN('I','D')
/


--------------------------------------------------------------------------------
--one-time fix purge selectors
--------------------------------------------------------------------------------
set serveroutput on 
BEGIN
  FOR i IN (
    SELECT DISTINCT process_instance
    ,      length, ownerid, partition_name
    FROM   ps_nvs_treeslctlog l
    ,	   psprcsrqst r
    WHERE  r.prcsinstance = l.process_instance
    and	   r.runstatus IN('2','9')
    and	   l.partition_name != ' '
  ) LOOP
    dbms_output.put_line('Purging PI:'||i.process_instance);
    xx_nvision_selectors.purge_selectors(i.process_instance);
  END LOOP;
END;
/



@@treeselector_triggers


/*-------------------------------------------------------------------------------------------------------------------------------------
/*--Test script
/*-------------------------------------------------------------------------------------------------------------------------------------
set pages 99 lines 200 serveroutput on 
column selector_num heading 'Selector|Number' format 999999 
column table_name format a18
column ownerid heading 'Owner ID' format a8
column partition_position heading 'Part|Pos' format 999
column partition_name format a20
column process_instance heading 'Process|Instance' format 99999999
column length format 99
column num_rows heading 'Num|Rows'
column high_value format a20
column client_info format a48
column module format a12
column timestamp format a28
rollback;
exec dbms_application_info.set_module('TEST_MODULE','TEST_ACTION');
delete from ps_nvs_treeslctlog where selector_num = 42;
DELETE FROM PSTREESELECT10 WHERE SELECTOR_NUM=42;
SELECT * FROM ps_nvs_treeslctlog WHERE SELECTOR_NUM=42;
select * from user_tab_partitions where table_name = 'PSTREESELECT10' AND partition_position<4;
commit;
INSERT INTO PSTREESELECT10(SELECTOR_NUM,TREE_NODE_NUM,RANGE_FROM_10,RANGE_TO_10) 
SELECT DISTINCT 42,L.TREE_NODE_NUM, SUBSTR(L.RANGE_FROM,1,10),SUBSTR(L.RANGE_TO,1,10) 
FROM PSTREELEAF L WHERE L.SETID='GLOBE' AND L.SETCNTRLVALUE=' ' AND L.TREE_NAME='GAAP_ACCOUNT' AND L.EFFDT=TO_DATE('1901-01-01','YYYY-MM-DD')
AND rownum <= 10;

select sql_id, sql_text, module, action
, substr(regexp_substr(s.SQL_TEXT,'SETID=\''[^'']+'),8) setid
, substr(regexp_substr(s.SQL_TEXT,'TREE_NAME=\''[^'']+'),12) tree_name
FROM   sys.v_$sql s
where s.sql_text like 'INSERT%PSTREESELECT%SELECT DISTINCT %,%GAAP_ACCOUNT%'
and rownum = 1
/
select DISTINCT sql_id, substr(regexp_substr(s.SQL_TEXT,'TREE_NAME=\''[^'']+'),12) tree_name
, sql_text
FROM   sys.v_$sql s
WHERE  module = 'TEST_MODULE'
AND    action = 'TEST_ACTION'
and s.sql_text like 'INSERT INTO '||'PSTREESELECT10'||'%SELECT%'
and s.sql_text like 'INSERT INTO '||'PSTREESELECT10'||'%SELECT% '||42||',%'
/

commit;
select * from user_jobs where what like '%dbms_stats%';
SELECT * FROM ps_nvs_treeslctlog WHERE SELECTOR_NUM=42;
select table_name, num_rows, last_analyzed
from user_tables where table_name = 'PSTREESELECT10';
select table_name, partition_position, partition_name, high_value, num_rows, last_analyzed
from user_tab_partitions where table_name = 'PSTREESELECT10';

DELETE FROM PSTREESELECT10 WHERE SELECTOR_NUM=42;
commit;
select * from user_jobs where what like '%nvision_selectors%';
SELECT * FROM ps_nvs_treeslctlog WHERE selector_num =42;

column table_name format a18
column partition_name format a12
select table_name, partition_position, partition_name, num_rows, high_value
from user_tab_partitions where table_name = 'PSTREESELECT10' 
--AND partition_position<4
;
select selector_num, partition_name, timestamp from ps_nvs_treeslctlog where length = 10 and partition_name != ' ';
delete from ps_nvs_treeslctlog where selector_num = 42;
/*-------------------------------------------------------------------------------------------------------------------------------------*/

ttitle 'Tree Selector Log'
set pages 99 lines 200 termout off
column business_unit format a5 heading 'Business|Unit'
column process_instance heading 'Process|Instance'
column tree_name   format a18
column report_id   format a18
column layout_id   format a18
column module      format a12 heading 'Module'
column action      format a26 heading 'Action'
column appinfo_action  format a26 heading 'Action'
column client_info format a50
column selector_num format 999999 heading 'Selector|Number'
column timestamp format a30
with u as (
	SELECT  s.module, s.action
	, 	substr(regexp_substr(s.SQL_TEXT,'TREE_NAME=\''[^'']+'),12) tree_name
	, 	TO_NUMBER(substr(regexp_substr(s.SQL_TEXT,'DISTINCT ([[:digit:]])+'),10)) selector_num
	from	v$sql s
	where 	s.sql_text like 'INSERT%PSTREESELECT%SELECT%DISTINCT%'
	union
	select 	t.module, t.action
	, 	CAST(substr(regexp_substr(x.SQL_TEXT,'TREE_NAME=\''[^'']+'),12) AS VARCHAR2(30)) tree_name
	, 	TO_NUMBER(substr(regexp_substr(x.SQL_TEXT,'DISTINCT ([[:digit:]])+'),10)) selector_num
	from	dba_hist_sqltext x
	,	dba_hist_sqlstat t
	where	x.dbid = t.dbid
	and	x.sql_id = t.sql_id
	and	x.sql_text like 'INSERT%PSTREESELECT%SELECT%DISTINCT%'
), t as (
select /*+MATERIALIZE*/ * from u
), l as (
select	l.selector_num, l.process_instance, l.length, l.timestamp, l.module, l.appinfo_action, l.client_info, l.status_flag, l.num_rows
,	NVL(l.tree_name, 
	        (SELECT t.tree_name
		from	t
		where 	t.module = l.module
		and	t.action = l.appinfo_action
		and     t.selector_num = l.selector_num
		and	rownum=1)
	) tree_name
,	substr(regexp_substr(l.appinfo_action,':([[:alnum:]])+',1,2),2) business_unit
,	substr(regexp_substr(l.appinfo_action,':([[:alnum:]])+',1,1),2) report_id
FROM	ps_nvs_treeslctlog l
LEFT OUTER JOIN pstreeselctl s
ON s.selector_num = l.selector_num
)
select 	l.*
,	r.layout_id
from	l
	left outer join ps_nvs_report r
	on r.business_unit = l.business_unit
	and r.report_id = l.report_id
order by selector_num
/
TTITLE OFF
set termout on

---------------------------------------------------------------------------------------
--drop extended stats from partitioned tree selectors
---------------------------------------------------------------------------------------*
BEGIN
  FOR i IN(
    select e.* 
    from user_stat_extensions e
    , user_tables t
    where e.table_name like 'PSTREESELECT__'
    and t.table_name = e.table_name
    and t.partitioned = 'YES'
  ) LOOP
    dbms_stats.drop_extended_stats(user,i.table_name,i.extension);
  END LOOP;
END;
/



/*-------------------------------------------------------------------------------------
--CLEAR failed jobs
/*-------------------------------------------------------------------------------------

BEGIN
  FOR i IN(
    select * from user_jobs
    where FAILURES >0
    AND   (what like '%nvision_selectors%'
    OR     what like 'gfcpsstats11.set_record_prefs(%);'
    oR     what LIKE 'dbms_stats.gather_table_stats(%PSTREESELECT%SYS_P%force%TRUE);')
  ) loop
    dbms_job.remove(i.job);
    commit;
  end loop;
END;
/

/*-------------------------------------------------------------------------------------
--tree structures
/*-------------------------------------------------------------------------------------
ttitle 'Tree Structures'
select  c.*
,	s.tree_strct_id
,	s.dtl_recname
,	s.dtl_fieldname
from	pstreeselctl c
,	pstreedefn d
,	pstreestrct s
where	c.setid = d.setid
and	c.setcntrlvalue = d.setcntrlvalue
and	c.tree_name = d.tree_name
and	c.effdt = d.effdt
and	s.tree_strct_id = d.tree_strct_id
--and d.tree_name IN('INTNL_MCC_RPTG1','INTNL_MCC_RPTG2','INTNL_MCC_RPTG3','INTNL_MCC_RPTG4','GAAP_ACCOUNT','INTNL_GAAP_CONSOL','FUNCTION','MGMT_COMBO_CODE') 
--and  s.dtl_fieldname IN('CHARTFIELD1','CHARTFIELD2','ACCOUNT')
--and d.tree_strct_id IN('CONSOLIDATION')
order by 1,2,3
/
ttitle off



---------------------------------------------------------------------------------------
--force stats jobs to run or remove if partition removed
---------------------------------------------------------------------------------------*/
column tabname format a18
column partname format a12
set serveroutput on 
begin
  for i in (
with x as (
select 	j.*
,	substr(regexp_substr(what,'\''[^\'']+',30,1),2) tabname
,	substr(regexp_substr(what,'\''[^\'']+',30,3),2) partname
from	user_jobs j
where	what like 'dbms_stats.gather_table_stats(%);'
)
select 	x.*, p.tablespace_name
from	x
	left outer join user_tab_partitions p
	on p.table_name = x.tabname 
	and	p.partition_name = x.partname
and 	rownum <= 10
  ) LOOP
    IF i.tablespace_name IS NOT NULL THEN
      dbms_output.put_line(i.job||':'||i.what);
      dbms_job.run(i.job);
    ELSE
      dbms_output.put_line('Remove job '||i.job||':'||i.what);
      dbms_job.remove(i.job);
    END IF;
  END LOOP;
END;
/



--------------------------------------------------------------------------------
--remove erroneous static selectors
--------------------------------------------------------------------------------
set serveroutput on 
BEGIN
  FOR i IN (
select	c.*
from	pstreeselctl c
,	ps_nvs_treeslctlog l
,	pstreedefn d
where	c.setid = d.setid
and	c.setcntrlvalue = d.setcntrlvalue
and	c.tree_name = d.tree_name
and	c.effdt = d.effdt
and	l.selector_num = c.selector_num
and	d.tree_name = l.tree_name
and	d.tree_acc_selector = 'D'
order by c.selector_num
  ) LOOP
    UPDATE ps_nvs_treeslctlog 
    SET    status_flag = 'D'
    WHERE  selector_num = i.selector_Num
    AND    status_flag = 'S';
    DELETE FROM pstreeselctl 
    WHERE  selector_num = i.selector_Num
    AND    tree_name = i.tree_name;
  END LOOP;
END;
/


/*-------------------------------------------------------------------------------------
-- purge process
/*-------------------------------------------------------------------------------------*/
set serveroutput on 
exec sysadm.xx_nvision_selectors.purge;
select ownerid, status_flag, count(*)
from ps_nvs_treeslctlog
group by ownerid, status_flag
order by 1,2
/

ttitle 'Allocated Dynamic Selectors'
select *
from ps_nvs_treeslctlog
where NOT status_flag IN('S','X')
/
ttitle off
------------------------------------------------------------------------------------------------------------------------------------*/
spool off

	