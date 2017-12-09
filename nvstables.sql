REM nvstables.sql
spool nvstables
alter session set current_schema=SYSADM;

drop table nvstables purge
/
create table nvstables
(object_type varchar2(30)
,object_name varchar2(30)
,sql_opname  varchar2(30)
)
/
create unique index nvstables on nvstables (object_Type,object_name,sql_opname)
/
truncate table nvstables
/
insert into nvstables
with x as (
 	SELECT /*+MATERIALIZE*/ DISTINCT h.sql_plan_hash_value
        , CASE 
            WHEN sql_opname IS NULL THEN 'SELECT'
            WHEN sql_opname = 'PL/SQL EXECUTE' THEN 'SELECT'
            ELSE h.sql_opname
        END as sql_opname
	FROM dbA_hist_active_Sess_history h
	where   1=1
	and     h.program like 'PSNVS%'
	and     h.sql_plan_hash_value > 0
)
select  distinct p.object_type, p.object_name, sql_opname
from    dba_hist_sql_plan p
,	x
where 	p.plan_hash_value = x.sql_plan_hash_value 
and	p.object_owner = 'SYSADM'
and	p.object_name IS NOT NULL
order by 1,2
/

begin 
  FOR i IN(
     select DISTINCT i.table_name, t.object_type, i.index_name, t.sql_opname
     from   nvstables t
     ,	    user_indexes i
     where i.index_name = t.object_name
     and   (t.object_type LIKE 'INDEX%' OR t.object_Type is null)
  ) LOOP
    BEGIN 
      UPDATE nvstables t
      SET    object_type = 'TABLE'
      ,      object_name = i.table_name
      WHERE  object_type = i.object_type
      AND    object_name = i.index_name
      AND    sql_opname = i.sql_opname;
    EXCEPTION WHEN dup_val_on_index THEN
      DELETE FROM nvstables t
      WHERE  object_type = i.object_type
      AND    object_name = i.index_name
      AND    sql_opname = i.sql_opname;
    END;
  END LOOP;
END;
/

begin 
  FOR i IN(
     select n.*
     from   nvstables n
     ,	    user_tables t
     where  t.table_name = n.object_name
     and    n.object_type IS NULL
  ) LOOP
    BEGIN 
      UPDATE nvstables t
      SET    object_type = 'TABLE'
      WHERE  object_type IS NULL
      AND    object_name = i.object_name
      AND    sql_opname = i.sql_opname;
    EXCEPTION WHEN dup_val_on_index THEN
      DELETE FROM nvstables t
      WHERE  object_type IS NULL
      AND    object_name = i.object_name
      AND    sql_opname = i.sql_opname;
    END;
  END LOOP;
END;
/

begin 
  FOR i IN(
    SELECT n.*, DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) sqltablename
    FROM   psrecdefn r
    ,	   nvstables n
    WHERE  n.object_name like 'PS_'||r.recname
    AND	   n.object_type like 'INDEX%'
  ) LOOP
    BEGIN 
      UPDATE nvstables t
      SET    object_type = 'TABLE'
      ,      object_name = i.sqltablename
      WHERE  object_type = i.object_type
      AND    object_name = i.object_name
      AND    sql_opname = i.sql_opname;
    EXCEPTION WHEN dup_val_on_index THEN
      DELETE FROM nvstables t
      WHERE  object_type = i.object_type
      AND    object_name = i.object_name
      AND    sql_opname = i.sql_opname;
    END;
  END LOOP;
END;
/

DELETE FROM nvstables
WHERE object_name IN('PSTREESELECT05','PSTREESELECT06','PSTREESELECT08','PSTREESELECT10','PSTREESELCTL');
DELETE FROM nvstables
WHERE object_name IN(
 	select table_name
	from all_tables
	where owner like 'NVEXEC%');

INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(nvstables,nvstableS)*/ INTO nvstables
SELECT 	'VIEW', replace(object_name,'MV','VW'), 'SELECT'
FROM	nvstables n
,	psrecdefn r
WHERE 	n.object_type = 'MAT_VIEW REWRITE'
and	object_name = DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename)
and	r.recname = SUBSTR(object_name,4)
and	r.rectype = 0
/

INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(nvstables,nvstableS)*/ INTO nvstables
SELECT 	'VIEW', DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename), 'SELECT'
FROM	psrecdefn r
WHERE 	r.rectype = 1
AND	r.recname like '<summary ledger reporting views>'
/

INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(nvstables,nvstables)*/ INTO nvstables
SELECT 	'TABLE', DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename), 'SELECT'
FROM	psrecdefn r
WHERE 	r.rectype = 0
AND	r.recname like '<summary ledgers>'
/

COMMIT;

set pages 99 lines 200
select * from nvstables
order by 1,2
/
spool off

break on report
set head off pause off timi off feedback off trimspool on pages 0 lines 200 echo off
spool nvsprivs.sql
with u as (
SELECT 	username
FROM	all_users
WHERE   username like 'NVEXEC%'
)
select 	'GRANT SELECT ON sysadm.'||n.object_name||' TO '||u.username||';'
from	nvstables n
,	u
where	n.sql_opname = 'SELECT'
union all
select 	DISTINCT 'GRANT SELECT, INSERT, UPDATE, DELETE ON sysadm.'||n.object_name||' TO '||u.username||';'
from	nvstables n
,	u
where	n.sql_opname != 'SELECT'
and	n.object_type = 'TABLE'
union all
select 	DISTINCT 'CREATE SYNONYM '||u.username||'.'||n.object_name||' for sysadm.'||n.object_name||';'
from	nvstables n
,	u
WHERE NOT EXISTS(
	select /*+UNNEST*/ 'x'
	from all_synonyms s
	where s.table_name = n.object_name
        and   s.table_owner = 'SYSADM'
        and   s.owner = 'PUBLIC'
        )
/
spool off
set head on pages 99 echo on feedback on