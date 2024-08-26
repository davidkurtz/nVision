REM treeselector_statprefs.sql
clear screen
spool treeselector_statprefs

begin
  for i in (
    SELECT t.owner, t.table_name
    ,      SUBSTR(t.table_name,-2) length
    FROM   all_users u
    ,      all_tables t
    WHERE  (u.username = 'SYSADM' OR u.username like 'NVEXEC%')
    AND    t.owner = u.username
    AND    t.table_name like 'PSTREESELECT__'
  ) LOOP
    dbms_stats.set_table_prefs(i.owner,i.table_name,'INCREMENTAL','FALSE');
    dbms_stats.set_table_prefs(i.owner,i.table_name,'CASCADE','TRUE');
    dbms_stats.set_table_prefs(i.owner,i.table_name,'STALE_PERCENT','1');
    dbms_stats.set_table_prefs(i.owner,i.table_name,'DEGREE','1'); --added 24.1.2023
    dbms_stats.set_table_prefs(i.owner,i.table_name,'GRANULARITY','PARTITION');
    dbms_stats.set_table_prefs(i.owner,i.table_name,'NO_INVALIDATE','TRUE'); --added 24.1.2023
    dbms_stats.set_table_prefs(i.owner,i.table_name,'METHOD_OPT'
                              ,'FOR ALL COLUMNS SIZE 1' --22.2.2023 no histograms
                              --FOR COLUMNS SIZE 1 (SELECTOR_NUM, TREE_NODE_NUM) (SELECTOR_NUM, RANGE_FROM_'||i.length||') (SELECTOR_NUM, RANGE_TO_'||i.length||')' --23.2.2023 no extended statistics
                              );
    dbms_stats.lock_table_stats(i.owner,i.table_name);
    --dbms_stats.gather_table_stats(i.owner,i.table_name,force=>TRUE);
  END LOOP;
end;
/

--Remove all extended statistics – added 23.2.2023
begin
  for i IN (
    select * 
    from all_stat_extensions e
    WHERE (e.owner = 'SYSADM' OR e.owner like 'NVEXEC%')
    AND   e.table_name like 'PSTREESELECT__'
  ) LOOP
    dbms_stats.drop_extended_stats(i.owner, i.table_name, i.extension);
  END LOOP;
end;
/


set lines 200 pages 99
break on owner skip 1 on table_name skip 1
column owner format a10
column partition_name format a25
column preference_value format a100 word_wrapped on
column table_name format a18
select *
from all_tab_stat_prefs
where table_name like 'PSTREESELECT__'
and (owner = 'SYSADM' or owner like 'NVEXEC%')
order by NULLIF(owner,'SYSADM') nulls first, table_name, preference_name
/

select owner, table_name, partition_position, partition_name, num_rows, blocks, last_analyzed, stattype_locked
from all_tab_statistics
where table_name like 'PSTREESELECT__'
and (owner = 'SYSADM' or owner like 'NVEXEC%')
and (num_rows != 0 OR num_rows IS NULL)
--and partitioned = 'YES'
order by NULLIF(owner,'SYSADM') nulls first, table_name, partition_position nulls first
fetch first 5000 rows only
/

REM report on any extended statistics
with x as (
select extension_name, count(*) num_extensions
, listagg(DISTINCT table_name,', ' ON OVERFLOW TRUNCATE WITH COUNT) within group (order by table_name) table_names
, listagg(DISTINCT owner,', ' ON OVERFLOW TRUNCATE WITH COUNT) within group (order by owner) owners
from all_stat_extensions
where table_name like 'PSTREESELECT__'
and (owner = 'SYSADM' or owner like 'NVEXEC%')
group by extension_name
)
select (select extension FROM all_stat_extensions e WHERE e.extension_name = x.extension_name and rownum = 1) extension
, x.*
from x
order by owners, table_names
/

spool off
break on report