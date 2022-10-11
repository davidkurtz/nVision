REM treeselector_statprefs.sql
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
    dbms_stats.set_table_prefs(i.owner,i.table_name,'GRANULARITY','PARTITION');
    dbms_stats.set_table_prefs(i.owner,i.table_name,'METHOD_OPT'
                              ,'FOR ALL COLUMNS SIZE AUTO FOR COLUMNS SIZE 254 SELECTOR_NUM TREE_NODE_NUM FOR COLUMNS SIZE 1 RANGE_FROM_'||i.length||
                               ' RANGE_TO_'||i.length||' (SELECTOR_NUM, TREE_NODE_NUM) (SELECTOR_NUM, RANGE_FROM_'||i.length||') (SELECTOR_NUM, RANGE_TO_'||i.length||')'
                              );
    --dbms_stats.gather_table_stats(i.owner,i.table_name);
  END LOOP;
end;
/


set lines 200 pages 99
break on owner skip 1 on table_name skip 1
column owner format a10
column partition_name format a20
column preference_value format a100 word_wrapped on
column table_name format a18
select *
from all_tab_stat_prefs
where table_name like 'PSTREESELECT__'
and (owner = 'SYSADM' or owner like 'NVEXEC%')
order by 1,2,3
/

select owner, table_name, partition_position, partition_name, num_rows, blocks, last_analyzed
from all_tab_statistics
where table_name like 'PSTREESELECT__'
and (owner = 'SYSADM' or owner like 'NVEXEC%')
--and partitioned = 'YES'
order by owner, table_name, partition_position nulls first
/

spool off
break on report