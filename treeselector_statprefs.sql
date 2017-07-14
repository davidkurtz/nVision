REM treeselector_statprefs.sql
spool treeselector_statprefs

begin
  for i in (
    SELECT t.owner, t.table_name
    ,      SUBSTR(t.table_name,-2) length
    FROM   all_users u
    ,      all_tables t
    WHERE  u.username like 'NVEXEC%'
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
  END LOOP;
end;
/


set lines 200
break on owner skip 1 on table_name skip 1
column preference_value format a100 word_wrapped on
select *
from all_tab_stat_prefs
where table_name like 'PSTREESELECT__'
order by 1,2,3
/

spool off
break on report