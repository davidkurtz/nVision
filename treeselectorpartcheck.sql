REM treeselectorpartcheck.sql
set echo on pages 999 lines 200 trimspool on
clear screen
spool treeselectorpartcheck.lst
column partitioning_type heading 'Part|Type' format a10
column partition_count heading 'Partition|Count' format 9999999
column table_name format a15
column owner_list heading 'Owner List' format a120
column owner_list2 heading 'Owner List' format a80
column part_pos_list format a80 heading 'Part|Pos|List'
select table_name, partitioned
, listagg(owner,', ') within group (order by owner) owner_list
from all_tables
where (owner = 'SYSADM' or owner LIKE 'NVEXEC%')
and table_name like 'PSTREESELECT__'
group by table_name, partitioned
order  by 1,2
/

select table_name, partitioning_type, partition_count
, listagg(owner,', ') within group (order by owner) owner_list
from all_part_tables
where (owner = 'SYSADM' or owner LIKE 'NVEXEC%')
and table_name like 'PSTREESELECT__'
group by table_name, partitioning_type, partition_count
order  by 1,2
/

with x as (select table_owner, table_name
, listagg(partition_position,', ') within group (order by partition_position) part_pos_list
from all_tab_partitions
where (table_owner = 'SYSADM' or table_owner LIKE 'NVEXEC%')
and table_name like 'PSTREESELECT__'
group by table_owner, table_name)
select table_name, part_pos_list
, listagg(table_owner,', ') within group (order by table_owner) owner_list2
from x
group by table_name, part_pos_list
order  by 1,2
/

spool off
