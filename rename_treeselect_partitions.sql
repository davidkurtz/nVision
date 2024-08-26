REM rename_treeselect_partitions.sql
set serveroutput on 
ALTER SESSION SET CURRENT_SCHEMA=SYSADM;
exec xx_nvision_selectors.set_debug_level(0);
exec xx_nvision_selectors.create_interval_parts('NVEXEC01',05,42,0);
exec xx_nvision_selectors.rename_partitions('NVEXEC01',05,42);

DECLARE
  l_cmd            VARCHAR2(1000 CHAR);
  l_job_no         NUMBER;
BEGIN
  FOR i IN (
    select DISTINCT t.owner, t.table_name --, p.partition_name
    FROM dba_part_tables t, dba_tab_partitions p
    WHERE t.table_name like 'PSTREESELECT__'
    --AND t.table_name NOT IN('PSTREESELECT05','PSTREESELECT10')
    AND t.partitioning_type = 'RANGE'
    AND t.interval = '1'
    AND p.table_owner = t.owner
    and p.table_name = t.table_name
    and not p.partition_name LIKE p.table_name||'%'
    ORDER BY 1
  ) LOOP
    l_cmd := 'xx_nvision_selectors.rename_partitions('''||i.owner||''','||SUBSTR(i.table_name,-2)||');';
    dbms_job.submit(l_job_no,l_cmd);
    dbms_output.put_line('job '||l_job_no||':'||l_cmd);
  END LOOP;
  COMMIT;
END;
/

set pages 9999
column owner format a10
column table_name format a18
column def_parameters format a30
column interval format a10
column interval_subpartition format a10
column partition_pattern format a20
column min(partition_name) format a22
column max(partition_name) format a22
column index_owner format a10
column index_name format a18
column partition_name format a22
column high_value format a20
column parameters format a20

select * from dba_ind_partitions
where index_name like 'PS_PSTREESELECT10'
and index_owner = 'SYSADM'
order by partition_position
fetch first 50 rows only;
;

select * from dba_part_indexes
where index_name like 'PS_PSTREESELECT10'
fetch first 50 rows only;

REM running jobs
select * from dbA_scheduler_running_jobs where owner = 'SYSADM';
--exec DBMS_SCHEDULER.stop_JOB (job_name => 'DBMS_JOB$_479910');
--exec DBMS_SCHEDULER.drop_JOB (job_name => 'DBMS_JOB$_473670');

select * from dbA_scheduler_job_log
where owner = 'SYSADM'
order by log_date desc
fetch first 50 rows only;

select * from dbA_scheduler_job_run_details
where owner = 'SYSADM'
order by log_date desc
fetch first 50 rows only;

with x as (
select selector_num, length, REGEXP_REPLACE(partition_name,'[0-9]{3,}','nnn',1,0) partition_pattern from ps_nvs_treeslctlog
) select length, partition_pattern, count(*), max(selector_num) from x group by length, partition_pattern order by 1,2;

REM table parititon name summary
with x as (
select table_owner, table_name, partition_name, REGEXP_REPLACE(partition_name,'[0-9]{3,}','nnn',1,0) partition_pattern
from all_tab_partitions where table_name like 'PSTREESELECT__' and (table_owner = 'SYSADM' OR table_owner like 'NVEXEC__')
) select table_owner, table_name, partition_pattern, count(*), min(partition_name), max(partition_name) from x 
group by table_owner, table_name, partition_pattern
having count(*)>1 OR MIN(partition_name) like 'SYS%'
ORDER BY 2,1,3;


REM index partitions with system generated names
with x as (
select index_owner, index_name, partition_name, REGEXP_REPLACE(partition_name,'[0-9]{3,}','nnn',1,0) partition_pattern
from all_ind_partitions where index_name like 'PS_PSTREESELECT__' and (index_owner = 'SYSADM' OR index_owner like 'NVEXEC__')
and partition_name like 'SYS_P%'
) select index_owner, index_name, partition_pattern, count(*), min(partition_name), max(partition_name) from x 
group by index_owner, index_name, partition_pattern
ORDER BY 4 desc,1,2;


--exec dbms_stats.gather_table_stats('SYSADM','PSTREESELECT10','NO_SUCH_PARTITION',force=>TRUE);