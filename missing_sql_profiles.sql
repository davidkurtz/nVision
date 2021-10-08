REM missing_sql_profiles.sql
spool missing_sql_profiles
column force_matching_signature format 99999999999999999999
clear screen
with s as (
select s.force_matching_signature, sum(s.elapsed_time_delta)/1e6 elapsed_time, sum(s.executions_delta) num_execs
, min(x.begin_interval_time) begin_interval_time
from dbA_hist_sqlstat s,dba_hist_snapshot x
where x.dbid = s.dbid
and x.instance_number = s.instance_number
and x.snap_id = s.snap_id
group by s.force_matching_signature
)
select a.force_matching_signature, a.sql_profile_name, s.begin_interval_time, s.elapsed_time, s.num_execs, p.created
from dmk_fms_profiles a
 LEFT OUTER JOIN dba_sql_profiles p
 ON p.signature = a.force_matching_signature
 LEFT OUTER JOIN s
 ON s.force_matching_signature = a.force_matching_signature
WHERE p.created IS NULL
AND s.elapsed_time is not null
AND a.delete_profile = 'N'
/
spool off

