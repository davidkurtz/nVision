REM reset_selector_num.sql

DECLARE
  l_cmd            VARCHAR2(1000 CHAR);
  l_job_no         NUMBER;
BEGIN
  l_cmd := 'xx_nvision_selectors.reset_selector_num;';
  dbms_job.submit(l_job_no,l_cmd);
  dbms_output.put_line('job '||l_job_no||':'||l_cmd);
  COMMIT;
END;
/

REM running jobs
select * from dbA_scheduler_running_jobs where owner = 'SYSADM';
--exec DBMS_SCHEDULER.stop_JOB (job_name => 'DBMS_JOB$_479910');
--exec DBMS_SCHEDULER.drop_JOB (job_name => 'DBMS_JOB$_473670');

REM completed jobs
select * from dbA_scheduler_job_log
where owner = 'SYSADM'
order by log_date desc
fetch first 5 rows only;

select * from dbA_scheduler_job_run_details
where owner = 'SYSADM'
order by log_date desc
fetch first 5 rows only;

column selector_num heading 'Sel|Num' format 9999
select x00.selector_num
,      x01.selector_num
,      x02.selector_num
,      x03.selector_num
,      x04.selector_num
,      x05.selector_num
,      x06.selector_num
,      x07.selector_num
,      x08.selector_num
,      x09.selector_num
,      x10.selector_num
,      x11.selector_num
,      x12.selector_num
,      x13.selector_num
,      x14.selector_num
,      x15.selector_num
,      x16.selector_num
,      x17.selector_num
,      x18.selector_num
,      x19.selector_num
,      x20.selector_num
,      x21.selector_num
,      x22.selector_num
,      x23.selector_num
,      x24.selector_num
,      x25.selector_num
,      x26.selector_num
,      x27.selector_num
,      x28.selector_num
,      x29.selector_num
,      x30.selector_num
from   sysadm.pstreeselnum x00
,      nvexec01.pstreeselnum x01
,      nvexec02.pstreeselnum x02
,      nvexec03.pstreeselnum x03
,      nvexec04.pstreeselnum x04
,      nvexec05.pstreeselnum x05
,      nvexec06.pstreeselnum x06
,      nvexec07.pstreeselnum x07
,      nvexec08.pstreeselnum x08
,      nvexec09.pstreeselnum x09
,      nvexec10.pstreeselnum x10
,      nvexec11.pstreeselnum x11
,      nvexec12.pstreeselnum x12
,      nvexec13.pstreeselnum x13
,      nvexec14.pstreeselnum x14
,      nvexec15.pstreeselnum x15
,      nvexec16.pstreeselnum x16
,      nvexec17.pstreeselnum x17
,      nvexec18.pstreeselnum x18
,      nvexec19.pstreeselnum x19
,      nvexec20.pstreeselnum x20
,      nvexec21.pstreeselnum x21
,      nvexec22.pstreeselnum x22
,      nvexec23.pstreeselnum x23
,      nvexec24.pstreeselnum x24
,      nvexec25.pstreeselnum x25
,      nvexec26.pstreeselnum x26
,      nvexec27.pstreeselnum x27
,      nvexec28.pstreeselnum x28
,      nvexec29.pstreeselnum x29
,      nvexec30.pstreeselnum x30
/
