REM nvision_prcscategory.sql
REM (c)Go-Faster Consultancy 2021
REM update categories with concurrencies
REM assumes PSNT_E runs Excel, PSNT_X runs OpenXML

spool nvision_prcscategory

break on prcsname skip 1
set pages 99 lines 180 trimspool on

select  c.*
from    ps_servercategory c
where   c.maxconcurrent>0
and     servername like 'PSNT%'
and     prcscategory like 'nVision%'
order by 1,2
/

REM Excel nVision should not run anywhere other than the PSNT_E% process schedulers
REM so concurrency for the nVisionExcel category is set to 0 on all other process schedulers.
update  ps_servercategory c
set     maxconcurrent = 0
where   prcscategory = 'nVisionExcel'
and     not servername like 'PSNT_E%'
and     maxconcurrent != 0
/

REM Only a single Excel nVision should not run concurrently on each PSNT_E% process scheduler
REM so concurrency for is set to 1 if not already set.
update  ps_servercategory c
set     maxconcurrent = 1
where   prcscategory = 'nVisionExcel'
and     servername like 'PSNT_E%'
and     maxconcurrent != 1
/

REM Open nVision should not run anywhere other than the PSNT_X% process schedulers
REM so concurrency for the nVisionOpenXML category is set to 0 on all other process schedulers.
update  ps_servercategory c
set     maxconcurrent = 0
where   prcscategory = 'nVisionOpenXML'
and     not servername like 'PSNT_X%'
and     maxconcurrent != 0
/

REM The concurrency of OpenXML nVisions is set to the maximum number of API aware process on the PSNT_X% process schedulers
update  ps_servercategory c
set     maxconcurrent = (SELECT maxapiaware FROM ps_serverdefn s
                         WHERE s.servername = c.servername)
where   prcscategory = 'nVisionOpenXML'
and     servername like 'PSNT_X%'
/

update pslock
set version = version + 1
where objecttypename IN('SYS','PPC')
/

update psversion
set version = version + 1
where objecttypename IN('SYS','PPC')
/

update ps_serverdefn
set version = (SELECT version from psversion where objecttypename = 'PPC')
, lastupddttm = systimestamp
;
update ps_prcsdefn
set version = (SELECT version from psversion where objecttypename = 'PPC')
, lastupddttm = systimestamp
where prcstype like 'nVision%'
/

commit
/

break on prcscategory skip 1
select * from ps_servercategory
where prcscategory IN('nVisionExcel','nVisionOpenXML')
order by 2,1
/

spool off


