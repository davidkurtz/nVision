REM nvision_prcscategory.sql
REM (c)Go-Faster Consultancy 2021
REM update categories with concurrencies
REM assumes PSNT_E runs Excel, PSNT_X runs OpenXML

spool nvision_prcscategory

break on prcsname skip 1
set pages 99 lines 180 trimspool on

select  p.prcsname, c.*
from    ps_prcsdefn p
,       ps_servercategory c
where   p.prcstype like 'nVision-ReportBook' 
and     p.prcsname IN('RPTBOOKE','RPTBOOK')
and     c.prcscategory = p.prcscategory
and     c.maxconcurrent>0
order by 1,2
/

update  ps_servercategory c
set     maxconcurrent = 0
where   prcscategory = 'nVisionExcel'
and     not servername like 'PSNT_E%'
and     maxconcurrent != 0
/

update  ps_servercategory c
set     maxconcurrent = 1
where   prcscategory = 'nVisionExcel'
and     servername like 'PSNT_E%'
and     maxconcurrent != 1
/

update  ps_servercategory c
set     maxconcurrent = 0
where   prcscategory = 'nVisionOpenXML'
and     not servername like 'PSNT_X%'
and     maxconcurrent != 0
/

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


