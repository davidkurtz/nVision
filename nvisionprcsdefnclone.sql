REM nvisionprcsdefnclone.sql
spool nvisionprcsdefnclone

REM add process categories
insert into PS_PRCS_CAT_TBL (prcscategory, descr) values ('nVisionExcel','nVision Excel');
insert into PS_PRCS_CAT_TBL (prcscategory, descr) values ('nVisionOpenXML','nVision OpenXML');

REM increment version numbers prior to updating version numbered objects
update pslock    set version = version + 1 where objecttypename IN('SYS','PPC')
/
update psversion set version = version + 1 where objecttypename IN('SYS','PPC')
/

REM update process definitions
update ps_prcsdefn
set version = (SELECT VERSION from psversion where objecttypename = 'PPC')
,   PRCSCATEGORY = 'nVisionOpenXML' 
where prcstype like 'nVision%'
and prcsname IN('NVSRUN','RPTBOOK')
/
insert into ps_prcsdefn
(PRCSTYPE, PRCSNAME, VERSION, PARMLIST, PARMLISTTYPE, CMDLINE, CMDLINETYPE, WORKINGDIR, WORKINGDIRTYPE, OUTDESTTYPE, OUTDEST, OUTDESTSRC, SQRRTFLAG, LOGRQST, APIAWARE, PRCSPRIORITY, RUNLOCATION, SERVERNAME, MVSSHELLID, MSGLOGTBL, RQSTTBL, RECURNAME, DESCR, LASTUPDDTTM, LASTUPDOPRID, RECVRYPRCSTYPE, RECVRYPRCSNAME, RETENTIONDAYS, PT_RETENTIONDAYS, OUTDESTFORMAT, PSRF_FOLDER_NAME, RESTARTENABLED, RETRYCOUNT, TIMEOUTMINUTES, MAXCONCURRENT, PRCSCATEGORY, FILEDEPEND, PRCSFILENAME, TIMEOUTMAXMINS, PRCSSHOWURL, PT_PRCS_EN_GEN_RUN, PT_PRCS_RUNCNTLSEC, PRCSREADONLY, TIMESTENMODE, PTSCHDL_NAME, EMAILID, PTPRCSIBMSGSLOGLEV, DESCRLONG)
select PRCSTYPE, PRCSNAME||'E'
, (SELECT VERSION from psversion where objecttypename = 'PPC')
, PARMLIST, PARMLISTTYPE, CMDLINE, CMDLINETYPE, WORKINGDIR, WORKINGDIRTYPE, OUTDESTTYPE, OUTDEST, OUTDESTSRC, SQRRTFLAG, LOGRQST, APIAWARE, PRCSPRIORITY, RUNLOCATION, SERVERNAME, MVSSHELLID, MSGLOGTBL, RQSTTBL, RECURNAME
, DESCR||' (Excel)'
, LASTUPDDTTM, LASTUPDOPRID, RECVRYPRCSTYPE, RECVRYPRCSNAME, RETENTIONDAYS, PT_RETENTIONDAYS, OUTDESTFORMAT, PSRF_FOLDER_NAME, RESTARTENABLED, RETRYCOUNT, TIMEOUTMINUTES, MAXCONCURRENT
, 'nVisionOpenXML' PRCSCATEGORY
, FILEDEPEND, PRCSFILENAME, TIMEOUTMAXMINS, PRCSSHOWURL, PT_PRCS_EN_GEN_RUN, PT_PRCS_RUNCNTLSEC, PRCSREADONLY, TIMESTENMODE, PTSCHDL_NAME, EMAILID, PTPRCSIBMSGSLOGLEV, DESCRLONG
from ps_prcsdefn
where prcstype like 'nVision%'
and prcsname IN('NVSRUN','RPTBOOK')
/
insert into ps_prcsdefngrp
(PRCSTYPE, PRCSNAME, prcsgrp)
SELECT PRCSTYPE, PRCSNAME||'E', prcsgrp
from ps_prcsdefngrp
where prcstype like 'nVision%'
and prcsname IN('NVSRUN','RPTBOOK')
/
insert into ps_prcsdefnpnl
(PRCSTYPE, PRCSNAME, pnlgrpname)
SELECT PRCSTYPE, PRCSNAME||'E', pnlgrpname
from ps_prcsdefnpnl
where prcstype like 'nVision%'
and prcsname IN('NVSRUN','RPTBOOK')
/
insert into ps_prcsdefnxfer
(PRCSTYPE, PRCSNAME, XFERCODE, MENUNAME, BARNAME, ITEMNUM, PNLNAME, MENUACTION)
SELECT PRCSTYPE, PRCSNAME||'E', XFERCODE, MENUNAME, BARNAME, ITEMNUM, PNLNAME, MENUACTION
from ps_prcsdefnxfer
where prcstype like 'nVision%'
and prcsname IN('NVSRUN','RPTBOOK')
/

REM create duplicate process schedulers (based on PSNT)

INSERT INTO ps_serverdefn
(SERVERNAME ,VERSION ,DESCR ,SLEEPTIME ,HEARTBEAT ,MAXAPIUNAWARE ,MAXAPIAWARE ,OPSYS ,DISTNODENAME ,TRANSFERLOGFILES ,TRANSFERMAXRETRY ,TRANSFERINTERVAL ,SRVRLOADBALOPTN ,REDISTWRKOPTION ,DAEMONGROUP ,DAEMONSLEEPTIME ,DAEMONENABLED ,DAEMONCYCLECNT ,LASTUPDDTTM ,LASTUPDOPRID ,DAEMONPRCSINST ,MAXCPU ,MINMEM ,PRCSNOTIFYFREQ)
with n as (
select level n from dual connect by level <= 2 --number of process schedulers
), x as (
select c.*, DECODE(prcscategory,'nVisionExcel','E','nVisionOpenXML','X') type
from PS_PRCS_CAT_TBL c
where prcscategory like 'nVision%'
)
SELECT SERVERNAME||'_'||x.type||n.n
, (SELECT VERSION from psversion where objecttypename = 'PPC')
,SUBSTR(s.DESCR||' ('||x.descr||')',1,30),SLEEPTIME ,HEARTBEAT ,MAXAPIUNAWARE 
,CASE WHEN x.type = 'E' THEN 1 ELSE MAXAPIAWARE END
,CASE WHEN x.type = 'E' THEN '2' ELSE OPSYS END
,DISTNODENAME ,TRANSFERLOGFILES ,TRANSFERMAXRETRY ,TRANSFERINTERVAL 
,1 SRVRLOADBALOPTN /*enable load balancing*/
,1 REDISTWRKOPTION /*same OS*/
,DAEMONGROUP ,DAEMONSLEEPTIME ,DAEMONENABLED ,DAEMONCYCLECNT ,systimestamp ,'GFC' ,DAEMONPRCSINST ,MAXCPU ,MINMEM ,PRCSNOTIFYFREQ
from ps_serverdefn s, x, n
where servername = 'PSNT'
/

INSERT INTO ps_serverclass
(SERVERNAME ,OPSYS ,PRCSTYPE ,PRCSPRIORITY ,MAXCONCURRENT)
with x as (
select c.*, DECODE(prcscategory,'nVisionExcel','E','nVisionOpenXML','X') type
from PS_PRCS_CAT_TBL c
where prcscategory like 'nVision%'
)
select s.SERVERNAME ,c.OPSYS ,c.PRCSTYPE ,c.PRCSPRIORITY 
,CASE WHEN x.type = 'E' THEN 1 ELSE GREATEST(s.maxapiaware,c.MAXCONCURRENT) END
from ps_serverdefn s, ps_serverclass c, x
where c.servername = 'PSNT'
and s.servername LIKE c.SERVERNAME||'_'||x.type||'%'
and (c.prcstype like 'nVision%'
or   c.prcstype = 'PSJob')
/


--create every category on every server - all missing categories have 0 concurrency so can't run
insert into ps_servercategory
(SERVERNAME ,PRCSCATEGORY ,PRCSPRIORITY ,MAXCONCURRENT)
select s.SERVERNAME ,c.PRCSCATEGORY ,5 PRCSPRIORITY ,0 MAXAPIAWARE
from ps_serverdefn s
,    PS_PRCS_CAT_TBL c
where not exists(
   select 'x' from ps_servercategory sc
   where  sc.servername = s.servername
   and    sc.prcscategory = c.prcscategory)
--and c.prcscategory like 'nVision%'
/

--then fix the concurrencies
update ps_servercategory
set    maxconcurrent = CASE WHEN servername like 'PSNT_E%' 
                            THEN 1 ELSE 0 END
where  prcscategory = 'nVisionExcel'
/

update ps_servercategory c
set    maxconcurrent = CASE WHEN servername like 'PSNT_X%' 
                            THEN (SELECT maxapiaware FROM ps_serverdefn s WHERE s.servername = c.servername) 
                            ELSE 0 END
where  prcscategory = 'nVisionOpenXML'
/

commit
/


spool off