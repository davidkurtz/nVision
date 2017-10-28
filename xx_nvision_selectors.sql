REM xx_nvision_selectors.sql
set echo on 
spool xx_nvision_selectors
rollback;
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--nvision selector population logging package
--------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE sysadm.xx_nvision_selectors AS
PROCEDURE logdel
(p_length           INTEGER
);
PROCEDURE logins 
(p_length           INTEGER
,p_ownerid         VARCHAR2
);
PROCEDURE purge_selectors
(p_process_instance INTEGER 
);
PROCEDURE purge
(p_selector_num     INTEGER DEFAULT NULL
);
PROCEDURE rowins
(p_selector_num     INTEGER 
,p_range_from       VARCHAR2 DEFAULT NULL
,p_range_to         VARCHAR2 DEFAULT NULL
);
PROCEDURE rowdel
(p_selector_num     INTEGER 
);
PROCEDURE reset_selector_num;
--exposed for testing only
--PROCEDURE gather_selector_stats
--(p_length INTEGER
--,p_selector_num INTEGER
--);
END xx_nvision_selectors;
/



--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--nvision selector population logging package body
--------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE BODY sysadm.xx_nvision_selectors AS
--------------------------------------------------------------------------------------------------------------
--Constants that should not be changed
--------------------------------------------------------------------------------------------------------------
k_module           CONSTANT VARCHAR2(64 CHAR) := $$PLSQL_UNIT; --name of package for instrumentation
k_dfps             CONSTANT VARCHAR2(20 CHAR) := 'YYYYMMDDHH24MISS'; --date format picture string
k_dfpsh            CONSTANT VARCHAR2(30 CHAR) := 'HH24:MI:SS DD.MM.YYYY'; --date format picture string for humans
k_purge_days       CONSTANT INTEGER := 31; --number of days after which to purge selector log
k_timeout_days     CONSTANT INTEGER := 2; --number of days after which nVision assumed to have terminated
k_stats_gather_job CONSTANT BOOLEAN := TRUE; --true to always submit stats gather job, otherwise only on static selectors
-------------------------------------------------------------------------------------------------------
--package global variables
-------------------------------------------------------------------------------------------------------
l_debug_level    INTEGER := 8;  -- variable to hold debug level of package
l_debug_indent   INTEGER := 0; -- indent level of procedure
-------------------------------------------------------------------------------------------------------
g_selector_num   INTEGER :=0;
g_counter        INTEGER :=0;
g_range_from_min VARCHAR2(30 CHAR);
g_range_from_max VARCHAR2(30 CHAR);
g_range_to_min   VARCHAR2(30 CHAR);
g_range_to_max   VARCHAR2(30 CHAR);

-------------------------------------------------------------------------------------------------------
-- to optionally print debug text during package run time
-------------------------------------------------------------------------------------------------------
PROCEDURE debug_msg(p_msg VARCHAR2 DEFAULT ''
                   ,p_debug_level INTEGER DEFAULT 5) IS
BEGIN
  IF p_debug_level <= l_debug_level AND p_msg IS NOT NULL THEN
    sys.dbms_output.put_line(TO_CHAR(SYSDATE,k_dfpsh)||':'||LPAD('.',l_debug_indent,'.')||'('||p_debug_level||')'||p_msg);
  END IF;
END debug_msg;

--------------------------------------------------------------------------------
--get partition name for selector
--------------------------------------------------------------------------------
FUNCTION get_partition_name(p_selector_num INTEGER) RETURN VARCHAR2 IS
  l_partition_name all_tab_partitions.partition_name%TYPE := '';
BEGIN
  debug_msg('get_partition_name('||p_selector_num||')');
  SELECT p.partition_name
  INTO   l_partition_name
  FROM   ps_nvs_treeslctlog l
  ,      all_tab_partitions p
  WHERE  l.selector_num = p_selector_num
  AND    p.partition_name = l.partition_name
  AND    p.table_name = 'PSTREESELECT'||LTRIM(TO_CHAR(l.length,'00'))
  AND    p.table_owner = l.ownerid
  ;

  debug_msg('Found partition:'||l_partition_name);
  RETURN(l_partition_name);
EXCEPTION
  WHEN no_data_found THEN 
    RETURN(l_partition_name);
END;
--------------------------------------------------------------------------------
--drop partition in a selector table
--------------------------------------------------------------------------------
PROCEDURE purge_selector
(p_length         INTEGER
,p_selector_num   INTEGER 
,p_ownerid        VARCHAR2
,p_partition_name all_tab_partitions.partition_name%TYPE DEFAULT ''
) AS 
  l_cmd            VARCHAR2(1000 CHAR);
  l_num_rows       INTEGER;
  l_table_name     psrecdefn.sqltablename%TYPE;
  l_partition_name all_tab_partitions.partition_name%TYPE;
  l_job_no         INTEGER;

  e_last_partition EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_last_partition, -14758); --ORA-14758: Last partition in the range section cannot be dropped

  e_partition_does_not_exist EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_partition_does_not_exist, -2149); --ORA-02149: Specified partition does not exist  

BEGIN
  l_table_name := 'PSTREESELECT'||LTRIM(TO_CHAR(p_length,'00'));

  IF p_partition_name IS NULL OR p_partition_name = ' ' THEN
    l_partition_name := get_partition_name(p_selector_num);
  ELSE
    l_partition_name := p_partition_name;
  END IF;

  IF l_partition_name IS NULL THEN
    l_cmd := 'DELETE FROM '||p_ownerid||'.'||l_table_name||' WHERE selector_num := 1'; 
    EXECUTE IMMEDIATE l_cmd USING p_selector_num;
  ELSE
    BEGIN
--    l_cmd := 'SELECT COUNT(*) FROM '||l_table_name||' PARTITION('||l_partition_name||')'; 
--    EXECUTE IMMEDIATE l_cmd INTO l_num_rows;
--    debug_msg(l_cmd||':'||l_num_rows);

      l_cmd := 'ALTER TABLE '||p_ownerid||'.'||l_table_name||' DROP PARTITION '||l_partition_name||' UPDATE INDEXES';
      debug_msg(l_cmd);
      EXECUTE IMMEDIATE l_cmd;

      UPDATE ps_nvs_treeslctlog l
      SET    partition_name = ' '
      ,      status_flag = 'X'
      WHERE  selector_num = p_selector_num
      AND    partition_name = l_partition_name
      RETURNING job_no INTO l_job_no;

      FOR i IN( /*remove any jobs related to this partition*/
        SELECT * 
        FROM   user_jobs
        WHERE  job = l_job_no
        OR     what LIKE 'dbms_stats.gather_table_stats(%'||l_table_name||'%'||l_partition_name||'%force%TRUE);'
      ) LOOP
        debug_msg('Remove job '||i.job||':'||i.what); 
        dbms_job.remove(i.job);
     end loop;

    EXCEPTION 
      WHEN e_partition_does_not_exist THEN
        debug_msg('Partition does not exist.  Marking as deleted in log'); 
        UPDATE ps_nvs_treeslctlog l
        SET    partition_name = ' '
        ,      status_flag = 'X'
        WHERE  selector_num = p_selector_num
        AND    partition_name = l_partition_name;
  
      WHEN e_last_partition THEN 
        debug_msg('Cannot drop last partition'); --do nothing leave log record
    END;
  END IF;
  COMMIT;
END purge_selector;

--------------------------------------------------------------------------------
--purge selectors for a Process Instance
--------------------------------------------------------------------------------
PROCEDURE purge_selectors
(p_process_instance INTEGER 
) AS 
  l_module       VARCHAR2(64 CHAR);
  l_action       VARCHAR2(64 CHAR);
BEGIN
  dbms_application_info.read_module(l_module, l_action);
  dbms_application_info.set_module(NVL(l_module,k_module), NVL(l_action,'purge_selectors('||p_process_instance||')'));

  FOR i IN (
    SELECT *
    FROM   ps_nvs_treeslctlog l
    WHERE  l.process_instance = p_process_instance
    OR    (    l.process_instance = 0
           AND l.timestamp < SYSDATE-k_timeout_days)
  ) LOOP
    purge_selector(i.length, i.selector_num, i.ownerid, i.partition_name);
  END LOOP;

  dbms_application_info.set_module(l_module, l_action);
END purge_selectors;
--------------------------------------------------------------------------------
--update stats directly
--evade job lag by updating known number of rows on table and index partition
--------------------------------------------------------------------------------
PROCEDURE set_selector_stats
(p_length         INTEGER
,p_ownerid        VARCHAR2
,p_partition_name VARCHAR2 DEFAULT NULL
,p_selector_num   INTEGER
,p_num_rows       INTEGER
) AS
  PRAGMA AUTONOMOUS_TRANSACTION; 
  k_rowsperblock CONSTANT INTEGER := 160; --assumed rows per block to calculate number of blocks
  k_avgrowlen    CONSTANT INTEGER := 30;  --assumed average row length
  l_table_name   psrecdefn.sqltablename%TYPE;
  l_srec         dbms_stats.statrec;

BEGIN
  debug_msg('set_selector_stats('||p_length||','||p_ownerid||'.'||p_partition_name||','||p_selector_num||','||p_num_rows||')');
  l_table_name     := 'PSTREESELECT'||LTRIM(TO_CHAR(p_length,'00'));

  IF p_num_rows IS NOT NULL THEN
    IF p_partition_name IS NOT NULL THEN
      debug_msg('set_table_stats('||p_ownerid||'.'||l_table_name||','||p_partition_name||','||p_num_rows||')',7);
      dbms_stats.set_table_stats(ownname=>p_ownerid
                                ,tabname=>l_table_name
                                ,partname=>p_partition_name
                                ,numrows=>p_num_rows
		                ,numblks=>GREATEST(5,CEIL(p_num_rows/k_rowsperblock)) /*arbitary estimate*/
                                ,avgrlen=>k_avgrowlen
                                ,force=>TRUE);
    END IF;
  END IF;

  FOR i IN ( /*look for locally partitioned indexes on selector table*/
    SELECT index_name 
    FROM   all_part_indexes 
    WHERE  owner = p_ownerid 
    AND    table_name = l_table_name 
    AND    locality = 'LOCAL'
  ) LOOP
    debug_msg('set_indexstats('||p_ownerid||'.'||i.index_name||','||p_partition_name||','||p_num_rows||','||p_selector_num||')',7);
    dbms_stats.set_index_stats(ownname=>p_ownerid /*num rows on index partition*/
                              ,indname=>i.index_name
                              ,partname=>p_partition_name
                              ,numrows=>p_num_rows
                              ,numdist=>p_num_rows /*it is not generally true but it is for selector indexes*/
		              ,numlblks=>CEIL(p_num_rows/k_rowsperblock) /*arbitary estimate*/
                              ,avglblk=>1 /*leaf blocks per key*/
                              ,avgdblk=>1 /*data blocks per key*/
                              ,clstfct=>p_num_rows /*higher than reality, probably around 80%*/
                              ,indlevel=>1 /*blevel*/
                              ,force=>TRUE);
  END LOOP;

  l_srec.epc     := 2;    /*two endpoints*/
  l_srec.eavs    := 0;
  l_srec.rpcnts  := NULL;
  l_srec.bkvals  := dbms_stats.numarray(0,p_num_rows); /*one bucket*/
  dbms_stats.prepare_column_values(l_srec,dbms_stats.numarray(p_selector_num,p_selector_num));
  dbms_stats.set_column_stats(ownname=>p_ownerid /*set min/max value on selector num*/
                             ,tabname=>l_table_name
                             ,colname=>'SELECTOR_NUM'
                             ,partname=>p_partition_name
                             ,distcnt=>1
                             ,density=>1 /*all rows same value*/
                             ,nullcnt=>0
                             ,srec=>l_srec
                             ,avgclen=>length(to_char(p_selector_num))
                             ,force=>TRUE);

  l_srec.epc     := 2;    /*two endpoints*/
  l_srec.eavs    := 0;
  l_srec.rpcnts  := NULL;
  l_srec.bkvals  := dbms_stats.numarray(0,p_num_rows); /*one bucket*/
  dbms_stats.prepare_column_values(l_srec,dbms_stats.chararray(g_range_from_min,g_range_from_max));
  dbms_stats.set_column_stats(ownname=>p_ownerid
                             ,tabname=>l_table_name
                             ,colname=>'RANGE_FROM_'||LTRIM(TO_CHAR(p_length,'00'))
                             ,partname=>p_partition_name
                             ,distcnt=>p_num_rows
                             ,density=>1/NULLIF(p_num_rows,0)
                             ,nullcnt=>0
                             ,srec=>l_srec
                             ,avgclen=>p_length+1
                             ,force=>TRUE);

  l_srec.epc     := 2;    /*two endpoints*/
  l_srec.eavs    := 0;
  l_srec.rpcnts  := NULL;
  l_srec.bkvals  := dbms_stats.numarray(0,p_num_rows); /*one bucket*/
  dbms_stats.prepare_column_values(l_srec,dbms_stats.chararray(g_range_to_min,g_range_to_max));
  dbms_stats.set_column_stats(ownname=>p_ownerid
                             ,tabname=>l_table_name
                             ,colname=>'RANGE_TO_'||LTRIM(TO_CHAR(p_length,'00'))
                             ,partname=>p_partition_name
                             ,distcnt=>p_num_rows
                             ,density=>1/NULLIF(p_num_rows,0)
                             ,nullcnt=>0
                             ,srec=>l_srec
                             ,avgclen=>p_length+1
                             ,force=>TRUE);

END set_selector_stats;
--------------------------------------------------------------------------------
--gather stats on tree selector
--------------------------------------------------------------------------------
PROCEDURE gather_selector_stats
(p_length       INTEGER
,p_selector_num INTEGER
,p_ownerid      VARCHAR2
,p_num_rows     INTEGER DEFAULT NULL
,p_status_flag  VARCHAR2 DEFAULT 'I'
) AS 
  l_table_name     psrecdefn.sqltablename%TYPE;
  l_partition_name all_tab_partitions.partition_name%TYPE;
  l_cmd            VARCHAR2(1000 CHAR);
  l_job_no         NUMBER;
BEGIN
  debug_msg('gather_selector_stats('||p_length||','||p_selector_num||','||p_ownerid||','||p_num_rows||','||p_status_flag||')');
  l_table_name     := 'PSTREESELECT'||LTRIM(TO_CHAR(p_length,'00'));
  l_partition_name := get_partition_name(p_selector_num);

  set_selector_stats(p_length, p_ownerid, l_partition_name, p_selector_num, p_num_rows);

  IF p_status_flag = 'S' OR k_stats_gather_job THEN
    l_cmd := 'dbms_stats.gather_table_stats(ownname=>'''||p_ownerid||''',tabname=>'''||l_table_name||'''';
    IF l_partition_name IS NOT NULL THEN
      l_cmd := l_cmd||',partname=>'''||l_partition_name||''',granularity=>''PARTITION''';
    END IF;
    l_cmd := l_cmd||',force=>TRUE);';
    debug_msg(l_cmd);

    BEGIN
      SELECT job
      INTO   l_job_no
      FROM   user_jobs
      WHERE  failures IS NULL
      AND    what = l_cmd;
      debug_msg(l_cmd||' already submitted as job '||l_job_no);
    EXCEPTION
      WHEN no_data_found THEN /*job not already running*/
       dbms_job.submit(l_job_no,l_cmd);

       UPDATE ps_nvs_treeslctlog
       SET    job_no = NVL(l_job_no,0)
       WHERE  selector_num = p_selector_num;

       debug_msg('submitted as job '||l_job_no);
    END;
  END IF;

END gather_selector_stats;

--------------------------------------------------------------------------------
--store selector number in package global variable
--------------------------------------------------------------------------------
PROCEDURE rowins
(p_selector_num INTEGER 
,p_range_from   VARCHAR2 DEFAULT NULL
,p_range_to     VARCHAR2 DEFAULT NULL
) AS 
BEGIN
  debug_msg('rowins('||p_selector_num||','||p_range_from||','||p_range_to||':g_selector_num='||g_selector_num||')',9);
  IF p_selector_num IS NULL THEN
    NULL;
  ELSIF g_selector_num != p_selector_num OR g_selector_num IS NULL THEN
    debug_msg('Reset global variables to this row',9);
    g_selector_num := p_selector_num;
    g_range_from_min := p_range_from;
    g_range_from_max := p_range_from;
    g_range_to_min := p_range_to;
    g_range_to_max := p_range_to;
    g_counter := 1;
  ELSE
    g_counter := g_counter+1;
    IF p_range_from IS NULL THEN
      NULL;
    ELSIF p_range_from < g_range_from_min THEN
      g_range_from_min := p_range_from;
    ELSIF p_range_from > g_range_from_max THEN
      g_range_from_max := p_range_from;
    END IF;

    IF p_range_to IS NULL THEN
      NULL;
    ELSIF p_range_to < g_range_to_min THEN
      g_range_to_min := p_range_to;
    ELSIF p_range_to > g_range_to_max THEN
      g_range_to_max := p_range_to;
    END IF;
  END IF;
  debug_msg('g_selector_num='||g_selector_num||' g_counter='||g_counter,9);
END rowins;

--------------------------------------------------------------------------------
--store selector number in package global variable
--------------------------------------------------------------------------------
PROCEDURE rowdel
(p_selector_num INTEGER 
) AS 
BEGIN
  IF p_selector_num IS NOT NULL THEN
    g_selector_num := p_selector_num;
--  g_counter := g_counter-1; /*no need to count deletions*/
  END IF;
  debug_msg('g_selector_num='||g_selector_num,9);
END rowdel;

--------------------------------------------------------------------------------
--purge 
--------------------------------------------------------------------------------
PROCEDURE purge
(p_selector_num INTEGER DEFAULT NULL
) AS 
  l_selector_num INTEGER;
  l_module       VARCHAR2(64 CHAR);
  l_action       VARCHAR2(64 CHAR);
  l_client_info  VARCHAR2(64 CHAR);
BEGIN
  dbms_application_info.read_module(l_module, l_action);
  dbms_application_info.read_client_info(l_client_info);
  dbms_application_info.set_module(NVL(l_module,k_module), NVL(l_action,'purge('||p_selector_num||')'));

  --add log entries for partitions where selector not logged
  FOR i IN (
    WITH x as (
      SELECT p.table_owner
      ,      p.table_name
      ,      SUBSTR(p.table_name,-2) length
      ,      p.partition_position
      ,      p.partition_name
      ,      NVL(p.num_rows,0) num_rows
      ,      NVL(p.last_analyzed,SYSDATE) timestamp
      ,      p.high_value, p.high_value_length
      FROM   all_tab_partitions p
      WHERE  p.table_name LIKE 'PSTREESELECT__'
    )
    SELECT x.*
    FROM   x
    WHERE  x.partition_position>1 --omit first partition 
    AND NOT EXISTS(
      SELECT 'x'
      FROM   ps_nvs_treeslctlog l
      WHERE  l.length = TO_NUMBER(x.length)
      AND    l.ownerid = x.table_owner
      AND    l.partition_name = x.partition_name)
  ) LOOP
    l_selector_num := SUBSTR(i.high_value,1,i.high_value_length) - 1;

    BEGIN
      INSERT INTO ps_nvs_treeslctlog
      (selector_num, process_instance, length, num_rows, timestamp, module, appinfo_action, client_info, status_flag, tree_name, ownerid, partition_name, job_no)
      VALUES
      (l_selector_num, 0, i.length, i.num_rows, i.timestamp, k_module, 'PURGE', NVL(l_client_info,' '), 'I', ' ', i.table_owner, i.partition_name, 0);
      debug_msg('Add log file entry for selector_num:'||l_selector_num||', partition '||i.partition_name);
    EXCEPTION 
      WHEN dup_val_on_index THEN
        UPDATE ps_nvs_treeslctlog
        SET    ownerid = i.table_owner
        ,      partition_name = i.partition_name
        ,      length = i.length
        WHERE  selector_num = l_selector_num;
        debug_msg('Update existing log entry for selector '||l_selector_Num||':'||i.table_owner||'.'||i.table_name||'.'||i.partition_name,8);
    END;

  END LOOP;

  --purge log entries where no process instance or older than timeout days
  FOR i IN (
    SELECT l.selector_num, l.length
    ,      l.status_flag, l.ownerid, l.partition_name, l.process_instance, l.timestamp
    ,      NVL(r.runstatus,0) runstatus
    FROM   ps_nvs_treeslctlog l
      LEFT OUTER JOIN psprcsrqst r
      ON r.prcsinstance = l.process_instance
    WHERE  l.status_flag IN('I','D')
    AND    l.partition_name != ' '
    AND   (   (l.process_instance > 0 AND (r.runstatus != '7' OR r.runstatus IS NULL))
           OR (l.process_instance = 0 AND timestamp < SYSDATE-k_timeout_days))
    AND   (l.selector_num = p_selector_num OR p_selector_num IS NULL)
    ORDER BY l.selector_num
  ) LOOP 
    purge_selector(i.length,i.selector_num,i.ownerid,i.partition_name);
  END LOOP;

  DELETE FROM ps_nvs_treeslctlog
  WHERE  status_flag = 'X'
  AND    timestamp < TRUNC(SYSDATE-k_purge_days);

  BEGIN
    FOR i IN(
      SELECT * 
      FROM   user_jobs
      WHERE  FAILURES >0
      AND   (what like '%nvision_selectors%'
      OR     what like 'gfcpsstats11.set_record_prefs(%);'
      OR     what LIKE 'dbms_stats.gather_table_stats(%PSTREESELECT%SYS_P%force%TRUE);')
    ) loop
      dbms_job.remove(i.job);
      COMMIT;
    END LOOP;
  END;

  dbms_application_info.set_module(l_module, l_action);
  
END purge;

--------------------------------------------------------------------------------
--reset selector num to 0, and clear out unlogged selectors
--------------------------------------------------------------------------------
PROCEDURE reset_selector_num AS
  l_sql CLOB;
  l_module       VARCHAR2(64 CHAR);
  l_action       VARCHAR2(64 CHAR);
BEGIN
  dbms_application_info.read_module(l_module, l_action);
  dbms_application_info.set_module(NVL(l_module,k_module), NVL(l_action,'reset_selector_num'));

  UPDATE pstreeselnum SET selector_num = 0;

  FOR i IN (
    SELECT owner, table_name
    FROM   all_tables
    WHERE  table_name = 'PSTREESELCTL'
  ) LOOP
    l_sql := 'DELETE FROM '||i.owner||'.'||i.table_name;
    EXECUTE IMMEDIATE l_sql;
    debug_msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted from '||i.owner||'.'||i.table_name);
  END LOOP;

  UPDATE ps_nvs_treeslctlog
  SET    status_flag = 'I'
  WHERE  status_flag = 'S';

  FOR i IN (
    SELECT owner, table_name
    ,      SUBSTR(table_name,-2) length
    FROM   all_part_tables
    WHERE  table_name LIKE 'PSTREESELECT__'
  ) LOOP
    l_sql := 'DELETE FROM '||i.owner||'.'||i.table_name
          ||' WHERE NOT selector_num IN(SELECT selector_num FROM ps_nvs_treeslctlog l WHERE l.ownerid='''||i.owner||''' AND l.length='||i.length||' AND status_flag=''I'')';
    debug_msg(l_sql,9);
    EXECUTE IMMEDIATE l_sql;
    debug_msg(TO_CHAR(SQL%ROWCOUNT)||' rows deleted from '||i.owner||'.'||i.table_name);
  END LOOP;

  COMMIT;
  purge;
  dbms_application_info.set_module(l_module, l_action);

END reset_selector_num;
--------------------------------------------------------------------------------
--delete entry from log table
--can assume all rows for a given selector deleted and only one selector deleted at a time
--------------------------------------------------------------------------------
PROCEDURE logdel
(p_length INTEGER
) AS 
  l_process_instance INTEGER;
  l_module           VARCHAR2(64 CHAR);
  l_action           VARCHAR2(64 CHAR);
  l_client_info      VARCHAR2(64 CHAR);
  l_length INTEGER;
BEGIN
  dbms_application_info.read_module(l_module, l_action);
  dbms_application_info.read_client_info(l_client_info);

  l_process_instance := psftapi.get_prcsinstance();

  debug_msg('logdel:selector_num='||g_selector_num);

  IF g_selector_num > 0 THEN
    UPDATE ps_nvs_treeslctlog
    SET    status_flag = 'D'
/*  ,      num_rows = 0 --retain num rows on log*/
    WHERE  selector_num = g_selector_num;

  --do not purge partition when deleting selectors
  --purge_selector_job(p_length,g_selector_num);

  END IF;
  g_selector_num := 0;
  g_counter := 0;
END logdel;

--------------------------------------------------------------------------------
--insert entry into log table
--------------------------------------------------------------------------------
PROCEDURE logins 
(p_length INTEGER
,p_ownerid VARCHAR2
) AS 
  l_process_instance INTEGER;
  l_module           VARCHAR2(64 CHAR);
  l_action           VARCHAR2(64 CHAR);
  l_client_info      VARCHAR2(64 CHAR);
--l_setid            ps_nvs_treeslctlog.setid%TYPE := ' ';
  l_tree_name        ps_nvs_treeslctlog.tree_name%TYPE := ' ';
  l_static_tree_name ps_nvs_treeslctlog.tree_name%TYPE := ' ';
  l_selector_num     INTEGER := 0;
  l_partition_name   all_tab_partitions.partition_name%TYPE;
  l_table_name       psrecdefn.sqltablename%TYPE;
  l_status_flag      ps_nvs_treeslctlog.status_flag%TYPE := 'I';
BEGIN
  dbms_application_info.read_module(l_module, l_action);
  dbms_application_info.read_client_info(l_client_info);

  l_process_instance := psftapi.get_prcsinstance();
  l_table_name := 'PSTREESELECT'||LTRIM(TO_CHAR(p_length,'00'));

  debug_msg('logins:selector_num='||g_selector_num||' counter='||g_counter||',owner='||p_ownerid);

  BEGIN --identify tree name to selector log
    debug_msg('INSERT INTO '||l_table_name||'%SELECT% '||g_selector_num||',%');
    SELECT DISTINCT 
--         substr(regexp_substr(s.SQL_TEXT,'SETID=\''[^'']+'),8) setid,
           substr(regexp_substr(s.SQL_TEXT,'TREE_NAME=\''[^'']+'),12) tree_name
    INTO   --l_setid, 
           l_tree_name
    FROM   sys.v_$sql s
    WHERE  s.sql_text like 'INSERT INTO '||l_table_name||'%SELECT DISTINCT '||g_selector_num||',%'
    AND    s.module = l_module
    AND    s.action = l_action
    AND    s.parsing_schema_name = p_ownerid
--  AND    ROWNUM=1
    ;
    debug_msg('Tree:'||l_tree_name);
  EXCEPTION
    WHEN too_many_rows THEN 
      debug_msg('Too Many Trees:'||l_tree_name,3);
      NULL;
    WHEN no_data_found THEN 
      debug_msg('No Tree Found',3);
      l_tree_name := ' ';
  END;

  --identify partition name
  debug_msg('Table '||p_ownerid||'.'||l_table_name||', selector '||g_selector_num||': Identify partition',8);
  FOR i IN ( /*run through the partitions in descending partition position order*/
    SELECT table_name, partition_position, partition_name, high_value, high_value_length
    FROM   all_tab_partitions p
    WHERE  table_owner = p_ownerid
    AND    table_name = l_table_name
    ORDER BY table_name, partition_position desc
  ) LOOP
    l_selector_num := SUBSTR(i.high_value,1,i.high_value_length) - 1; /*seletor high value-1*/
    IF l_selector_num = g_selector_num THEN
      l_partition_name := i.partition_name;
      debug_msg('Partition:'||l_partition_name);
      EXIT;
    ELSIF l_selector_num < g_selector_num THEN
      debug_msg('No Partition identified');
      EXIT;
    END IF;
  END LOOP;

  IF g_selector_num > 0 THEN
    BEGIN /*look up static selector table*/
      SELECT tree_name
      INTO   l_static_tree_name
      FROM   pstreeselctl
      WHERE  selector_num = g_selector_num;
      l_status_flag := 'S';
      l_tree_name := l_static_tree_name;
    EXCEPTION
      WHEN no_data_found THEN 
        l_status_flag := 'I';
    END;

    BEGIN
      INSERT INTO ps_nvs_treeslctlog
      (selector_num, process_instance, length, num_rows, timestamp, module, appinfo_action, client_info
      , status_flag, tree_name, ownerid, partition_name, job_no)
      VALUES
      (g_selector_num, NVL(l_process_instance,0), p_length, g_counter, systimestamp, NVL(l_module,' '), NVL(l_action,' '), NVL(l_client_info,' ')
      , l_status_flag, l_tree_name, p_ownerid, NVL(l_partition_name,' '), 0);
    EXCEPTION
      WHEN dup_val_on_index THEN
        UPDATE ps_nvs_treeslctlog l
        SET    l.num_rows = CASE WHEN l.status_flag IN ('D','X') THEN 0 ELSE l.num_rows END + g_counter
        ,      l.status_flag = l_status_flag
        ,      l.module = NVL(l_module,l.module)
        ,      l.appinfo_action = NVL(l_action,l.appinfo_action)
        ,      l.client_info = NVL(l_client_info,l.client_info)
        ,      l.tree_name = l_tree_name
        ,      l.ownerid = p_ownerid
        ,      l.partition_name = l_partition_name
        WHERE  l.selector_num = g_selector_num
        RETURNING num_rows INTO g_counter; /*get new total count of rows*/
    END;
  
    gather_selector_stats(p_length,g_selector_num,p_ownerid,g_counter,l_status_flag);
    g_selector_num := 0;
  END IF;
--g_counter := 0;
END logins;
--------------------------------------------------------------------------------
END xx_nvision_selectors;
/
show errors
spool off
pause
