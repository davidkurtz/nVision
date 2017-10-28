REM nvperfopts.sql
REM (c)2013 David Kurtz
REM 2017 modified for Chubb

spool nvperfopts
set pages 99 lines 200 trimspool on echo on
break on setid on tree_name skip 1
rollback
/
SELECT T.SETID, T.TREE_NAME, T.EFFDT
, T.TREE_ACC_SELECTOR --static selctors
, (SELECT X1.XLATSHORTNAME FROM PSXLATITEM X1 WHERE X1.FIELDNAME = 'TREE_ACC_SELECTOR' AND X1.FIELDVALUE = T.TREE_ACC_SELECTOR) translation
, T.TREE_ACC_SEL_OPT --between
, (SELECT X2.XLATSHORTNAME FROM PSXLATITEM X2 WHERE X2.FIELDNAME = 'TREE_ACC_SEL_OPT' AND X2.FIELDVALUE = T.TREE_ACC_SEL_OPT) transation
, T.TREE_ACC_METHOD --literals
, (SELECT X3.XLATSHORTNAME FROM PSXLATITEM X3 WHERE X3.FIELDNAME = 'TREE_ACC_METHOD' AND X3.FIELDVALUE = T.TREE_ACC_METHOD) transation
FROM PSTREEDEFN T
WHERE /*(T.TREE_ACC_SELECTOR != 'S'
OR T.TREE_ACC_SEL_OPT != 'B'
OR T.TREE_ACC_METHOD !='L')
AND */ t.tree_strct_id IN(SELECT tree_strct_id FROM pstreestrct WHERE node_fieldname = 'TREE_NODE')
and 1=2
ORDER BY 1,2,3
/

/*increment the version numbers */
UPDATE PSLOCK
SET VERSION = VERSION + 1
WHERE OBJECTTYPENAME IN('SYS', 'TDM')
/

UPDATE PSVERSION
SET VERSION = VERSION + 1
WHERE OBJECTTYPENAME IN('SYS', 'TDM')
/

/*update nvision flags and version number on trees*/
/*This is the general setting */
UPDATE PSTREEDEFN
SET TREE_ACC_SELECTOR = 'S' --static selctors
, TREE_ACC_SEL_OPT = 'B' --between - cannot single on static
, TREE_ACC_METHOD = 'L' --literals
, VERSION = (SELECT VERSION FROM PSLOCK WHERE OBJECTTYPENAME = 'TDM')
, lastupddttm = SYStimestamp
, lastupdoprid = 'DAVID.KURTZ'
WHERE (TREE_ACC_SELECTOR != 'S'
OR TREE_ACC_SEL_OPT != 'B'
OR TREE_ACC_METHOD != 'L')
AND tree_strct_id IN(SELECT tree_strct_id FROM pstreestrct WHERE node_fieldname = 'TREE_NODE')
/

UPDATE PSTREEDEFN
SET TREE_ACC_SELECTOR = 'D' --dynamic selctors
, TREE_ACC_SEL_OPT = 'S' --single values
, TREE_ACC_METHOD = 'L' --literal
, VERSION = (SELECT VERSION FROM PSLOCK WHERE OBJECTTYPENAME = 'TDM')
, lastupddttm = SYStimestamp
, lastupdoprid = 'DAVID.KURTZ'
WHERE (TREE_ACC_SELECTOR != 'D'
OR TREE_ACC_SEL_OPT != 'S'
OR TREE_ACC_METHOD != 'L')
--AND setid = 'GLOBE'
AND tree_strct_id IN(SELECT tree_strct_id FROM pstreestrct WHERE node_fieldname = 'TREE_NODE')
AND (tree_strct_id IN('ACCOUNT') /*add your list of tree structures for dynamic selectors*/
OR   tree_name IN('ACCOUNT')) /*list of trees for dynamic selectors*/
/

UPDATE PSTREEDEFN
SET TREE_ACC_SELECTOR = 'D' --dynamic selctors
, TREE_ACC_SEL_OPT = 'S' --single values
, TREE_ACC_METHOD = 'J' --join
, VERSION = (SELECT VERSION FROM PSLOCK WHERE OBJECTTYPENAME = 'TDM')
, lastupddttm = SYStimestamp
, lastupdoprid = 'DAVID.KURTZ'
WHERE (TREE_ACC_SELECTOR != 'D'
OR TREE_ACC_SEL_OPT != 'S'
OR TREE_ACC_METHOD != 'J')
AND tree_name IN('BIGTREE') /*list of trees to use single value joins
/


SELECT T.SETID, T.TREE_NAME, T.EFFDT, t.tree_strct_id, dtl_recname, dtl_fieldname
, T.TREE_ACC_SELECTOR --static selctors
, (SELECT X1.XLATSHORTNAME FROM PSXLATITEM X1 WHERE X1.FIELDNAME = 'TREE_ACC_SELECTOR' AND X1.FIELDVALUE = TREE_ACC_SELECTOR) translation
, T.TREE_ACC_SEL_OPT --between
, (SELECT X2.XLATSHORTNAME FROM PSXLATITEM X2 WHERE X2.FIELDNAME = 'TREE_ACC_SEL_OPT' AND X2.FIELDVALUE = TREE_ACC_SEL_OPT) translation
, T.TREE_ACC_METHOD --literals
, (SELECT X3.XLATSHORTNAME FROM PSXLATITEM X3 WHERE X3.FIELDNAME = 'TREE_ACC_METHOD' AND X3.FIELDVALUE = TREE_ACC_METHOD) translation
FROM PSTREEDEFN T
, pstreestrct s
WHERE /*(T.TREE_ACC_SELECTOR = 'S'
OR T.TREE_ACC_SEL_OPT = 'S'
OR T.TREE_ACC_METHOD IN('L'))
AND */ t.tree_strct_id = s.tree_strct_id
AND s.node_fieldname = 'TREE_NODE'
ORDER BY 1,2,3
/

select lastrefreshdttm from psstatus
/
UPDATE psstatus
SET lastrefreshdttm = SYSTIMESTAMP
/
select lastrefreshdttm from psstatus
/


--------------------------------------------------------------------------------
--remove erroneous static selectors
--------------------------------------------------------------------------------
set serveroutput on 
BEGIN
  FOR i IN (
select	c.*
from	pstreeselctl c
,	ps_nvs_treeslctlog l
,	pstreedefn d
where	c.setid = d.setid
and	c.setcntrlvalue = d.setcntrlvalue
and	c.tree_name = d.tree_name
and	c.effdt = d.effdt
and	l.selector_num = c.selector_num
and	d.tree_name = l.tree_name
and	d.tree_acc_selector = 'D'
order by c.selector_num
  ) LOOP
    UPDATE ps_nvs_treeslctlog 
    SET    status_flag = 'D'
    WHERE  selector_num = i.selector_Num
    AND    status_flag = 'S';
    DELETE FROM pstreeselctl 
    WHERE  selector_num = i.selector_Num
    AND    tree_name = i.tree_name;
  END LOOP;
END;
/


--------------------------------------------------------------------------------
--report on trees
--------------------------------------------------------------------------------
select l.setid, l.tree_name, l.effdt, x.selector_num, count(*)
from pstreeleaf l, pstreeselctl x
where l.setid = x.setid
and l.tree_name = x.tree_name
and l.effdt = x.effdt
and l.setcntrlvalue = x.setcntrlvalue
group by x.selector_num, l.setid, l.setcntrlvalue, l.tree_name, l.effdt
order by 1,2,3
/
REM now commit or rollback

spool off
