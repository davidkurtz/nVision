REM pstreeselctl.sql
spool pstreeselctl append

CREATE TABLE &&1..PSTREESELCTL (SETID VARCHAR2(5) NOT NULL,
   SETCNTRLVALUE VARCHAR2(20) NOT NULL,
   TREE_NAME VARCHAR2(18) NOT NULL,
   EFFDT DATE NOT NULL,
   VERSION INTEGER NOT NULL,
   SELECTOR_NUM INTEGER NOT NULL,
   SELECTOR_DT DATE NOT NULL,
   TREE_ACC_SEL_OPT VARCHAR2(1) NOT NULL,
   LENGTH SMALLINT NOT NULL) 
 TABLESPACE PTWORK 
 STORAGE (INITIAL 40000 NEXT 100000 MAXEXTENTS UNLIMITED PCTINCREASE 0) 
 PCTFREE 10 PCTUSED 80
/
CREATE UNIQUE  iNDEX &&1..PS_PSTREESELCTL ON &&1..PSTREESELCTL (SETID,
   SETCNTRLVALUE,
   TREE_NAME,
   EFFDT /*DESC*/) 
 TABLESPACE PSINDEX 
 STORAGE (INITIAL 40000 NEXT 100000 MAXEXTENTS UNLIMITED PCTINCREASE 0) 
 PCTFREE 10 PARALLEL NOLOGGING
/
ALTER INDEX &&1..PS_PSTREESELCTL NOPARALLEL LOGGING
/

GRANT SELECT, INSERT, UPDATE, DELETE ON pstreeselctl TO &&1.;

CREATE OR REPLACE TRIGGER &&1..xx_pstreeselctl_inc
AFTER INSERT OR UPDATE OR DELETE ON &&1..pstreeselctl
FOR EACH ROW
BEGIN
  IF deleting THEN
    UPDATE &&1..ps_nvs_treeslctlog
    SET    status_flag = 'D'
    ,      tree_name = :old.tree_name
    WHERE  selector_num = :old.selector_num
    AND    ownerid = '&&1.'
    AND    status_flag = 'S';
  ELSE
    UPDATE &&1..ps_nvs_treeslctlog
    SET    status_flag = 'S'
    ,      tree_name = :new.tree_name
    WHERE  selector_num = :new.selector_num
    AND    ownerid = '&&1.';
  END IF;
EXCEPTION WHEN OTHERS THEN NULL; --exception deliberately coded to suppress all exceptions
END;
/
spool off
