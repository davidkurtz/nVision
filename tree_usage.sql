REM tree_usage.sql
Set lines 200 pages 999 termout on
Break on report
Column length      heading 'Len'          format 99 
Column num_uses    heading 'Num|Uses'     format 99999
Column min_rows    heading 'Min|Rows'     format 999999
Column max_rows    heading 'Max|Rows'     format 999999
Column med_rows    heading 'Median|Rows'  format 999999
Column sum_rows    heading 'Sum|Rows'     format 99999999
Column avg_rows    heading 'Average|Rows' format 999999 
Column stddev_rows heading 'Std Dev|Rows' format 999999 
Column processes   heading 'Num|Procs'    format 9999 
column tree_acc_method   format a4 heading 'Acc|Meth'
column tree_Acc_Selector format a3 heading 'Sel'
column tree_acc_Sel_opt  format a3 heading 'Sel|Opt'
column num_trees format 9999 heading 'Num|Trees'
column max_effdt heading 'Max|Effdt'
spool tree_usage
With t as (
Select DISTINCT d.tree_name, s.dtl_fieldname
,      DECODE(d.tree_acc_method  ,' ','.',d.tree_acc_method)   tree_acc_method
,      DECODE(d.tree_acc_Selector,' ','.',d.tree_acc_Selector) tree_acc_Selector
,      DECODE(d.tree_acc_sel_opt ,' ','.',d.tree_acc_sel_opt)  tree_acc_sel_opt
,      COUNT(*) num_trees
,      MAX(d.effdt) max_effdt
From   pstreedefn d, pstreestrct s 
where  d.tree_Strct_id = s.tree_strct_id 
group by d.tree_name, s.dtl_fieldname, d.tree_acc_method, d.tree_acc_selector, d.tree_acc_Sel_opt 
), l as (
Select tree_name 
,      length 
,      count(*) num_uses 
,      min(num_rows) min_rows 
,      avg(num_rows) avg_rows 
,      median(num_Rows) med_rows 
,      max(num_rowS) max_rows 
,      stddev(num_Rows) stddev_rows 
,      sum(num_rows) sum_rows 
,      count(distinct process_instance) processes 
From   ps_nvs_treeslctlog l 
Where  num_rows>0 
/*and APPINFO_ACTION like 'PI=%:%:%'
And timestamp >= sysdate-7*/
Group by tree_name, length
)
Select l.*
,      t.dtl_fieldname, t.tree_acc_method, t.tree_acc_Selector, t.tree_acc_sel_opt
,      t.num_trees, t.max_effdt
FROM l
     LEFT OUTER JOIN t
     ON l.tree_name = t.tree_name
Order by sum_rows
/
spool off
