REM tree_usage.sql
Set lines 200 pages 999
Break on report
Column length format 99 heading 'Len'
Column num_uses heading 'Num|Uses' format 99999
Column min_rows heading 'Min|Rows' format 99999
Column max_rows heading 'Max|Rows' format 99999
Column med_rows heading 'Median|Rows' format 99999
Column sum_rows heading 'Sum|Rows' format 99999999
Column avg_rows format 99999 heading 'Average|Rows'
Column stddev_rows format 99999 heading 'Std Dev|Rows'
Column processes format 9999 heading 'Num|Procs'
spool tree_usage
With t as (
Select DISTINCT d.tree_name, s.dtl_fieldname, d.tree_acc_method
,       d.tree_acc_Selector, d.tree_acc_sel_opt
From pstreedefn d, pstreestrct s
where d.tree_Strct_id = s.tree_strct_id
), l as (
Select  tree_name
, length
, count(*) num_uses
, min(num_rows) min_rows
, avg(num_rows) avg_rows
, median(num_Rows) med_rows
, max(num_rowS) max_rows
, stddev(num_Rows) stddev_rows
, sum(num_rows) sum_rows
, count(distinct process_instance) processes
From ps_nvs_treeslctlog l
Where num_rows>0
And timestamp >= sysdate-7
Group by tree_name, length
)
Select l.*, 
t.dtl_fieldname, t.tree_acc_method, t.tree_acc_Selector, t.tree_acc_sel_opt
FROM t, l
WHERE t.tree_name = l.tree_name
Order by sum_rows
/
spool off