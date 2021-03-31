REM excel_only_reportbooks.sql
REM (c)Go-Faster Consultancy 2021
REM Reportbooks with layouts identified as required to be run on Excel nVision

break on report
compute sum of all_layouts on report
compute sum of num_excel_layouts on report
column oprid format a10
column runcntlid format a20
column all_layouts       heading 'All|Layouts' format 9999
column num_excel_layouts heading 'Number of|Excel|Layouts'
column excel_layouts     heading 'Excel Layouts' format a30
column openxml_layouts   heading 'OpenXML Layouts' format a70
spool excel_only_reportbooks

set lines 180
ttitle 'ReportBooks with both Excel and OpenXML nVision layouts'
with x as (
select b.oprid, b.run_cntl_id
,      COUNT(DISTINCT n.layout_id) all_layouts
,      count(DISTINCT e.layout_id) num_excel_layouts
,      listagg(DISTINCT e.layout_id,', ') within group (order by e.layout_id) excel_layouts
,      listagg(DISTINCT CASE WHEN e.layout_id IS NULL THEN n.layout_id END,', ') within group (order by e.layout_id) openxml_layouts
FROM   psnvsbookrequst b
,      ps_nvs_report n
  LEFT OUTER JOIN ps_nvs_redir_excel e
  ON n.layout_id = e.layout_id
  AND e.eff_status = 'A'
where  b.eff_status = 'A'
and    n.business_unit = b.business_unit
and    n.report_id = b.report_id
group by b.oprid, b.run_cntl_id
)
select *
from x
where num_excel_layouts > 0
and all_layouts > num_excel_layouts
/


ttitle 'ReportBooks with only Excel nVision layouts'
with x as (
select b.oprid, b.run_cntl_id
,      COUNT(DISTINCT n.layout_id) all_layouts
,      count(DISTINCT e.layout_id) num_excel_layouts
,      listagg(DISTINCT e.layout_id,', ') within group (order by e.layout_id) excel_layouts
,      listagg(DISTINCT CASE WHEN e.layout_id IS NULL THEN n.layout_id END,', ') within group (order by e.layout_id) openxml_layouts
FROM   psnvsbookrequst b
,      ps_nvs_report n
  LEFT OUTER JOIN ps_nvs_redir_excel e
  ON n.layout_id = e.layout_id
  AND e.eff_status = 'A'
where  b.eff_status = 'A'
and    n.business_unit = b.business_unit
and    n.report_id = b.report_id
group by b.oprid, b.run_cntl_id
)
select *
from x
where num_excel_layouts > 0
and all_layouts = num_excel_layouts
ORDER BY 1,2
/

/*
ttitle 'ReportBooks with only OpenXML nVision layouts'
with x as (
select b.oprid, b.run_cntl_id
,      COUNT(DISTINCT n.layout_id) all_layouts
,      count(DISTINCT e.layout_id) num_excel_layouts
,      listagg(DISTINCT e.layout_id,', ') within group (order by e.layout_id) excel_layouts
,      listagg(DISTINCT CASE WHEN e.layout_id IS NULL THEN n.layout_id END,', ') within group (order by e.layout_id) openxml_layouts
FROM   psnvsbookrequst b
,      ps_nvs_report n
  LEFT OUTER JOIN ps_nvs_redir_excel e
  ON n.layout_id = e.layout_id
  AND e.eff_status = 'A'
where  b.eff_status = 'A'
and    n.business_unit = b.business_unit
and    n.report_id = b.report_id
group by b.oprid, b.run_cntl_id
)
select *
from x
where num_excel_layouts = 0
and all_layouts > 0
ORDER BY 1,2
*/
ttitle off
spool off


