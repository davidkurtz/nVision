REM gfc_nvsion_excel_redirect_metadata.sql
REM (c)Go-Faster Consultancy 2021
REM load metadata of layouts that have to run on Excel rather than OpenXML

spool gfc_nvsion_excel_redirect_metadata

INSERT INTO ps_nvs_redir_excel VALUES ('EXCELNVS','A');
commit;

spool off
