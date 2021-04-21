Additional Instrumentation

1. fga_sys_privs
2. fga_handler

nVision tree selector logging

3. nvision_dynamic_selectors.sql - build logging table and a load of one time fixes this then calls
	a) xx_nvision_selectors.sql - create package
	b) treeselector_triggers.sql - buld compound triggers on tree selectors

Monitoring Reports

4. treeanal - selector log