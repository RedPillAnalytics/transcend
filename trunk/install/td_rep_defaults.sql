DEFINE app_schema_tfd = &1

-- set the default logging, registration and runmodes
EXEC &app_schema_tfd..td_control.set_logging_level('default',2,3);
EXEC &app_schema_tfd..td_control.set_runmode;
EXEC &app_schema_tfd..td_control.set_registration;

-- don't register TD_SQL components
-- that's because DBMS_MONITOR should look for calling modules for tracing
exec &app_schema_tfd..td_control.set_registration('td_sql.exec_sql','noregister');
exec &app_schema_tfd..td_control.set_registration('td_sql.exec_auto','noregister');
