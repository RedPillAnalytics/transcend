DEFINE app_schema_tfd = &1

ALTER SESSION SET current_schema=&app_schema_tfd;

-- set the default logging, registration and runmodes
EXEC td_control.set_logging_level('default',2,3);
EXEC td_control.set_runmode;
EXEC td_control.set_registration;

-- don't register TD_SQL components
-- that's because DBMS_MONITOR should look for calling modules for tracing
exec &app_schema_tfd..td_control.set_registration('td_sql.exec_sql','noregister');
exec &app_schema_tfd..td_control.set_registration('td_sql.exec_auto','noregister');

ALTER SESSION SET current_schema=&_USER;
