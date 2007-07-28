-- set the default logging, registration and runmodes
EXEC td_control.set_logging_level('default',2,3);
EXEC td_control.set_runmode;
EXEC td_control.set_registration;

-- don't register TD_SQL components
-- that's because DBMS_MONITOR should look for calling modules for tracing
exec td_control.set_registration('td_sql.exec_sql','noregister');
exec td_control.set_registration('td_sql.exec_auto','noregister');
