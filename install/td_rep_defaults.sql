DEFINE app_schema_tfd = &1

ALTER SESSION SET current_schema=&app_schema_tfd;

-- set the default logging, registration and runmodes
EXEC td_control.set_logging_level('default',2,3);
EXEC td_control.set_runmode;
EXEC td_control.set_registration;

ALTER SESSION SET current_schema=&_USER;
