DEFINE app_schema_tfd = &1

ALTER SESSION SET current_schema=&app_schema_tfd;

-- set the default logging, registration and runmodes
EXEC td_control.set_logging_level('default',2,3);
EXEC td_control.set_runmode;
EXEC td_control.set_registration;

-- add notification events
EXEC td_control.add_notification_event('file_ot.audit_file','file too large','File outside size threshholds','The file referenced below is larger than the configured threshhold:');
EXEC td_control.add_notification_event('file_ot.audit_file','file too small','File outside size threshholds','The file referenced below is smaller than the configured threshhold:');

ALTER SESSION SET current_schema=&_USER;
