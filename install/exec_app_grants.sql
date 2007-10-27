PROMPT 'Running exec_app_grants.sql'
-- creates a role for a repository user
-- grants the role to a repository user

-- &1 IS the application SCHEMA
DEFINE app_schema_eag = &1
-- create role for this application
DROP ROLE &app_schema_eag._app;
CREATE ROLE &app_schema_eag._app;

-- grant execute on components to this role
GRANT EXECUTE ON STRING_AGG_OT to &app_schema_eag._app;
GRANT EXECUTE ON TD_EXT to &app_schema_eag._app;
GRANT EXECUTE ON TD_INST to &app_schema_eag._app;
GRANT EXECUTE ON APP_OT to &app_schema_eag._app;
GRANT EXECUTE ON NOTIFY_OT to &app_schema_eag._app;
GRANT EXECUTE ON EMAIL_OT to &app_schema_eag._app;
GRANT EXECUTE ON EVOLVE_OT to &app_schema_eag._app;
GRANT EXECUTE ON TD_HOST to &app_schema_eag._app;
GRANT EXECUTE ON TD_SQL to &app_schema_eag._app;
GRANT EXECUTE ON FILE_OT to &app_schema_eag._app;
GRANT EXECUTE ON EXTRACT_OT to &app_schema_eag._app;
GRANT EXECUTE ON FEED_OT to &app_schema_eag._app;
GRANT EXECUTE ON TD_DDL to &app_schema_eag._app;
GRANT EXECUTE ON TD_ETL to &app_schema_eag._app;
GRANT EXECUTE ON TD_FILES to &app_schema_eag._app;
GRANT EXECUTE ON TD_CONTROL to &app_schema_eag._app;
GRANT EXECUTE ON TD_OWB to &app_schema_eag._app;

-- grant select on the object views to this role
GRANT SELECT ON EMAIL_OV to &app_schema_eag._app;
GRANT SELECT ON EXTRACT_OV to &app_schema_eag._app;
GRANT SELECT ON FEED_OV to &app_schema_eag._app;
