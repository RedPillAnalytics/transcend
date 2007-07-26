PROMPT 'Running exec_app_grants.sql'
-- creates a role for a repository user
-- grants the role to a repository user

-- &1 IS the application SCHEMA
DEFINE app_schema = &1
-- create role for this application
DROP ROLE &app_schema._app;
CREATE ROLE &app_schema._app;

-- grant execute on components to this role
GRANT EXECUTE ON STRING_AGG_TYPE to &app_schema._app;
GRANT EXECUTE ON TD_EXT to &app_schema._app;
GRANT EXECUTE ON BASETYPE to &app_schema._app;
GRANT EXECUTE ON APPTYPE to &app_schema._app;
GRANT EXECUTE ON NOTIFYTYPE to &app_schema._app;
GRANT EXECUTE ON EMAILTYPE to &app_schema._app;
GRANT EXECUTE ON TDTYPE to &app_schema._app;
GRANT EXECUTE ON TD_CORE to &app_schema._app;
GRANT EXECUTE ON TD_SQL to &app_schema._app;
GRANT EXECUTE ON FILETYPE to &app_schema._app;
GRANT EXECUTE ON EXTRACTTYPE to &app_schema._app;
GRANT EXECUTE ON FEEDTYPE to &app_schema._app;
GRANT EXECUTE ON TD_DBAPI to &app_schema._app;
GRANT EXECUTE ON TD_FILEAPI to &app_schema._app;
GRANT EXECUTE ON TD_CONTROL to &app_schema._app;
GRANT EXECUTE ON TD_OWBAPI to &app_schema._app;

-- grant select on the object views to this role
GRANT SELECT ON EMAIL_OT to &app_schema._app;
GRANT SELECT ON EXTRACT_OT to &app_schema._app;
GRANT SELECT ON FEED_OT to &app_schema._app;
