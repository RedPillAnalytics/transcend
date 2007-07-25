-- creates a role for a repository user
-- grants the role to a repository user

-- &1 IS the application SCHEMA

-- create role for this application
DROP ROLE &1._app;
CREATE ROLE &1._app;

-- grant execute on components to this role
GRANT EXECUTE ON STRING_AGG_TYPE to &1._app;
GRANT EXECUTE ON TD_EXT to &1._app;
GRANT EXECUTE ON BASETYPE to &1._app;
GRANT EXECUTE ON APPTYPE to &1._app;
GRANT EXECUTE ON NOTIFYTYPE to &1._app;
GRANT EXECUTE ON EMAILTYPE to &1._app;
GRANT EXECUTE ON TDTYPE to &1._app;
GRANT EXECUTE ON TD_CORE to &1._app;
GRANT EXECUTE ON TD_SQL to &1._app;
GRANT EXECUTE ON FILETYPE to &1._app;
GRANT EXECUTE ON EXTRACTTYPE to &1._app;
GRANT SELECT ON EXTRACT_OT_vw to &1._app;
GRANT EXECUTE ON FEEDTYPE to &1._app;
GRANT EXECUTE ON TD_DBAPI to &1._app;
GRANT EXECUTE ON TD_FILEAPI to &1._app;
GRANT EXECUTE ON TD_CONTROL to &1._app;
GRANT EXECUTE ON TD_OWBAPI to &1._app;

-- grant select on the object views to this role
GRANT SELECT ON EMAIL_OT to &1._app;
GRANT SELECT ON EXTRACT_OT to &1._app;
GRANT SELECT ON FEED_OT to &1._app;
