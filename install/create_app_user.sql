PROMPT 'Running create_app_user.sql'

-- &1 IS the repository schema
DEFINE app_schema_cau = &1

WHENEVER sqlerror exit sql.sqlcode
DECLARE
   e_user_exists EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_user_exists, -1920 );
BEGIN
   BEGIN
      EXECUTE IMMEDIATE 'CREATE USER &app_schema_cau identified by no2&app_schema_cau';
      EXECUTE IMMEDIATE 'grant connect to &app_schema_cau';
   EXCEPTION
      WHEN e_user_exists
      THEN
      NULL;
   END;
END;
/
WHENEVER sqlerror continue