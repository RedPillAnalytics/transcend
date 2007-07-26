PROMPT 'Running create_app_user.sql'

-- &1 IS the repository schema
DEFINE app_schema_cau = &1
-- &2 IS the tablespace name
DEFINE tablespace_cu = &2

WHENEVER sqlerror exit sql.sqlcode

VARIABLE old_tbspace char(30)
VARIABLE tbspace_changed char(3)
DECLARE
   l_app_schema_cau VARCHAR2(30) := upper('&app_schema_cau');
   l_tablespace VARCHAR2(30) := upper('&tablespace_cu');
   e_user_exists EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_user_exists, -1920 );
   e_no_tbspace	 EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_no_tbspace, -959 );
BEGIN
   BEGIN
      EXECUTE IMMEDIATE 'CREATE USER '
      	      		||l_app_schema_cau
                        ||' identified by no2'
                        ||l_app_schema_cau;
   EXCEPTION
      WHEN e_user_exists
      THEN
      NULL;
   END;
END;
/
WHENEVER sqlerror continue