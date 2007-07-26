PROMPT 'Running create_user.sql'

-- &1 IS the repository schema
DEFINE rep_schema = &1
-- &2 IS the tablespace name
DEFINE tablespace = &2

WHENEVER sqlerror exit sql.sqlcode
DECLARE
   l_rep_schema VARCHAR2(30) := '&rep_schema';
   l_tablespace VARCHAR2(30) := '&tablespace';
   e_user_exists EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_user_exists, -1920 );
   e_no_tbspace	 EXCEPTION;
   PRAGMA EXCEPTION_INIT( e_no_tbspace, -959 );
BEGIN
   BEGIN
      EXECUTE IMMEDIATE 'CREATE USER '
      	      		||l_rep_schema
                        ||' identified by no2'
                        ||l_rep_schema
                        ||' default tablespace '
      			||l_tablespace;
   EXCEPTION
      WHEN e_user_exists
      THEN
        NULL;
      WHEN e_no_tbspace
      THEN
      raise_application_error(-20001,'Tablespace '||upper(l_tablespace)||' does not exist');
   END;
END;
/
WHENEVER sqlerror continue