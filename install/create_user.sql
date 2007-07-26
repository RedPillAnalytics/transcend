PROMPT 'Running create_user.sql'

-- &1 IS the repository schema
DEFINE rep_schema = &1
-- &2 IS the tablespace name
DEFINE tablespace = &2

WHENEVER sqlerror exit sql.sqlcode

VARIABLE old_tbspace char(30)
DECLARE
   l_rep_schema VARCHAR2(30) := upper('&rep_schema');
   l_tablespace VARCHAR2(30) := upper('&tablespace');
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
      -- get the current default tablespace of the repository user
      SELECT default_tablespace
	INTO :old_tbspace
	FROM dba_users
       WHERE username=l_rep_schema;
      WHEN e_no_tbspace
      THEN
      raise_application_error(-20001,'Tablespace '||l_tablespace||' does not exist');
   END;
END;
/
WHENEVER sqlerror continue