CREATE OR REPLACE PACKAGE BODY td_log
AS
   -- global variables placed in the package body because they should be accessed or set outside the package
   g_session_id	      NUMBER         DEFAULT SYS_CONTEXT( 'USERENV', 'SESSIONID' );
   g_instance_name    VARCHAR(30)    DEFAULT SYS_CONTEXT( 'USERENV', 'INSTANCE_NAME' );
   g_machine	      VARCHAR2(50)   DEFAULT SYS_CONTEXT( 'USERENV', 'HOST' )
			     	       	     || '['
				 	     || SYS_CONTEXT( 'USERENV', 'IP_ADDRESS' )
         				     || ']';
   g_dbuser	      VARCHAR2(30)   DEFAULT SYS_CONTEXT( 'USERENV', 'SESSION_USER' );
   g_osuser	      VARCHAR2(30)   DEFAULT SYS_CONTEXT( 'USERENV', 'OS_USER' );
   g_client_info      VARCHAR2(30);
   g_module	      VARCHAR2(30);
   g_action	      VARCHAR2(30);
   g_registration     VARCHAR2(30);
   g_logging_level    VARCHAR2(30);
   g_runmode 	      VARCHAR2(10) DEFAULT 'runtime';

   -- decipher the calling package name
   PROCEDURE set_default_module
   AS
      l_call_stack    VARCHAR2( 4096 ) DEFAULT DBMS_UTILITY.format_call_stack;
      l_num           NUMBER;
      l_found_stack   BOOLEAN          DEFAULT FALSE;
      l_line          VARCHAR2( 255 );
      l_cnt           NUMBER           := 0;
      l_name          VARCHAR2( 30 );
      l_caller        VARCHAR2( 30 );
   BEGIN
      LOOP
         l_num := INSTR( l_call_stack, CHR( 10 ));
         EXIT WHEN( l_cnt = 3 OR l_num IS NULL OR l_num = 0 );
         l_line := SUBSTR( l_call_stack, 1, l_num - 1 );
         l_call_stack := SUBSTR( l_call_stack, l_num + 1 );

         IF ( NOT l_found_stack )
         THEN
            IF ( l_line LIKE '%handle%number%name%' )
            THEN
               l_found_stack := TRUE;
            END IF;
         ELSE
            l_cnt := l_cnt + 1;

            -- l_cnt = 1 is ME
            -- l_cnt = 2 is MY Caller
            -- l_cnt = 3 is Their Caller
            IF ( l_cnt = 3 )
            THEN
               l_line := SUBSTR( l_line, 21 );

               IF ( l_line LIKE 'pr%' )
               THEN
                  l_num := LENGTH( 'procedure ' );
               ELSIF( l_line LIKE 'fun%' )
               THEN
                  l_num := LENGTH( 'function ' );
               ELSIF( l_line LIKE 'package body%' )
               THEN
                  l_num := LENGTH( 'package body ' );
               ELSIF( l_line LIKE 'pack%' )
               THEN
                  l_num := LENGTH( 'package ' );
               ELSIF( l_line LIKE 'anonymous%' )
               THEN
                  l_num := LENGTH( 'anonymous block ' );
               ELSE
                  l_num := NULL;
               END IF;

               IF ( l_num IS NOT NULL )
               THEN
                  l_caller := LTRIM( RTRIM( UPPER( SUBSTR( l_line, 1, l_num - 1 ))));
               ELSE
                  l_caller := 'TRIGGER';
               END IF;

               l_line := SUBSTR( l_line, NVL( l_num, 1 ));
               l_num := INSTR( l_line, '.' );
               l_name := LTRIM( RTRIM( SUBSTR( l_line, l_num + 1 )));
            END IF;
         END IF;
      END LOOP;

      g_module := LOWER( l_name );
   END set_default_module;

   -- begins debug mode
   PROCEDURE begin_debug
   AS
   BEGIN
      g_runmode := 'debug';
   END begin_debug;

   -- begins debug mode
   PROCEDURE end_debug
   AS
   BEGIN
      g_runmode := 'debug';
   END end_debug;
      
   -- returns Boolean for determining runmode
   FUNCTION is_debugmode
      RETURN BOOLEAN
   AS
   BEGIN
      RETURN CASE g_runmode
         WHEN 'debug'
            THEN TRUE
         ELSE FALSE
      END;
   END is_debugmode;   
   
   -- DEFAULT ACCESSOR METHODS
   FUNCTION runmode
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_runmode;
   END runmode;      
   
   PROCEDURE runmode ( p_runmode VARCHAR2 )
   AS
   BEGIN
      g_runmode := p_runmode;
   END runmode;      
   
   FUNCTION registration
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_registration;
   END registration;      
   
   PROCEDURE registration ( p_registration VARCHAR2 )
   AS
   BEGIN
      g_registration := p_registration;
   END registration;

   FUNCTION logging_level
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_logging_level;
   END logging_level;      
   
   PROCEDURE logging_level ( p_logging_level VARCHAR2 )
   AS
   BEGIN
      g_logging_level := p_logging_level;
   END logging_level;      
   
   -- MODIFIED ACCESSOR METHODS
   
   -- when setting the module, only have to give the stored program unit name
   -- method will append the rest
   FUNCTION module
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN g_module;
   END module;      
   
   PROCEDURE module ( p_module VARCHAR2 )
   AS
   BEGIN
      g_module := p_module;
   END module;      

   
   -- CUSTOM ACCESSOR METHODS
   -- used to return a distinct error message number by label
   FUNCTION get_err_cd( p_name VARCHAR2 )
      RETURN NUMBER
   AS
      l_code   err_cd.code%TYPE;
   BEGIN
      SELECT code
        INTO l_code
        FROM err_cd
       WHERE NAME = p_name;

      RETURN l_code;
   END get_err_cd;
   
   -- used to return a distinct error message text string by label
   FUNCTION get_err_msg( p_name VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_msg   err_cd.MESSAGE%TYPE;
   BEGIN
      SELECT MESSAGE
        INTO l_msg
        FROM err_cd
       WHERE NAME = p_name;

      RETURN l_msg;
   END get_err_msg;
   BEGIN
      -- go ahead and write the package_name as the default module
      set_default_module;
      
      -- set the default action name
      g_action := 'begin module';
      
END td_log;
/