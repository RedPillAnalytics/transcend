CREATE OR REPLACE TYPE BODY apptype
AS
   CONSTRUCTOR FUNCTION apptype(
      p_action         VARCHAR2 DEFAULT 'begin module',
      p_module         VARCHAR2 DEFAULT NULL,
      p_client_info    VARCHAR2 DEFAULT NULL,
      p_registration   VARCHAR2 DEFAULT 'register',
      p_runmode        VARCHAR2 DEFAULT 'runtime'
   )
      RETURN SELF AS RESULT
   AS
      l_results   NUMBER;
   BEGIN
      -- first we need to populate the module attribute, because it helps us determine parameter values
      module :=
         LOWER( CASE
                   WHEN p_module IS NULL
                      THEN get_package_name
                   ELSE get_package_name || '.' || p_module
                END
              );
      -- we also set the action, which may be used one day to fine tune parameters
      action := LOWER( p_action );
      -- now set the runmode
      set_runmode( p_runmode );
      -- get the registration value for this module
      set_registration( p_registration );

      -- if we are registering, then we need to save the old values
      IF SELF.is_registered
      THEN
         -- read previous app_info settings
         -- if not registering with oracle, then this is not necessary
         DBMS_APPLICATION_INFO.read_client_info( prev_client_info );
         DBMS_APPLICATION_INFO.read_module( prev_module, prev_action );
      END IF;

      -- populate attributes with new app_info settings
      client_info := NVL( p_client_info, prev_client_info );

      IF SELF.is_registered
      THEN
         -- now set the new values
         DBMS_APPLICATION_INFO.set_client_info( client_info );
         DBMS_APPLICATION_INFO.set_module( module, action );
      END IF;

      RETURN;
   END apptype;
   MEMBER PROCEDURE set_action( p_action VARCHAR2 )
   AS
   BEGIN
      action := LOWER( p_action );

      IF is_registered
      THEN
         DBMS_APPLICATION_INFO.set_action( action );
      END IF;
   END set_action;
   MEMBER PROCEDURE clear_app_info
   AS
   BEGIN
      action := prev_action;
      module := prev_module;
      client_info := prev_client_info;

      IF is_registered
      THEN
         DBMS_APPLICATION_INFO.set_client_info( prev_client_info );
         DBMS_APPLICATION_INFO.set_module( prev_module, prev_action );
      END IF;
   END clear_app_info;
   -- SET method for RUNMODE
   MEMBER PROCEDURE set_runmode( p_runmode VARCHAR2 )
   AS
   BEGIN
      CASE
         WHEN REGEXP_LIKE( 'debug', '^' || NVL( p_runmode, '^\W$' ), 'i' )
         THEN
            runmode := 'debug';
         WHEN REGEXP_LIKE( 'runtime', '^' || NVL( p_runmode, '^\W$' ), 'i' )
         THEN
            runmode := 'runtime';
         ELSE
            raise_application_error
                                ( -20022,
                                     'The specified parameter value is not recognized : '
                                  || p_runmode
                                );
      END CASE;
   END set_runmode;
   -- SET method for REGISTRATION
   MEMBER PROCEDURE set_registration( p_registration VARCHAR2 )
   AS
   BEGIN
      CASE
         WHEN REGEXP_LIKE( 'register', '^' || NVL( p_registration, '^\W$' ), 'i' )
         THEN
            registration := 'register';
         WHEN REGEXP_LIKE( 'noregister', '^' || NVL( p_registration, '^\W$' ), 'i' )
         THEN
            registration := 'noregister';
         ELSE
            raise_application_error
                                ( -20022,
                                     'The specified parameter value is not recognized : '
                                  || p_registration
                                );
      END CASE;
   END set_registration;
   -- method for returning boolean if the application is registered
   MEMBER FUNCTION is_registered
      RETURN BOOLEAN
   AS
   BEGIN
      RETURN CASE registration
         WHEN 'register'
            THEN TRUE
         ELSE FALSE
      END;
   END is_registered;
   -- gets the package name
   MEMBER FUNCTION get_package_name
      RETURN VARCHAR2
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

      RETURN LOWER( l_name );
   END get_package_name;
   -- GET method for DEBUG mode
   MEMBER FUNCTION is_debugmode
      RETURN BOOLEAN
   AS
   BEGIN
      RETURN CASE runmode
         WHEN 'debug'
            THEN TRUE
         ELSE FALSE
      END;
   END is_debugmode;
END;
/