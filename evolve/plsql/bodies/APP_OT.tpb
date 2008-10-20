CREATE OR REPLACE TYPE BODY app_ot
AS
   CONSTRUCTOR FUNCTION app_ot(
      p_action        VARCHAR2 DEFAULT 'begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL
   )
      RETURN SELF AS RESULT
   AS
      l_results   NUMBER;
   BEGIN
      -- read in all the previous values
      read_prev_info;
      -- first we need to populate the module attribute, because it helps us determine parameter values
      td_inst.module( LOWER( p_module ));
      -- we also set the action, which may be used one day to fine tune parameters
      td_inst.action( LOWER( p_action ));
      -- read previous app_info settings
      -- populate attributes with new app_info settings
      td_inst.client_info( NVL( p_client_info, td_inst.client_info ));
      -- now register the above values
      td_inst.register;
      RETURN;
   END app_ot;
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

   MEMBER PROCEDURE change_action( p_action VARCHAR2 )
   AS
   BEGIN
      td_inst.action( p_action );
      td_inst.REGISTER;
   END change_action;

   MEMBER PROCEDURE clear_app_info
   AS
   BEGIN
      td_inst.client_info( prev_client_info );
      td_inst.module( prev_module );
      td_inst.action( prev_action );
      td_inst.REGISTER;
   END clear_app_info;
   
   MEMBER PROCEDURE read_prev_info
   AS
   BEGIN
      -- read in the previous values of all instrumentation attributes
      prev_action := td_inst.action;
      prev_module := td_inst.module;
      prev_client_info := td_inst.client_info;
      prev_registration := td_inst.registration;
      prev_logging_level := td_inst.logging_level;
      prev_consistent_name := td_inst.consistent_name;
      prev_batch_id := td_inst.batch_id;
      prev_runmode := td_inst.runmode;
   END read_prev_info;
END;
/

SHOW errors