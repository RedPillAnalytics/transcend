CREATE OR REPLACE TYPE BODY evolve_ot
AS
   CONSTRUCTOR FUNCTION evolve_ot(
      p_action        VARCHAR2 DEFAULT 'begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL
   )
      RETURN SELF AS RESULT
   AS
      l_results         NUMBER;
      l_runmode         VARCHAR2( 10 );
      l_logging_level   NUMBER;
      l_registration    VARCHAR2( 30 );
      l_debug_level     NUMBER;
      l_parm_value      v$parameter.value%type;
      l_scn             v$database.current_scn%type;
      l_sql             VARCHAR2(4000);
   BEGIN
      -- read in all the previous values
      SELF.read_prev_info;
      -- first we need to populate the module attribute, because it helps us determine parameter values
      td_inst.module( LOWER( p_module ));
      -- we also set the action, which may be used one day to fine tune parameters
      td_inst.action( LOWER( p_action ));
      -- read previous app_info settings
      -- populate attributes with new app_info settings
      td_inst.client_info( NVL( p_client_info, td_inst.client_info ));

      -- READ CONFIGURATION TABLES TO PULL VALUES

      -- get the MODULE_CONF values
      BEGIN
         SELECT logging_level, debug_level, 
                CASE td_inst.runmode WHEN 'full debug' THEN 'full debug' ELSE LOWER( default_runmode ) end,
                LOWER( registration )
           INTO l_logging_level,
                l_debug_level,
                l_runmode,
                l_registration
           FROM ( SELECT logging_level,
                         debug_level,
                         default_runmode,
                         registration,
                         parameter_level,
                         MAX( parameter_level ) OVER( PARTITION BY 1 )
                                                                   max_parameter_level
                   FROM ( SELECT logging_level, debug_level,
                                 default_runmode, 
                                 registration,
                                 module,
                                 CASE
                                    WHEN module = evolve_adm.all_modules
                                       THEN 1
                                    ELSE 2
                                 END parameter_level
                           FROM module_conf )
                  WHERE ( module = td_inst.module OR module = evolve_adm.all_modules ))
          WHERE parameter_level = max_parameter_level;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
	    evolve.raise_err( 'parm_not_configured','MODULE_NAME' );
      END;
         
      -- set the environment attributs
      td_inst.runmode( l_runmode );
      td_inst.registration( l_registration );
      td_inst.REGISTER;
      
      -- now set the logging level
      -- this is determined by both LOGGING_LEVEL and RUNMODE
      td_inst.logging_level( CASE
                                WHEN evolve.is_debugmode
                                   THEN l_debug_level
                                ELSE l_logging_level
                             END
                           );

      -- log module and action changes at a high logging level
      -- this is level 4
      evolve.log_msg(    'MODULE "'
                          || td_inst.module
                          || '" beginning in RUNMODE "'
                          || td_inst.runmode
                          || '"',
                          4
                        );
      evolve.log_msg( 'Inital ACTION attribute set to "' || td_inst.action || '"', 4 );

      -- set session level parameters
      -- also have to allow for the session level parameters that don't have a value, but instead are simply "enabled" or "disabled"
      -- examples: parallel dml, resumable
      -- do this by setting the value for that parameter as either "enabled" or "disabled"
      FOR c_params IN
         ( SELECT CASE parameter_type
                  WHEN 'implicit' THEN 'alter session '||value||' '||name
                  WHEN 'explicit' THEN 'alter session set '||name||' = '||value
                  END ddl,
                  parameter_type,
                  name,
                  value 
             FROM 
                  ( SELECT CASE 
                           WHEN lower(value) IN ('enable','disable') THEN 'implicit'
                           ELSE 'explicit'
                           END parameter_type,
                           value,
                           name
                      FROM parameter_conf
                     WHERE lower( module ) IN (td_inst.module,evolve_adm.all_modules)
                  )
         )
      LOOP

         evolve.log_msg( 'Session SQL: ' || c_params.DDL, 4 );
         
         IF NOT evolve.is_debugmode
         THEN
            EXECUTE IMMEDIATE ( c_params.DDL );
         END IF;
         
         IF c_params.parameter_type = 'explicit'
         THEN

            l_sql := 'select value from v$parameter where lower(name) = lower('''||c_params.name||''')';
      
            BEGIN
               
               evolve.log_msg( 'SQL for pulling session parameter: '||l_sql, 5 );
               
               EXECUTE IMMEDIATE l_sql
               INTO l_parm_value;
               
            EXCEPTION
               WHEN others THEN NULL;

            END;
            
            evolve.log_msg( 'Value for session parameter '||c_params.name||' (only visible with SELECT on v$parameter): '||l_parm_value, 4 );
         END IF;
            
      END LOOP;

      -- now, set the starttime
      td_inst.starttime(SYSDATE);

      RETURN;
   END evolve_ot;
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

   MEMBER PROCEDURE read_prev_info
   AS
   BEGIN
      -- read in the previous values of all instrumentation attributes
      prev_action := td_inst.action;
      prev_module := td_inst.module;
      prev_client_info := td_inst.client_info;
      prev_registration := td_inst.registration;
      prev_logging_level := td_inst.logging_level;
      prev_batch_id := td_inst.batch_id;
      prev_runmode := td_inst.runmode;
   END read_prev_info;
   
   MEMBER PROCEDURE change_action( p_action VARCHAR2 )
   AS
   BEGIN
      td_inst.action( p_action );
      evolve.log_msg( 'ACTION attribute changed to "' || td_inst.action || '"', 4 );
      td_inst.REGISTER;
      -- now, set the starttime
      td_inst.starttime(SYSDATE);
   END change_action;
   
   MEMBER PROCEDURE clear_app_info
   AS
   BEGIN
      td_inst.client_info( prev_client_info );
      td_inst.module( prev_module );
      td_inst.action( prev_action );
      evolve.log_msg( 'MODULE attribute returning to "' || td_inst.module || '"', 4 );
      evolve.log_msg( 'ACTION attribute returning to "' || td_inst.action || '"', 4 );
      td_inst.REGISTER;
   END clear_app_info;

   MEMBER PROCEDURE send( p_label     VARCHAR2, 
			  p_message   VARCHAR2 DEFAULT NULL )
   AS
      o_notify notification_ot := notification_ot( p_label => p_label );
   BEGIN
      -- instantiate the notification object
      
      o_notify.send( p_message => p_message );
   END send;
END;
/

SHOW errors