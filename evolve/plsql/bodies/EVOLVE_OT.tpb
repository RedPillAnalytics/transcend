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

      -- get the runmode value
      IF NOT td_inst.is_full_debugmode
      THEN
         BEGIN
            SELECT LOWER( default_runmode )
              INTO l_runmode
              FROM ( SELECT default_runmode, parameter_level,
                            MAX( parameter_level ) OVER( PARTITION BY 1 )
                                                                      max_parameter_level
                      FROM ( SELECT default_runmode, module,
                                    CASE
                                       WHEN module = 'default'
                                          THEN 1
                                       ELSE 2
                                    END parameter_level
                              FROM runmode_conf )
                     WHERE ( module = td_inst.module OR module = 'default' ))
             WHERE parameter_level = max_parameter_level;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
	       evolve_log.raise_err( 'parm_not_configured','RUNMODE' );
         END;

         td_inst.runmode( l_runmode );
      END IF;

      -- get the registration value for this module
      BEGIN
         SELECT LOWER( registration )
           INTO l_registration
           FROM ( SELECT registration, parameter_level,
                         MAX( parameter_level ) OVER( PARTITION BY 1 )
                                                                      max_parameter_level
                   FROM ( SELECT registration, module,
                                 CASE
                                    WHEN module = 'default'
                                       THEN 1
                                    ELSE 2
                                 END parameter_level
                           FROM registration_conf )
                  WHERE ( module = td_inst.module OR module = 'default' ))
          WHERE parameter_level = max_parameter_level;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
	    evolve_log.raise_err( 'parm_not_configured','REGISTRATION' );
      END;

      -- set the registration value
      td_inst.registration( l_registration );
      -- now register the application
      td_inst.REGISTER;

      -- get the logging level
      BEGIN
         SELECT LOWER( logging_level ), LOWER( debug_level )
           INTO l_logging_level, l_debug_level
           FROM ( SELECT logging_level, debug_level, parameter_level,
                         MAX( parameter_level ) OVER( PARTITION BY 1 )
                                                                      max_parameter_level
                   FROM ( SELECT logging_level, debug_level, module,
                                 CASE
                                    WHEN module = 'default'
                                       THEN 1
                                    ELSE 2
                                 END parameter_level
                           FROM logging_conf )
                  WHERE ( module = td_inst.module OR module = 'default' ))
          WHERE parameter_level = max_parameter_level;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
   	    evolve_log.raise_err( 'parm_not_configured','LOGGING_LEVEL or DEBUG_LEVEL' );
      END;

      td_inst.logging_level( CASE
                                WHEN evolve_log.is_debugmode
                                   THEN l_debug_level
                                ELSE l_logging_level
                             END
                           );
      -- log module and action changes to a high logging level
      evolve_log.log_msg(    'MODULE "'
                          || td_inst.module
                          || '" beginning in RUNMODE "'
                          || td_inst.runmode
                          || '"',
                          4
                        );
      evolve_log.log_msg( 'Inital ACTION attribute set to "' || td_inst.action || '"', 4 );

      -- set session level parameters
      FOR c_params IN
         ( SELECT CASE
                     WHEN REGEXP_LIKE( NAME, 'enable|disable', 'i' )
                        THEN 'alter session ' || NAME || ' ' || VALUE
                     ELSE 'alter session set ' || NAME || '=' || VALUE
                  END DDL
            FROM parameter_conf
           WHERE LOWER( module ) = td_inst.module )
      LOOP
         IF evolve_log.is_debugmode
         THEN
            evolve_log.log_msg( 'Session SQL: ' || c_params.DDL );
         ELSE
            EXECUTE IMMEDIATE ( c_params.DDL );
         END IF;
      END LOOP;

      RETURN;
   END evolve_ot;
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