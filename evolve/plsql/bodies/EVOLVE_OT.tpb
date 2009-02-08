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
         ( SELECT CASE
                     WHEN REGEXP_LIKE( name, 'enable', 'i' )
                        THEN 'alter session enable '||name
                     WHEN REGEXP_LIKE( name, 'disable', 'i' )
                        THEN 'alter session disable '||name
                     ELSE 'alter session set ' || name || '=' || value
                  END DDL
            FROM parameter_conf
            WHERE LOWER( module ) = td_inst.module OR module = evolve_adm.all_modules )
      LOOP
         IF evolve.is_debugmode
         THEN
            evolve.log_msg( 'Session SQL: ' || c_params.DDL );
         ELSE
            EXECUTE IMMEDIATE ( c_params.DDL );
         END IF;
      END LOOP;

      RETURN;
   END evolve_ot;
   
   OVERRIDING MEMBER PROCEDURE change_action( p_action VARCHAR2 )
   AS
   BEGIN
      td_inst.action( p_action );
      evolve.log_msg( 'ACTION attribute changed to "' || td_inst.action || '"', 4 );
      td_inst.REGISTER;
   END change_action;
   
   OVERRIDING MEMBER PROCEDURE clear_app_info
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