CREATE OR REPLACE TYPE BODY tdtype
AS
   CONSTRUCTOR FUNCTION tdtype(
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
      td_inst.module( LOWER( CASE
                                WHEN     REGEXP_LIKE( SELF.get_package_name,
                                                      'anonymous block',
                                                      'i'
                                                    )
                                     AND p_module IS NOT NULL
                                   THEN p_module
                                WHEN p_module IS NULL
                                   THEN SELF.get_package_name
                                ELSE SELF.get_package_name || '.' || p_module
                             END
                           )
                    );
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
               raise_application_error( td_inst.get_err_cd( 'parm_not_configured' ),
                                           td_inst.get_err_msg( 'parm_not_configured' )
                                        || ': RUNMODE'
                                      );
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
         raise_application_error( td_inst.get_err_cd( 'parm_not_configured' ),
                                  td_inst.get_err_msg( 'parm_not_configured' )
                                  || ': REGISTRATION'
                                );
      END;
      -- set the registration value
      td_inst.registration( l_registration );
      -- now register the application
      td_inst.register;

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
         raise_application_error( td_inst.get_err_cd( 'parm_not_configured' ),
                                  td_inst.get_err_msg( 'parm_not_configured' )
                                  || ': LOGGING_LEVEL or DEBUG_LEVEL'
                                );
      END;

      td_inst.logging_level( CASE
                             WHEN td_inst.is_debugmode
                             THEN l_debug_level
                             ELSE l_logging_level
                             END
                           );

      -- log module and action changes to a high logging level
      td_inst.log_msg(    'MODULE "'
                       || td_inst.module
                       || '" beginning in RUNMODE "'
                       || td_inst.runmode
                       || '"',
                       4
                     );
      td_inst.log_msg( 'Inital ACTION attribute set to "' || td_inst.action || '"', 4 );

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
         IF td_inst.is_debugmode
         THEN
            td_inst.log_msg( 'Session SQL: ' || c_params.DDL );
         ELSE
            EXECUTE IMMEDIATE ( c_params.DDL );
         END IF;
      END LOOP;

      RETURN;
   END tdtype;

   MEMBER PROCEDURE send( p_module_id NUMBER, p_message VARCHAR2 DEFAULT NULL )
   AS
      l_notify_method   notify_conf.notify_method%TYPE;
      l_notify_id       notify_conf.notify_id%TYPE;
      o_email           emailtype;
   BEGIN
      SELECT notify_method, notify_id
        INTO l_notify_method, l_notify_id
        FROM ( SELECT notify_method, notify_id, parameter_level,
                      MAX( parameter_level ) OVER( PARTITION BY 1 ) max_parameter_level
                FROM ( SELECT notify_method, notify_id, module, module_id, action,
                              CASE
                                 WHEN action IS NULL
                                      AND module_id IS NULL
                                    THEN 1
                                 WHEN action IS NULL
                                 AND module_id IS NOT NULL
                                    THEN 2
                                 WHEN action IS NOT NULL AND module_id IS NULL
                                    THEN 3
                                 WHEN action IS NOT NULL AND module_id IS NOT NULL
                                    THEN 4
                              END parameter_level
                        FROM notify_conf )
               WHERE ( module = td_inst.module )
                 AND ( action = td_inst.action OR action IS NULL )
                 AND ( module_id = p_module_id OR module_id IS NULL ))
       WHERE parameter_level = max_parameter_level;

      CASE l_notify_method
         WHEN 'email'
         THEN
            SELECT VALUE( t )
              INTO o_email
              FROM email_ot t
             WHERE t.notify_id = l_notify_id;

            o_email.MESSAGE :=
               CASE p_message
                  WHEN NULL
                     THEN o_email.MESSAGE
                  ELSE o_email.MESSAGE || CHR( 10 ) || CHR( 10 ) || p_message
               END;
            o_email.module := td_inst.module;
            o_email.action := td_inst.action;
            o_email.send;
         ELSE
            raise_application_error( td_inst.get_err_cd( 'notify_method_invalid' ),
                                     td_inst.get_err_msg( 'notify_method_invalid' )
                                   );
      END CASE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         td_inst.log_msg( 'Notification not configured for this action', 3 );
   END send;
END;
/