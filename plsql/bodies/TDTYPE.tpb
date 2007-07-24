CREATE OR REPLACE TYPE BODY tdtype
AS
   CONSTRUCTOR FUNCTION tdtype(
      p_action        VARCHAR2 DEFAULT 'begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL,
      p_runmode       VARCHAR2 DEFAULT NULL
   )
      RETURN SELF AS RESULT
   AS
      l_results   NUMBER;
BEGIN
       -- get information about the session for logging purposes
       self.set_session_info;
       -- first we need to populate the module attribute, because it helps us determine parameter values
       SELF.set_module( p_module );
       -- we also set the action, which may be used one day to fine tune parameters
       SELF.set_action( p_action );
       -- now we can use the MODULE attribute to get the runmode
       SELF.set_runmode( p_runmode );

      -- get the registration value for this module
      BEGIN
         SELECT LOWER( REGISTER )
           INTO REGISTER
           FROM ( SELECT REGISTER, parameter_level,
                         MAX( parameter_level ) OVER( PARTITION BY 1 )
                                                                      max_parameter_level
                   FROM ( SELECT REGISTER, module,
                                 CASE
                                    WHEN module = 'default'
                                       THEN 1
                                    ELSE 2
                                 END parameter_level
                           FROM registration_conf )
                  WHERE ( module = SELF.module OR module = 'default' ))
          WHERE parameter_level = max_parameter_level;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            raise_application_error( td_ext.get_err_cd( 'parm_not_configured' ),
                                     td_ext.get_err_msg( 'parm_not_configured' ) || ': REGISTER'
                                   );
      END;

      -- get the logging level
      IF SELF.is_debugmode
      THEN
         BEGIN
            SELECT LOWER( debug_level )
              INTO logging_level
              FROM ( SELECT debug_level, parameter_level,
                            MAX( parameter_level ) OVER( PARTITION BY 1 )
                                                                      max_parameter_level
                      FROM ( SELECT debug_level, module,
                                    CASE
                                       WHEN module = 'default'
                                          THEN 1
                                       ELSE 2
                                    END parameter_level
                              FROM logging_conf )
                     WHERE ( module = SELF.module OR module = 'default' ))
             WHERE parameter_level = max_parameter_level;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               raise_application_error( td_ext.get_err_cd( 'parm_not_configured' ),
                                           td_ext.get_err_msg( 'parm_not_configured' )
                                        || ': DEBUG_LEVEL'
                                      );
         END;
      ELSE
         BEGIN
            SELECT LOWER( logging_level )
              INTO logging_level
              FROM ( SELECT logging_level, parameter_level,
                            MAX( parameter_level ) OVER( PARTITION BY 1 )
                                                                      max_parameter_level
                      FROM ( SELECT logging_level, module,
                                    CASE
                                       WHEN module = 'default'
                                          THEN 1
                                       ELSE 2
                                    END parameter_level
                              FROM logging_conf )
                     WHERE ( module = SELF.module OR module = 'default' ))
             WHERE parameter_level = max_parameter_level;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               raise_application_error( td_ext.get_err_cd( 'parm_not_configured' ),
                                           td_ext.get_err_msg( 'parm_not_configured' )
                                        || ': LOGGING_LEVEL'
                                      );
         END;
      END IF;

      -- if we are registering, then we need to save the old values
      IF SELF.REGISTER = 'yes'
      THEN
         -- read previous app_info settings
         -- if not registering with oracle, then this is not necessary
         DBMS_APPLICATION_INFO.read_client_info( prev_client_info );
         DBMS_APPLICATION_INFO.read_module( prev_module, prev_action );
      END IF;

      IF SELF.REGISTER = 'yes'
      THEN
         -- now set the new values
         DBMS_APPLICATION_INFO.set_client_info( client_info );
         DBMS_APPLICATION_INFO.set_module( module, action );
      END IF;
      -- populate attributes with new app_info settings
      client_info := NVL( p_client_info, prev_client_info );

      SELF.log_msg( 'MODULE "' || module || '" beginning in RUNMODE "' || runmode || '"',
                    4
                  );
      SELF.log_msg( 'Inital ACTION attribute set to "' || action || '"', 4 );

      -- set session level parameters
      FOR c_params IN
         ( SELECT CASE
                     WHEN REGEXP_LIKE( NAME, 'enable|disable', 'i' )
                        THEN 'alter session ' || NAME || ' ' || VALUE
                     ELSE 'alter session set ' || NAME || '=' || VALUE
                  END DDL
            FROM parameter_conf
           WHERE LOWER( module ) = SELF.module )
      LOOP
         IF NOT SELF.is_debugmode
         THEN
            EXECUTE IMMEDIATE c_params.DDL;
         END IF;

         SELF.log_msg( 'SESSION DDL: ' || c_params.DDL, 3 );
      END LOOP;

      RETURN;
   END tdtype;
   OVERRIDING MEMBER PROCEDURE change_action( p_action VARCHAR2 )
   AS
   BEGIN
      SELF.set_action( p_action );
      SELF.log_msg( 'ACTION attribute changed to "' || action || '"', 4 );

      IF REGISTER = 'yes'
      THEN
         -- set the action attribute for DBMS_APPLICATION_INFO
         DBMS_APPLICATION_INFO.set_action( action );
      END IF;
   END change_action;
   OVERRIDING MEMBER PROCEDURE clear_app_info
   AS
   BEGIN
      action := prev_action;
      module := prev_module;
      client_info := prev_client_info;
      SELF.log_msg( 'ACTION attribute changed to "' || action || '"', 4 );
      SELF.log_msg( 'MODULE attribute changed to "' || module || '"', 4 );

      IF REGISTER = 'yes'
      THEN
         DBMS_APPLICATION_INFO.set_client_info( prev_client_info );
         DBMS_APPLICATION_INFO.set_module( prev_module, prev_action );
      END IF;
   END clear_app_info;
   MEMBER PROCEDURE send( p_module_id NUMBER, p_message VARCHAR2 DEFAULT NULL )
   AS
      l_notify_method   notify_conf.notify_method%TYPE;
      l_notify_id       notify_conf.notify_id%TYPE;
      o_email           email;
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
               WHERE ( module = SELF.module )
                 AND ( action = SELF.action OR action IS NULL )
                 AND ( module_id = p_module_id OR module_id IS NULL ))
       WHERE parameter_level = max_parameter_level;

      CASE l_notify_method
         WHEN 'email'
         THEN
            SELECT VALUE( t )
              INTO o_email
              FROM email_ot t
             WHERE t.notify_id = l_notify_id;

            o_email.runmode := runmode;
            o_email.MESSAGE :=
               CASE p_message
                  WHEN NULL
                     THEN o_email.MESSAGE
                  ELSE o_email.MESSAGE || CHR( 10 ) || CHR( 10 ) || p_message
               END;
            o_email.module := SELF.module;
            o_email.action := SELF.action;
            o_email.send;
         ELSE
            raise_application_error( td_ext.get_err_cd( 'notify_method_invalid' ),
                                     td_ext.get_err_msg( 'notify_method_invalid' )
                                   );
      END CASE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         SELF.log_msg( 'Notification not configured for this action', 3 );
   END send;
END;
/