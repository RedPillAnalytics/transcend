CREATE OR REPLACE PACKAGE BODY evolve_adm
IS
   PROCEDURE set_logging_level(
      p_module          VARCHAR2 DEFAULT 'default',
      p_logging_level   NUMBER DEFAULT 2,
      p_debug_level     NUMBER DEFAULT 3,
      p_mode            VARCHAR2 DEFAULT 'upsert'
   )
   IS
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN
      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         UPDATE logging_conf
            SET logging_level = p_logging_level,
                debug_level = p_debug_level,
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE module = LOWER( p_module );
      END IF;

      -- if the update was unsuccessful above, or an insert it specifically requested, then do an insert
      IF ( SQL%ROWCOUNT = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN
         BEGIN
            INSERT INTO logging_conf
                        ( logging_level, debug_level, module
                        )
                 VALUES ( p_logging_level, p_debug_level, LOWER( p_module )
                        );
         EXCEPTION
            WHEN e_dup_conf
            THEN
               raise_application_error
                                 ( -20011,
                                   'An attempt was made to add a duplicate configuration'
                                 );
         END;
      END IF;

      -- if a delete is specifically requested, then do a delete
      IF LOWER( p_mode ) = 'delete'
      THEN
         DELETE FROM logging_conf
               WHERE module = LOWER( p_module );
      END IF;

      -- if we still have not affected any records, then there's a problem
      IF SQL%ROWCOUNT = 0
      THEN
         raise_application_error( -20013,
                                  'This action affected no repository configurations'
                                );
      END IF;
   END set_logging_level;

   PROCEDURE set_runmode(
      p_module            VARCHAR2 DEFAULT 'default',
      p_default_runmode   VARCHAR2 DEFAULT 'runtime',
      p_mode              VARCHAR2 DEFAULT 'upsert'
   )
   IS
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN
      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         UPDATE runmode_conf
            SET default_runmode = LOWER( p_default_runmode ),
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE module = LOWER( p_module );
      END IF;

      -- if the update was unsuccessful above, or an insert it specifically requested, then do an insert
      IF ( SQL%ROWCOUNT = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN
         BEGIN
            INSERT INTO runmode_conf
                        ( default_runmode, module
                        )
                 VALUES ( LOWER( p_default_runmode ), LOWER( p_module )
                        );
         EXCEPTION
            WHEN e_dup_conf
            THEN
               raise_application_error
                                 ( -20011,
                                   'An attempt was made to add a duplicate configuration'
                                 );
         END;
      END IF;

      -- if a delete is specifically requested, then do a delete
      IF LOWER( p_mode ) = 'delete'
      THEN
         DELETE FROM runmode_conf
               WHERE module = LOWER( p_module );
      END IF;

      -- if we still have not affected any records, then there's a problem
      IF SQL%ROWCOUNT = 0
      THEN
         raise_application_error( -20013,
                                  'This action affected no repository configurations'
                                );
      END IF;
   END set_runmode;

   PROCEDURE set_registration(
      p_module         VARCHAR2 DEFAULT 'default',
      p_registration   VARCHAR2 DEFAULT 'appinfo',
      p_mode           VARCHAR2 DEFAULT 'upsert'
   )
   IS
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN
      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         UPDATE registration_conf
            SET registration = LOWER( p_registration ),
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE module = LOWER( p_module );
      END IF;

      -- if the update was unsuccessful above, or an insert it specifically requested, then do an insert
      IF ( SQL%ROWCOUNT = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN
         BEGIN
            INSERT INTO registration_conf
                        ( registration, module
                        )
                 VALUES ( LOWER( p_registration ), LOWER( p_module )
                        );
         EXCEPTION
            WHEN e_dup_conf
            THEN
               raise_application_error
                                 ( -20011,
                                   'An attempt was made to add a duplicate configuration'
                                 );
         END;
      END IF;

      -- if a delete is specifically requested, then do a delete
      IF LOWER( p_mode ) = 'delete'
      THEN
         DELETE FROM registration_conf
               WHERE module = LOWER( p_module );
      END IF;

      -- if we still have not affected any records, then there's a problem
      IF SQL%ROWCOUNT = 0
      THEN
         raise_application_error( -20013,
                                  'This action affected no repository configurations'
                                );
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
         RAISE;
   END set_registration;

   PROCEDURE set_notification_event(
      p_module    VARCHAR2,
      p_action    VARCHAR2,
      p_subject   VARCHAR2 DEFAULT NULL,
      p_message   VARCHAR2 DEFAULT NULL,
      p_mode      VARCHAR2 DEFAULT 'upsert'
   )
   IS
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN
      CASE
         WHEN p_mode = 'insert' AND (p_subject IS NULL OR p_message IS NULL)
         THEN
           raise_application_error(-20014, 'An insert requires a value for all parameters');
      ELSE
         NULL;
      END CASE;

      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         UPDATE notification_events
            SET subject = nvl( p_subject, subject ),
                MESSAGE = nvl( p_message, message ),
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE module = LOWER( p_module ) AND action = LOWER( p_action );
      END IF;

      -- if the update was unsuccessful above, or an insert it specifically requested, then do an insert
      IF ( SQL%ROWCOUNT = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN
         evolve_log.log_msg( 'Update was unsuccessful or insert was specified', 5 );

         BEGIN
            INSERT INTO notification_events
                        ( module, action, subject, MESSAGE
                        )
                 VALUES ( LOWER( p_module ), LOWER( p_action ), p_subject, p_message
                        );
         EXCEPTION
            WHEN e_dup_conf
            THEN
               raise_application_error
                                 ( -20011,
                                   'An attempt was made to add a duplicate configuration'
                                 );
         END;
      END IF;

      -- if a delete is specifically requested, then do a delete
      IF LOWER( p_mode ) = 'delete'
      THEN
         DELETE FROM notification_events
               WHERE module = LOWER( p_module ) AND action = LOWER( p_action );
      END IF;

      -- if we still have not affected any records, then there's a problem
      IF SQL%ROWCOUNT = 0
      THEN
         raise_application_error( -20013,
                                  'This action affected no repository configurations'
                                );
      END IF;
   END set_notification_event;

   PROCEDURE set_notification(
      p_label        VARCHAR2,
      p_module       VARCHAR2,
      p_action       VARCHAR2,
      p_method       VARCHAR2 DEFAULT NULL,
      p_enabled      VARCHAR2 DEFAULT NULL,
      p_required     VARCHAR2 DEFAULT NULL,
      p_sender       VARCHAR2 DEFAULT NULL,
      p_recipients   VARCHAR2 DEFAULT NULL,
      p_mode         VARCHAR2 DEFAULT 'upsert'
   )
   IS
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN
      CASE
         WHEN p_mode = 'insert' AND ( p_method IS NULL OR p_enabled IS NULL OR p_required IS NULL OR p_sender IS NULL OR p_recipients IS null)
         THEN
           raise_application_error(-20014, 'An insert requires a value for all parameters');
      ELSE
         NULL;
      END CASE;

      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         UPDATE notification_conf
            SET method = nvl(p_method,method),
                enabled = nvl(p_enabled,enabled),
                required = nvl(p_required,required),
                sender = nvl(p_sender,sender),
                recipients = nvl(p_recipients,recipients),
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE module = LOWER( p_module ) AND action = LOWER( p_action );
      END IF;

      -- if the update was unsuccessful above, or an insert it specifically requested, then do an insert
      IF ( SQL%ROWCOUNT = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN
         BEGIN
            INSERT INTO notification_conf
                        ( label, module, action,
                          method, enabled, required,
                          sender, recipients
                        )
                 VALUES ( LOWER( p_label ), LOWER( p_module ), LOWER( p_action ),
                          LOWER( p_method ), LOWER( p_enabled ), LOWER( p_required ),
                          LOWER( p_sender ), LOWER( p_recipients )
                        );
         EXCEPTION
            WHEN e_dup_conf
            THEN
               raise_application_error
                                 ( -20011,
                                   'An attempt was made to add a duplicate configuration'
                                 );
         END;
      END IF;

      -- if a delete is specifically requested, then do a delete
      IF LOWER( p_mode ) = 'delete'
      THEN
         DELETE FROM notification_events
               WHERE module = LOWER( p_module ) AND action = LOWER( p_action );
      END IF;

      -- if we still have not affected any records, then there's a problem
      IF SQL%ROWCOUNT = 0
      THEN
         raise_application_error( -20013,
                                  'This action affected no repository configurations'
                                );
      END IF;
   END set_notification;

   PROCEDURE set_error_conf(
      p_name         VARCHAR2 DEFAULT NULL,
      p_message      NUMBER   DEFAULT NULL,
      p_comment	     VARCHAR2 DEFAULT NULL,
      p_mode         VARCHAR2 DEFAULT 'upsert'
   )
   IS
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN
      CASE
         WHEN p_mode = 'insert' AND ( p_name IS NULL OR p_message IS NULL)
         THEN
           raise_application_error(-20014, 'An insert requires a value for all parameters except P_COMMENT');
      ELSE
         NULL;
      END CASE;

      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         UPDATE error_conf
            SET name = nvl(lower(p_name),name),
                message = nvl(p_message,message),
		COMMENT = nvl(p_comment,COMMENT),
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE lower(name) = LOWER( p_name ) OR lower(message) = lower( p_message );
      END IF;

      -- if the update was unsuccessful above, or an insert is specifically requested, then do an insert
      IF ( SQL%ROWCOUNT = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN
         BEGIN
            INSERT INTO error_conf
                        ( name, message, code, comment
                        )
                   VALUES ( lower(p_name), p_message, error_conf_code_seq.nextval, comment
                        );
         EXCEPTION
            WHEN e_dup_conf
            THEN
               raise_application_error
                                 ( -20011,
                                   'An attempt was made to add a duplicate configuration'
                                 );
         END;
      END IF;

      -- if a delete is specifically requested, then do a delete
      IF LOWER( p_mode ) = 'delete'
      THEN
         DELETE FROM error_conf
          WHERE lower(name) = LOWER( p_name )
	     OR lower(message) = lower(message);
      END IF;

      -- if we still have not affected any records, then there's a problem
      IF SQL%ROWCOUNT = 0
      THEN
         raise_application_error( -20013,
                                  'This action affected no repository configurations'
                                );
      END IF;
   END set_error_conf;

   PROCEDURE set_session_parameter(
      p_module   VARCHAR2,
      p_name     VARCHAR2,
      p_value    VARCHAR2,
      p_mode     VARCHAR2 DEFAULT 'upsert'
   )
   IS
      l_parameter   v$parameter.NAME%TYPE;
      e_dup_conf    EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN
      BEGIN
         SELECT NAME
           INTO l_parameter
           FROM v$parameter
          WHERE NAME = p_name AND isses_modifiable = 'TRUE';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            IF REGEXP_LIKE( p_name, 'enable|disable', 'i' )
            THEN
               NULL;
            ELSE
               raise_application_error
                  ( -20014,
                       'The specified parameter name is not a recognized database parameter: '
                    || p_name
                  );
            END IF;
      END;

      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         IF REGEXP_LIKE( p_name, 'disable|enable', 'i' )
         THEN
            UPDATE parameter_conf
               SET NAME = LOWER( p_name ),
                   modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                   modified_dt = SYSDATE
             WHERE module = LOWER( p_module ) AND VALUE = LOWER( p_value );
         ELSE
            UPDATE parameter_conf
               SET VALUE = LOWER( p_value ),
                   modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                   modified_dt = SYSDATE
             WHERE module = LOWER( p_module ) AND NAME = LOWER( p_name );
         END IF;
      END IF;

      -- if the update was unsuccessful above, or an insert it specifically requested, then do an insert
      IF ( SQL%ROWCOUNT = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN
         BEGIN
            INSERT INTO parameter_conf
                        ( NAME, VALUE, module
                        )
                 VALUES ( LOWER( p_name ), LOWER( p_value ), LOWER( p_module )
                        );
         EXCEPTION
            WHEN e_dup_conf
            THEN
               raise_application_error
                                 ( -20011,
                                   'An attempt was made to add a duplicate configuration'
                                 );
         END;
      END IF;

      -- if a delete is specifically requested, then do a delete
      IF LOWER( p_mode ) = 'delete'
      THEN
         DELETE FROM parameter_conf
               WHERE module = LOWER( p_module ) AND NAME = LOWER( p_name );
      END IF;

      -- if we still have not affected any records, then there's a problem
      IF SQL%ROWCOUNT = 0
      THEN
         raise_application_error( -20013,
                                  'This action affected no repository configurations'
                                );
      END IF;
   END set_session_parameter;   

   PROCEDURE set_default_configs(
      p_config   VARCHAR2 DEFAULT 'all',
      p_reset	 VARCHAR2 DEFAULT 'no'
   )
   IS
   BEGIN
      -- reset logging_level
      IF lower(p_config) IN ('all','logging_level')
      THEN
	 
	 IF td_core.is_true(p_reset)
	 THEN
	    DELETE FROM logging_level;
	 END IF;

	 evolve_adm.set_logging_level;
      END IF;

      -- reset runmode
      IF lower(p_config) IN ('all','runmode')
      THEN
	 IF td_core.is_true(p_reset)
	 THEN
	    DELETE FROM runmode;
	 END IF;

	 evolve_adm.set_runmode;
      END IF;

      -- reset registration
      IF lower(p_config) IN ('all','registration')
      THEN
	 
	 IF td_core.is_true(p_reset)
	 THEN
	    DELETE FROM registration;
	 END IF;

	 evolve_adm.set_registration;
      END IF;

      -- reset error_conf
      IF lower(p_config) IN ('all','errors')
      THEN
	 
	 IF td_core.is_true(p_reset)
	 THEN
	    DELETE FROM error_conf;
	 END IF;

	 
	 set_error_conf( p_name=> 'unrecognized_parm',
			 p_code=> 'The specified parameter value is not recognized');
	 set_error_conf( p_name=> 'notify_method_invalid',
			 p_code=> 'The notification method is not valid');
	 set_error_conf( p_name=> 'no_tab',
			 p_code=> 'The specified table does not exist');
	 set_error_conf( p_name=> 'no_object',
			 p_code=> 'The specified object does not exist');
	 set_error_conf( p_name=> 'not_partitioned',
			 p_code=> 'The specified table is not partititoned');
	 set_error_conf( p_name=> 'parms_not_compatible',
			 p_code=> 'The specified parameters are not compatible');
	 set_error_conf( p_name=> 'parm_not_configured',
			 p_code=> 'The specified parameter is not configured');
	 set_error_conf( p_name=> 'file_not_found',
			 p_code=> 'Expected file does not exist');
	 set_error_conf( p_name=> 'not_iot',
			 p_code=> 'The specified table is not index-organized');
	 set_error_conf( p_name=> 'not_compressed',
			 p_code=> 'The specified segment is not compresed');
	 set_error_conf( p_name=> 'no_part',
			 p_code=> 'The specified partition does not exist');
	 set_error_conf( p_name=> 'partitioned',
			 p_code=> 'The specified table is partitioned');
	 set_error_conf( p_name=> 'iot',
			 p_code=> 'The specified table is index-organized');
	 set_error_conf( p_name=> 'compressed',
			 p_code=> 'The specified segment is compresed');
	 set_error_conf( p_name=> 'no_or_wrong_object',
			 p_code=> 'The specified object does not exist or is of the wrong type');
	 set_error_conf( p_name=> 'too_many_objects',
			 p_code=> 'The specified parameters yield more than one object');
	 set_error_conf( p_name=> 'parm_not_supported',
			 p_code=> 'The specified parameter is not supported');	 

      END IF;

   END reset_default_configs;

   PROCEDURE clear_log(
      p_runmode      VARCHAR2 DEFAULT NULL,
      p_session_id   NUMBER DEFAULT SYS_CONTEXT( 'USERENV', 'SESSIONID' )
   )
   AS
   BEGIN
      DELETE FROM log_table
            WHERE session_id = p_session_id
              AND REGEXP_LIKE( runmode, NVL( p_runmode, '.' ), 'i' );
   END clear_log;

END evolve_adm;
/

SHOW errors