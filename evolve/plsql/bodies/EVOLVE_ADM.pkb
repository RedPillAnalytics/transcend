CREATE OR REPLACE PACKAGE BODY evolve_adm
IS
   PROCEDURE set_logging_level(
      p_module          VARCHAR2 DEFAULT all_modules,
      p_logging_level   NUMBER DEFAULT 1,
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
	       evolve.raise_err( 'dup_conf' );
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
	 evolve.raise_err( 'no_rep_obj' );
      END IF;
   END set_logging_level;

   PROCEDURE set_runmode(
      p_module            VARCHAR2 DEFAULT all_modules,
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
	       evolve.raise_err( 'dup_conf' );
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
	 evolve.raise_err( 'no_rep_obj' );
      END IF;
   END set_runmode;

   PROCEDURE set_registration(
      p_module         VARCHAR2 DEFAULT all_modules,
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
 	       evolve.raise_err( 'dup_conf' );
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
 	 evolve.raise_err( 'no_rep_obj' );
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
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
      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         UPDATE notification_events
            SET subject = NVL( p_subject, subject ),
                MESSAGE = NVL( p_message, MESSAGE ),
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE module = LOWER( p_module ) AND action = LOWER( p_action );
      END IF;

      -- if the update was unsuccessful above, or an insert it specifically requested, then do an insert
      IF ( SQL%ROWCOUNT = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN
         evolve.log_msg( 'Update was unsuccessful or insert was specified', 5 );
	 
         CASE
            WHEN p_subject IS NULL
            THEN
               evolve.raise_err( 'parm_req', 'P_SUBJECT' );
            WHEN p_message IS NULL
            THEN
               evolve.raise_err( 'parm_req', 'P_MESSAGE' );
            ELSE
               NULL;
         END CASE;

         BEGIN
            INSERT INTO notification_events
                        ( module, action, subject, MESSAGE
                        )
                 VALUES ( LOWER( p_module ), LOWER( p_action ), p_subject, p_message
                        );
         EXCEPTION
            WHEN e_dup_conf
            THEN
               evolve.raise_err( 'dup_conf' );
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
	 evolve.raise_err( 'no_rep_obj' );
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

      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         UPDATE notification_conf
            SET method = NVL( p_method, method ),
                enabled = NVL( p_enabled, enabled ),
                required = NVL( p_required, required ),
                sender = NVL( p_sender, sender ),
                recipients = NVL( p_recipients, recipients ),
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE module = LOWER( p_module ) AND action = LOWER( p_action );
      END IF;

      -- if the update was unsuccessful above, or an insert it specifically requested, then do an insert
      IF ( SQL%ROWCOUNT = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN
         CASE
            WHEN p_method IS NULL
            THEN
               evolve.raise_err( 'parm_req', 'P_METHOD' );
            WHEN p_enabled IS NULL
            THEN
               evolve.raise_err( 'parm_req', 'P_ENABLED' );
            WHEN p_required IS NULL
            THEN
               evolve.raise_err( 'parm_req', 'P_REQUIRED' );
            WHEN p_sender IS NULL
            THEN
               evolve.raise_err( 'parm_req', 'P_SENDER' );
            WHEN p_recipients IS NULL
            THEN
               evolve.raise_err( 'parm_req', 'P_RECIPIENTS' );
            ELSE
               NULL;
         END CASE;

         BEGIN
            INSERT INTO notification_conf
                        ( label, module, action, method,
                          enabled, required, sender, recipients
                        )
                 VALUES ( LOWER( p_label ), LOWER( p_module ), LOWER( p_action ), LOWER( p_method ),
                          LOWER( p_enabled ), LOWER( p_required ), LOWER( p_sender ), LOWER( p_recipients )
                        );
         EXCEPTION
            WHEN e_dup_conf
            THEN
               evolve.raise_err( 'dup_conf' );
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
	 evolve.raise_err( 'no_rep_obj' );
      END IF;
   END set_notification;

   PROCEDURE set_error_conf(
      p_name       VARCHAR2 DEFAULT NULL,
      p_message    VARCHAR2 DEFAULT NULL,
      p_comments   VARCHAR2 DEFAULT NULL,
      p_mode       VARCHAR2 DEFAULT 'upsert'
   )
   IS
      l_code       error_conf.code%TYPE;
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN

      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         UPDATE error_conf
            SET NAME = NVL( LOWER( p_name ), NAME ),
                MESSAGE = NVL( p_message, MESSAGE ),
                comments = NVL( p_comments, comments ),
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE LOWER( NAME ) = LOWER( p_name ) OR LOWER( MESSAGE ) = LOWER( p_message );
      END IF;

      -- if the update was unsuccessful above, or an insert is specifically requested, then do an insert
      IF ( SQL%ROWCOUNT = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN
         CASE
            WHEN p_name IS NULL
            THEN
               evolve.raise_err( 'parm_req', 'P_NAME' );
            WHEN p_message IS NULL
            THEN
               evolve.raise_err( 'parm_req', 'P_MESSAGE' );
            ELSE
               NULL;
         END CASE;

         -- error_codes for RAISE_APPLICATON_ERROR are a scarce resource
         -- need to use them carefully, and reuse wherever possible

         -- first, try and use 20101 (that is the lowest used for ERROR_CONF)
         -- if that is taken, then use the lowest gap number
         -- otherwise, use the max number +1
         BEGIN
            SELECT DISTINCT MIN( CASE
                                    WHEN min_code > 20101
                                       THEN 20101
                                    WHEN code + 1 <> lead_code
                                       THEN code + 1
                                    ELSE max_code + 1
                                 END
                               ) OVER( PARTITION BY 1 )
                       INTO l_code
                       FROM ( SELECT code, LEAD( code ) OVER( ORDER BY code ) lead_code,
                                     MIN( code ) OVER( PARTITION BY 1 ) min_code,
                                     MAX( code ) OVER( PARTITION BY 1 ) max_code
                               FROM error_conf );
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_code := 20101;
         END;

         BEGIN
            INSERT INTO error_conf
                        ( NAME, MESSAGE, code, comments
                        )
                 VALUES ( LOWER( p_name ), p_message, l_code, p_comments
                        );
         EXCEPTION
            WHEN e_dup_conf
            THEN
               evolve.raise_err( 'dup_conf' );
         END;
      END IF;

      -- if a delete is specifically requested, then do a delete
      IF LOWER( p_mode ) = 'delete'
      THEN
         DELETE FROM error_conf
               WHERE LOWER( NAME ) = LOWER( p_name ) OR LOWER( MESSAGE ) = LOWER( MESSAGE );
      END IF;

      -- if we still have not affected any records, then there's a problem
      IF SQL%ROWCOUNT = 0
      THEN
	 evolve.raise_err( 'no_rep_obj' );
      END IF;
   END set_error_conf;

   PROCEDURE set_session_parameter(
      p_name     VARCHAR2,
      p_value    VARCHAR2,
      p_module   VARCHAR2 DEFAULT all_modules,
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
            IF REGEXP_LIKE( p_value, 'enable|disable', 'i' )
            THEN
               NULL;
            ELSE
	       evolve.raise_err( 'no_db_parm',p_name);
            END IF;
      END;

      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         UPDATE parameter_conf
            SET VALUE = LOWER( p_value ),
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE module = LOWER( p_module ) AND NAME = LOWER( p_name );
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
               evolve.raise_err( 'dup_conf' );
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
	 evolve.raise_err( 'no_rep_obj' );
      END IF;
   END set_session_parameter;


   PROCEDURE set_method_conf(
      p_method_name     VARCHAR2,
      p_method_command  NUMBER,
      p_mode            VARCHAR2 DEFAULT 'upsert'
   )
   IS
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN
      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         UPDATE method_conf
            SET method_command = p_method_command,
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE lower( method_name ) = LOWER( p_method_name );
      END IF;

      -- if the update was unsuccessful above, or an insert it specifically requested, then do an insert
      IF ( SQL%ROWCOUNT = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN
         BEGIN
            INSERT INTO method_conf
                        ( method_name, method_command
                        )
                 VALUES ( lower( p_method_name ), p_method_command
                        );
         EXCEPTION
            WHEN e_dup_conf
            THEN
	       evolve.raise_err( 'dup_conf' );
         END;
      END IF;

      -- if a delete is specifically requested, then do a delete
      IF LOWER( p_mode ) = 'delete'
      THEN
         DELETE FROM method_conf
               WHERE lower( method_name ) = LOWER( p_method_name );
      END IF;

      -- if we still have not affected any records, then there's a problem
      IF SQL%ROWCOUNT = 0
      THEN
	 evolve.raise_err( 'no_rep_obj' );
      END IF;
   END set_method_conf;


   PROCEDURE set_default_configs( p_config VARCHAR2 DEFAULT 'all', p_reset VARCHAR2 DEFAULT 'no' )
   IS
   BEGIN
      -- reset logging_level
      IF LOWER( p_config ) IN( 'all', 'logging_level' )
      THEN
         IF td_core.is_true( p_reset )
         THEN
            DELETE FROM logging_conf;
         END IF;

         set_logging_level;
      END IF;

      -- reset runmode
      IF LOWER( p_config ) IN( 'all', 'runmode' )
      THEN
         IF td_core.is_true( p_reset )
         THEN
            DELETE FROM runmode_conf;
         END IF;

         set_runmode;
      END IF;

      -- reset registration
      IF LOWER( p_config ) IN( 'all', 'registration' )
      THEN
         IF td_core.is_true( p_reset )
         THEN
            DELETE FROM registration_conf;
         END IF;

         set_registration;
      END IF;
      
      -- set default notification events
      IF LOWER( p_config ) IN( 'all', 'notification' )
      THEN
         IF td_core.is_true( p_reset )
         THEN
            DELETE FROM notification_conf;
         END IF;
         
         -- configure notification event for the support functionality
         evolve_adm.set_notification_event
         ( 'evolve_adm',
           'send support dump',
           'support dump',
           'The attached support dump is sent from:'
         );

      END IF;


      -- set the default method paths
      IF LOWER( p_config ) IN( 'all', 'methods' )
      THEN
         IF td_core.is_true( p_reset )
         THEN
            DELETE FROM method_conf;
         END IF;
         
         -- configure execution methods
         set_method_conf
         (gzip_method,
           'gzip -df'
         );

         set_method_conf
         (zip_method,
           'unzip'
         );

         set_method_conf
         (compress_method,
           'uncompress'
         );

         set_method_conf
         (bzip2_method,
           'bunzip2'
         );

         set_method_conf
         (gpg_method,
           'gpg'
         );

      END IF;


      -- reset error_conf
      IF LOWER( p_config ) IN( 'all', 'errors' )
      THEN
         IF td_core.is_true( p_reset )
         THEN
            DELETE FROM error_conf;
         END IF;

         set_error_conf( p_name         => 'dup_conf',
                         p_message      => 'An attempt was made to add a duplicate configuration'
                       );
         set_error_conf( p_name         => 'no_rep_obj',
                         p_message      => 'This action affected no repository configurations'
                       );
         set_error_conf( p_name         => 'parm_req',
                         p_message      => 'Creating a new configuration requires the specified parameter'
                       );
         set_error_conf( p_name         => 'unrecognized_parm',
                         p_message      => 'The specified parameter value is not recognized'
                       );
         set_error_conf( p_name         => 'notify_method_invalid',
                         p_message      => 'The notification method is not valid'
                       );
         set_error_conf( p_name => 'no_tab', p_message => 'The specified table does not exist' );
         set_error_conf( p_name => 'no_object', p_message => 'The specified object does not exist' );
         set_error_conf( p_name         => 'no_dir_obj',
                         p_message      => 'The specified directory object does not exist' );
         set_error_conf( p_name         => 'no_dir_path',
                         p_message      => 'There is no directory object defined for the specififed path'
                       );
         set_error_conf
                      ( p_name         => 'too_many_dirs',
                        p_message      => 'There is more than one directory object defined for the specififed path'
                      );
         set_error_conf( p_name         => 'not_partitioned',
                         p_message      => 'The specified table is not partititoned' );
         set_error_conf( p_name         => 'parms_not_compatible',
                         p_message      => 'The specified parameters are not compatible'
                       );
         set_error_conf( p_name         => 'parm_not_configured',
                         p_message      => 'The specified parameter is not configured'
                       );
         set_error_conf( p_name => 'file_not_found', p_message => 'The specified file does not exist' );
         set_error_conf( p_name => 'not_iot', p_message => 'The specified table is not index-organized' );
         set_error_conf( p_name         => 'not_external',
                         p_message      => 'The specified table is not an external table' );
         set_error_conf( p_name => 'external', p_message => 'The specified table is an external table' );
         set_error_conf( p_name => 'not_compressed', p_message => 'The specified segment is not compresed' );
         set_error_conf( p_name => 'no_part', p_message => 'The specified partition does not exist' );
         set_error_conf( p_name => 'partitioned', p_message => 'The specified table is partitioned' );
         set_error_conf( p_name => 'iot', p_message => 'The specified table is index-organized' );
         set_error_conf( p_name => 'compressed', p_message => 'The specified segment is compresed' );
         set_error_conf( p_name => 'no_pk', p_message => 'The specified table has no primary key' );
         set_error_conf( p_name => 'no_column', p_message => 'The specified column does not exist or cannot be seen' );
         set_error_conf( p_name         => 'no_or_wrong_object',
                         p_message      => 'The specified object does not exist or is of the wrong type'
                       );
         set_error_conf( p_name         => 'too_many_objects',
                         p_message      => 'The specified parameters yield more than one object'
                       );
         set_error_conf( p_name         => 'parm_not_supported',
                         p_message      => 'The specified parameter is not supported'
                       );
         set_error_conf( p_name         => 'no_db_parm',
                         p_message      => 'The specified parameter name is not a recognized database parameter'
                       );
         set_error_conf
            ( p_name         => 'submit_sql',
              p_message      => 'Errors were generated by a process submitted to the Oracle scheduler. See the scheduler logs for details.'
            );
         set_error_conf
            ( p_name         => 'submit_sql_timeout',
              p_message      => 'The execution of a job submitted through the Oracle scheduler ran longer than the provided timeout'
            );
         set_error_conf( p_name         => 'host_cmd',
                         p_message      => 'Java Error: method hostCmd made unsuccessful system calls'
                       );
         set_error_conf( p_name         => 'copy_file',
                         p_message      => 'Java Error: method copyFile was unable to copy' );
         set_error_conf( p_name => 'utl_mail_err', p_message => 'Fatal UTL_MAIL error occured' );
         set_error_conf( p_name => 'invalid_compress_method', p_message => 'The specified compression method is not valid' );
         set_error_conf( p_name => 'invalid_encrypt_method', p_message => 'The specified encryption method is not valid' );
      END IF;
   END set_default_configs;

   PROCEDURE clear_log(
      p_runmode      VARCHAR2 DEFAULT NULL,
      p_session_id   NUMBER DEFAULT SYS_CONTEXT( 'USERENV', 'SESSIONID' )
   )
   AS
   BEGIN
      DELETE FROM log_table
            WHERE session_id = p_session_id AND REGEXP_LIKE( runmode, NVL( p_runmode, '.' ), 'i' );
   END clear_log;
END evolve_adm;
/

SHOW errors