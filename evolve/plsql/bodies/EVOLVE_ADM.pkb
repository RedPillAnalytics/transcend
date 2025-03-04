CREATE OR REPLACE PACKAGE BODY evolve_adm
IS
   PROCEDURE set_module_conf(
      p_module          VARCHAR2 DEFAULT all_modules,
      p_logging_level   NUMBER   DEFAULT 2,
      p_debug_level     NUMBER   DEFAULT 4,
      p_default_runmode VARCHAR2 DEFAULT 'runtime',
      p_registration    VARCHAR2 DEFAULT 'appinfo',
      p_mode            VARCHAR2 DEFAULT 'upsert'
   )
   IS
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN
      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         UPDATE module_conf
            SET logging_level   = p_logging_level,
                debug_level     = p_debug_level,
                default_runmode = lower( p_default_runmode ),
                registration    = lower( p_registration ),
                modified_user   = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt     = SYSDATE
          WHERE module          = LOWER( p_module );
      END IF;

      -- if the update was unsuccessful above, or an insert it specifically requested, then do an insert
      IF ( SQL%ROWCOUNT = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN
         BEGIN
            INSERT INTO module_conf
                        ( logging_level, debug_level, 
                          default_runmode, registration, 
                          module
                        )
                 VALUES ( p_logging_level, p_debug_level,
                          lower( p_default_runmode ), lower( p_registration ),
                          LOWER( p_module )
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
         DELETE FROM module_conf
               WHERE module = LOWER( p_module );
      END IF;

      -- if we still have not affected any records, then there's a problem
      IF SQL%ROWCOUNT = 0
      THEN
         evolve.raise_err( 'no_rep_obj' );
      END IF;
   END set_module_conf;

   PROCEDURE set_notification_event(
      p_event_name   VARCHAR2,
      p_module       VARCHAR2 DEFAULT NULL,
      p_action       VARCHAR2 DEFAULT NULL,
      p_subject      VARCHAR2 DEFAULT NULL,
      p_message      VARCHAR2 DEFAULT NULL,
      p_mode         VARCHAR2 DEFAULT 'upsert'
   )
   IS
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN
      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         UPDATE notification_event
            SET module        = lower( nvl( p_module, module ) ),
                action        = lower( nvl( p_action, action ) ),
                subject       = NVL( p_subject, subject ),
                MESSAGE       = NVL( p_message, MESSAGE ),
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt   = SYSDATE
          WHERE lower( event_name )  = LOWER( p_event_name );
      END IF;

      -- if the update was unsuccessful above, or an insert it specifically requested, then do an insert
      IF ( SQL%ROWCOUNT = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN
         evolve.log_msg( 'Update was unsuccessful or insert was specified', 5 );
	 
         CASE
            WHEN p_module IS NULL
            THEN
               evolve.raise_err( 'parm_req', 'P_MODULE' );
            WHEN p_action IS NULL
            THEN
               evolve.raise_err( 'parm_req', 'P_ACTION' );
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
            INSERT INTO notification_event
                        ( event_name, module, action, subject, MESSAGE
                        )
                 VALUES ( LOWER( p_event_name ), LOWER( p_module ), LOWER( p_action ), p_subject, p_message
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
         DELETE FROM notification_event
               WHERE event_name = LOWER( p_event_name );
      END IF;

      -- if we still have not affected any records, then there's a problem
      IF SQL%ROWCOUNT = 0
      THEN
	 evolve.raise_err( 'no_rep_obj' );
      END IF;
   END set_notification_event;

   PROCEDURE set_notification(
      p_label        VARCHAR2,
      p_event_name   VARCHAR2,
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
          WHERE lower( event_name ) = LOWER( p_event_name ) AND lower( label ) = lower( p_label );
      END IF;

      -- if the update was unsuccessful above, or an insert it specifically requested, then do an insert
      IF ( SQL%ROWCOUNT = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN
         CASE
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
                        ( label, event_name, method,
                          enabled, required, sender, recipients
                        )
                 VALUES ( LOWER( p_label ), LOWER( p_event_name ), LOWER( nvl( p_method, 'email') ),
                          LOWER( nvl( p_enabled, 'yes' ) ), LOWER( nvl( p_required, 'no' ) ), LOWER( p_sender ), LOWER( p_recipients )
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
         DELETE FROM notification_conf
               WHERE lower( event_name ) = LOWER( p_event_name )
                 AND lower( label ) = lower( p_label );
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


   PROCEDURE set_command_conf(
      p_name     VARCHAR2,
      p_value    VARCHAR2 DEFAULT NULL,
      p_path     VARCHAR2 DEFAULT NULL,
      p_flags    VARCHAR2 DEFAULT NULL,
      p_mode     VARCHAR2 DEFAULT 'upsert'
   )
   IS
      e_dup_conf   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_dup_conf, -1 );
   BEGIN
      -- this is the default method... update if it exists or insert it
      IF LOWER( p_mode ) IN( 'upsert', 'update' )
      THEN
         UPDATE command_conf
            SET value = CASE WHEN p_value = null_value THEN NULL WHEN p_value IS NULL THEN value ELSE p_value end,
                path = CASE WHEN p_path = null_value THEN NULL WHEN p_path IS NULL THEN path ELSE p_path end,
                flags = CASE WHEN p_flags = null_value THEN NULL WHEN p_flags IS NULL THEN flags ELSE p_flags end,
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE lower( name ) = LOWER( p_name );
      END IF;

      -- if the update was unsuccessful above, or an insert it specifically requested, then do an insert
      IF ( SQL%ROWCOUNT = 0 AND LOWER( p_mode ) = 'upsert' ) OR LOWER( p_mode ) = 'insert'
      THEN

         -- p_value is required for inserts
         IF p_value IS NULL
         THEN
            evolve.raise_err( 'parm_req', 'P_vALUE' );
         END IF;
         
         BEGIN
            INSERT INTO command_conf
                        ( name, value, path, flags
                        )
                 VALUES ( lower( p_name ), p_value, p_path, p_flags
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
         DELETE FROM command_conf
               WHERE lower( name ) = LOWER( p_name );
      END IF;

      -- if we still have not affected any records, then there's a problem
      IF SQL%ROWCOUNT = 0
      THEN
	 evolve.raise_err( 'no_rep_obj' );
      END IF;
   END set_command_conf;

   PROCEDURE set_default_configs( p_config VARCHAR2 DEFAULT 'all', p_reset VARCHAR2 DEFAULT 'no' )
   IS
   BEGIN
      -- reset logging_level
      IF LOWER( p_config ) IN( 'all', 'module' )
      THEN
         IF td_core.is_true( p_reset )
         THEN
            DELETE FROM module_conf;
         END IF;

         set_module_conf;
      END IF;
      
      -- set default notification events
      IF LOWER( p_config ) IN( 'all', 'notification' )
      THEN
         IF td_core.is_true( p_reset )
         THEN
            DELETE FROM notification_conf;
         END IF;
         
         -- configure notification event for the support functionality
         set_notification_event
         ( 'dump evolve session',
           'evolve.dump_session',
           'notify support',
           'Evolve session dump information',
           'The attached support dump is sent from '
         );

      END IF;

      -- set the default command values
      IF LOWER( p_config ) IN( 'all', 'commands' )
      THEN
         IF td_core.is_true( p_reset )
         THEN
            DELETE FROM command_conf;
         END IF;
         
         -- configure execution commands

         -- command for extracting a gzip archive
         set_command_conf
         ( p_name  => 'gunzip',
           p_value => 'gzip',
           p_path  => NULL,
           p_flags => '-df'
         );
         
         -- command for extracting a zip archive
         set_command_conf
         ( p_name  => 'unzip',
           p_value => 'unzip',
           p_path  => NULL,
           p_flags => '-u'
         );
         
         -- command for extracting a .Z archive
         set_command_conf
         ( p_name  => 'uncompress',
           p_value => 'uncompress',
           p_path  => NULL,
           p_flags => '-f'
         );
         
         -- command for extracting a bzip2 archive
         set_command_conf
         ( p_name  => 'bunzip',
           p_value => 'bzip2',
           p_path  => NULL,
           p_flags => '-df'
         );
         
         -- command for extracting a decrypting a GPG file
         set_command_conf
         ( p_name  => 'gpg_decrypt',
           p_value => 'gpg',
           p_path  => NULL,
           p_flags => '--no-tty --passphrase-fd 0 --batch --decrypt --output'
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
         set_error_conf( p_name => 'no_ind', p_message => 'The specified index does not exist' );
         set_error_conf( p_name => 'no_segment', p_message => 'The specified segment does not exist' );
         set_error_conf( p_name => 'multiple_segments', p_message => 'The specified segment name references more than one segment' );
         set_error_conf( p_name => 'no_object', p_message => 'The specified object does not exist' );
         set_error_conf( p_name => 'seg_not_supported', p_message => 'The segment type of the specified segment is not supported' );
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
                         p_message      => 'The specified table or index is not partititoned' );
         set_error_conf( p_name         => 'not_subpartitioned',
                         p_message      => 'The specified table or index is not subpartititoned' );
         set_error_conf( p_name         => 'parms_not_compatible',
                         p_message      => 'The specified parameters are not compatible'
                       );
         set_error_conf( p_name         => 'group_parms',
                         p_message      => 'The specified parameters are mutually inclusive'
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
         set_error_conf( p_name => 'no_part', p_message => 'The specified partition or subpartition does not exist' );
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
         set_error_conf( p_name => 'invalid_command', p_message => 'The specified command name does not match a configured value in the COMMAND_CONF table' );
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