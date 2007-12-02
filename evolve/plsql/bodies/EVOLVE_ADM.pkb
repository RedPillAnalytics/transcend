CREATE OR REPLACE PACKAGE BODY evolve_adm
IS
   PROCEDURE set_logging_level(
      p_module          VARCHAR2 DEFAULT 'default',
      p_logging_level   NUMBER DEFAULT 2,
      p_debug_level     NUMBER DEFAULT 4,
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
      p_subject   VARCHAR2,
      p_message   VARCHAR2,
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
            SET subject = p_subject,
                MESSAGE = p_message,
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
      p_method       VARCHAR2,
      p_enabled      VARCHAR2,
      p_required     VARCHAR2,
      p_sender       VARCHAR2,
      p_recipients   VARCHAR2,
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
            SET method = p_method,
                enabled = p_enabled,
                required = p_required,
                sender = p_sender,
                recipients = p_recipients,
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