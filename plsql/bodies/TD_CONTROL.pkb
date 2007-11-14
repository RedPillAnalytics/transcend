CREATE OR REPLACE PACKAGE BODY td_control
IS
   PROCEDURE check_module(
      p_module		VARCHAR2,
      p_allow_default	BOOLEAN DEFAULT false
   )
   IS
      l_package_name all_arguments.package_name%type;
   BEGIN
      
      BEGIN
	 SELECT package_name
	   INTO l_package_name
	   FROM all_arguments
	  WHERE lower(p_module) = lower(package_name||'.'||object_name);
      EXCEPTION
	 WHEN no_data_found
	 THEN
	 IF p_module = 'default'
	 THEN
	    NULL;
	 ELSE
	   raise_application_error( td_inst.get_err_cd( 'no_module' ),
                                    td_inst.get_err_msg( 'no_module' ) || ': ' || p_module
                                  );
	 END IF;
	 WHEN too_many_rows
	 THEN
	   NULL;
      END;
	 
      td_inst.log_msg('Check for package name '||l_package_name||' succeeded',4);

   END check_module;

   PROCEDURE set_logging_level(
      p_module          VARCHAR2 DEFAULT 'default',
      p_logging_level   NUMBER DEFAULT 2,
      p_debug_level     NUMBER DEFAULT 4
   )
   IS
   BEGIN
      check_module( p_module=> p_module,
		    p_allow_default => TRUE );
      UPDATE logging_conf
         SET logging_level = p_logging_level,
             debug_level = p_debug_level,
             modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
             modified_dt = SYSDATE
       WHERE module = p_module;
      
      IF SQL%ROWCOUNT = 0
      THEN
         INSERT INTO logging_conf
                     ( logging_id, logging_level, debug_level, module
                     )
              VALUES ( logging_conf_seq.NEXTVAL, p_logging_level, p_debug_level, p_module
                     );
      END IF;
   END set_logging_level;
   
   PROCEDURE set_runmode(
      p_module            VARCHAR2 DEFAULT 'default',
      p_default_runmode   VARCHAR2 DEFAULT 'runtime'
   )
   IS
   BEGIN
      check_module( p_module=> p_module,
		    p_allow_default => TRUE );

      UPDATE runmode_conf
         SET default_runmode = p_default_runmode,
             modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
             modified_dt = SYSDATE
       WHERE module = p_module;

      IF SQL%ROWCOUNT = 0
      THEN
         INSERT INTO runmode_conf
                     ( runmode_id, default_runmode, module
                     )
              VALUES ( runmode_conf_seq.NEXTVAL, p_default_runmode, p_module
                     );
      END IF;
   END set_runmode;

   PROCEDURE set_registration(
      p_module         VARCHAR2 DEFAULT 'default',
      p_registration   VARCHAR2 DEFAULT 'appinfo'
   )
   IS
   BEGIN
      check_module( p_module=> p_module,
		    p_allow_default => TRUE );

      UPDATE registration_conf
         SET registration = p_registration,
             modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
             modified_dt = SYSDATE
       WHERE module = p_module;

      IF SQL%ROWCOUNT = 0
      THEN
         INSERT INTO registration_conf
                     ( registration_id, registration, module
                     )
              VALUES ( registration_conf_seq.NEXTVAL, p_registration, p_module
                     );
      END IF;
   END set_registration;

   PROCEDURE add_notification_event(
      p_module		VARCHAR2,
      p_action 		VARCHAR2,
      p_subject		VARCHAR2,
      p_message         VARCHAR2
   )
   IS
   BEGIN
      -- check to make sure the module is an existing package
      check_module( p_module => p_module);      

      UPDATE notification_events
         SET subject = p_subject,
             message = p_message,
             modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
             modified_dt = SYSDATE
       WHERE module = lower(p_module)
	 AND action = lower(p_action);

      IF SQL%ROWCOUNT = 0
      THEN
         INSERT INTO notification_events
                     ( module, action, subject, message
                     )
              VALUES ( lower(p_module), lower(p_action), p_subject, p_message
                     );
      END IF;
   END add_notification_event;

   PROCEDURE add_notification(
      p_label		VARCHAR2,
      p_module		VARCHAR2,
      p_action 		VARCHAR2,
      p_method		VARCHAR2,
      p_enabled         VARCHAR2,
      p_required        VARCHAR2,
      p_sender		VARCHAR2,
      p_recipients	varchar2   
   )
   IS
   BEGIN
      check_module( p_module => p_module);      

      UPDATE notification_conf
         SET method = p_method,
             enabled = p_enabled,
	     required = p_required,
	     sender  = p_sender,
	     recipients = p_recipients,
             modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
             modified_dt = SYSDATE
       WHERE module = p_module
	 AND action = p_action;

      IF SQL%ROWCOUNT = 0
      THEN
         INSERT INTO notification_conf
                     ( label, module, action, method, enabled, required, sender, recipients
                     )
              VALUES ( p_label, p_module, p_action, p_method, p_enabled, p_required, p_sender, p_recipients
                     );
      END IF;
   END add_notification;

   PROCEDURE set_session_parameter( p_module VARCHAR2, p_name VARCHAR2, p_value VARCHAR2 )
   IS
      l_parameter   v$parameter.NAME%TYPE;
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
               raise_application_error( td_inst.get_err_cd( 'no_session_parm' ),
                                           td_inst.get_err_msg( 'no_session_parm' )
                                        || ': '
                                        || p_name
                                      );
            END IF;
      END;

      IF REGEXP_LIKE( p_name, 'disable|enable', 'i' )
      THEN
         UPDATE parameter_conf
            SET NAME = p_name,
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE module = p_module AND VALUE = p_value;
      ELSE
         UPDATE parameter_conf
            SET VALUE = p_value,
                modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
                modified_dt = SYSDATE
          WHERE module = p_module AND NAME = p_name;
      END IF;

      IF SQL%ROWCOUNT = 0
      THEN
         INSERT INTO parameter_conf
                     ( parameter_id, NAME, VALUE, module
                     )
              VALUES ( parameter_conf_seq.NEXTVAL, p_name, p_value, p_module
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
END td_control;
/