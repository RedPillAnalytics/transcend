CREATE OR REPLACE PACKAGE BODY td_control
IS
   PROCEDURE set_logging_level(
      p_module          VARCHAR2 DEFAULT 'default',
      p_logging_level   NUMBER DEFAULT 2,
      p_debug_level     NUMBER DEFAULT 4
   )
   IS
   BEGIN
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
      p_registration   VARCHAR2 DEFAULT 'register'
   )
   IS
   BEGIN
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