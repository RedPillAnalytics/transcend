CREATE OR REPLACE PACKAGE BODY control
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

   PROCEDURE set_session_parameter(
      p_module   VARCHAR2,
      p_name     VARCHAR2,
      p_value    VARCHAR2
   )
   IS
      l_parameter v$parameter.name%type;
   BEGIN
      BEGIN
	 SELECT name
	   INTO l_parameter
	   FROM v$parameter
	  WHERE name=p_name
	    AND isses_modifiable='TRUE';
      EXCEPTION
	 WHEN no_data_found
	 THEN
	   IF p_name = lower('enable')
	      THEN NULL;
	   ELSE
              raise_application_error( get_err_cd( 'no_session_parm' ),
                                       get_err_msg( 'no_session_parm' )||': '||p_name
                                     );
	   END IF;
      END;

      UPDATE parameter_conf
         SET value = p_value,
             modified_user = SYS_CONTEXT( 'USERENV', 'SESSION_USER' ),
             modified_dt = SYSDATE
       WHERE module = p_module
	 AND name = p_name;

      IF SQL%ROWCOUNT = 0
      THEN
         INSERT INTO parameter_conf
                ( parameter_id, name, value, module
                     )
		VALUES ( parameter_conf_seq.NEXTVAL, p_name, p_value, p_module
                     );
      END IF;
   END set_session_parameter;   

END control;
/