CREATE OR REPLACE TYPE BODY tdinc.applog
AS
   CONSTRUCTOR FUNCTION applog (
      p_action        VARCHAR2 DEFAULT 'Begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL,
      p_runmode       VARCHAR2 DEFAULT NULL)
      RETURN SELF AS RESULT
   AS
   BEGIN
      -- get the session id
      session_id := SYS_CONTEXT ('USERENV', 'SESSIONID');
      -- first we need to populate the module attribute, because it helps us determine parameter values
      module := p_module;
      -- we also set the action, which may be used one day to fine tune parameters
      action := p_action;
      -- now we can use the MODULE attribute to get parameters
      -- if this is null (unprovided), then the module or system parameter is pulled (in that order)
      -- the get_value_vchr function handles the hierarchy
      runmode :=
         CASE
            WHEN REGEXP_LIKE ('debug', '^' || NVL (p_runmode, '^\W$'), 'i')
               THEN 'debug'
            WHEN REGEXP_LIKE ('runtime', '^' || NVL (p_runmode, '^\W$'), 'i')
               THEN 'runtime'
            ELSE get_value_vchr ('runmode')
         END;
      -- get the registration value for this module
      registration := get_value_vchr ('registration');
      -- get the logging level
      logging_level := CASE
                         WHEN runmode = 'debug'
                            THEN 5
                         ELSE get_value_num ('logging_level')
                      END;

      -- if we are registering, then we need to save the old values
      IF SELF.is_registered
      THEN
         -- read previous app_info settings
         -- if not registering with oracle, then this is not necessary
         DBMS_APPLICATION_INFO.read_client_info (prev_client_info);
         DBMS_APPLICATION_INFO.read_module (prev_module, prev_action);
      END IF;

      -- populate attributes with new app_info settings
      client_info := NVL (p_client_info, prev_client_info);
      -- set other attributes
      instance_name := SYS_CONTEXT ('USERENV', 'INSTANCE_NAME');
      dbuser := SYS_CONTEXT ('USERENV', 'SESSION_USER');
      osuser := SYS_CONTEXT ('USERENV', 'OS_USER');
      machine :=
                SYS_CONTEXT ('USERENV', 'HOST') || '[' || SYS_CONTEXT ('USERENV', 'IP_ADDRESS')
                || ']';

      IF SELF.is_registered
      THEN
         -- now set the new values
         DBMS_APPLICATION_INFO.set_client_info (client_info);
         DBMS_APPLICATION_INFO.set_module (module, action);
      END IF;

      log_msg ('New MODULE ' || module || ' beginning in RUNMODE "' || runmode || '"', 4);
      log_msg ('Inital ACTION attribute set to "' || action || '"', 4);
      RETURN;
   END applog;
   -- used to pull the calling block from the dictionary
   -- used to populate CALL_STACK column in the LOG_TABLE
   MEMBER FUNCTION whence
      RETURN VARCHAR2
   AS
      l_call_stack    VARCHAR2 (4096) DEFAULT DBMS_UTILITY.format_call_stack || CHR (10);
      l_num           NUMBER;
      l_found_stack   BOOLEAN         DEFAULT FALSE;
      l_line          VARCHAR2 (255);
      l_cnt           NUMBER          := 0;
   BEGIN
      LOOP
         l_num := INSTR (l_call_stack, CHR (10));
         EXIT WHEN (l_cnt = 4 OR l_num IS NULL OR l_num = 0);
         l_line := SUBSTR (l_call_stack, 1, l_num - 1);
         l_call_stack := SUBSTR (l_call_stack, l_num + 1);

         IF (NOT l_found_stack)
         THEN
            IF (l_line LIKE '%handle%number%name%')
            THEN
               l_found_stack := TRUE;
            END IF;
         ELSE
            l_cnt := l_cnt + 1;
         END IF;
      END LOOP;

      RETURN l_line;
   END whence;
   MEMBER PROCEDURE set_action (p_action VARCHAR2)
   AS
   BEGIN
      action := p_action;
      log_msg ('ACTION attribute changed to "' || p_action || '"', 4);

      IF is_registered
      THEN
         DBMS_APPLICATION_INFO.set_action (p_action);
      END IF;
   END set_action;
   MEMBER PROCEDURE clear_app_info
   AS
   BEGIN
      IF is_registered
      THEN
         DBMS_APPLICATION_INFO.set_client_info (prev_client_info);
         DBMS_APPLICATION_INFO.set_module (prev_module, prev_action);
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_err;
         RAISE;
   END clear_app_info;
   -- used to write a standard message to the LOG_TABLE
   MEMBER PROCEDURE log_msg (
      p_msg      VARCHAR2,
      p_level    NUMBER DEFAULT 2,
      p_stdout   VARCHAR2 DEFAULT 'yes')
   -- P_MSG is simply the text that will be written to the LOG_TABLE
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_whence   VARCHAR2 (1024);
      l_code     NUMBER                        DEFAULT SQLCODE;
      l_msg      log_table.msg%TYPE;
      l_scn      v$database.current_scn%TYPE;
   BEGIN
      -- still write as much to the logfile if we can even if it's too large for the log table
      BEGIN
         l_msg := p_msg;
      EXCEPTION
         WHEN VALUE_ERROR
         THEN
            l_msg := SUBSTR (l_msg, 0, 1998) || '>>';
      END;

      -- find out what called me
      l_whence := whence;

      -- get the current_scn
      SELECT current_scn
        INTO l_scn
        FROM v$database;

      IF logging_level >= p_level
      THEN
         -- write the record to the log table
         INSERT INTO log_table
                     (msg,
                      client_info,
                      module,
                      action,
                      runmode,
                      session_id,
                      current_scn,
                      instance_name,
                      machine,
                      dbuser,
                      osuser,
                      code,
                      call_stack,
                      back_trace)
              VALUES (l_msg,
                      NVL (SELF.client_info, 'Not Set'),
                      NVL (SELF.module, 'Not Set'),
                      NVL (SELF.action, 'Not Set'),
                      SELF.runmode,
                      SELF.session_id,
                      l_scn,
                      SELF.instance_name,
                      SELF.machine,
                      SELF.dbuser,
                      SELF.osuser,
                      l_code,
                      l_whence,
                      REGEXP_REPLACE (SUBSTR (DBMS_UTILITY.format_error_backtrace, 1, 4000),
                                      '[[:cntrl:]]',
                                      '; '));

         COMMIT;

              -- also output the message to the screen
              -- the client can control whether or not they want to see this
              -- in sqlplus, just SET SERVEROUTPUT ON or OFF
         -- by default, all messages are logged to STDOUT
         -- this can be controlled per message with P_STDOUT, which defaults to 'yes'
         IF REGEXP_LIKE ('yes', p_stdout, 'i')
         THEN
            DBMS_OUTPUT.put_line (p_msg);
         END IF;
      END IF;
   END log_msg;
   MEMBER PROCEDURE log_err
   AS
      l_msg   VARCHAR2 (1020) DEFAULT SQLERRM;
   BEGIN
      log_msg (l_msg, 1, 'no');
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
   END log_err;
   MEMBER PROCEDURE log_cnt_msg (p_count NUMBER, p_msg VARCHAR2 DEFAULT NULL)
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      -- store in COUNT_TABLE numbers of records affected by particular actions in modules
      INSERT INTO count_table
                  (client_info,
                   module,
                   action,
                   runmode,
                   session_id,
                   row_cnt)
           VALUES (NVL (SELF.client_info, 'Not Set'),
                   NVL (SELF.module, 'Not Set'),
                   NVL (SELF.action, 'Not Set'),
                   SELF.runmode,
                   SELF.session_id,
                   p_count);

      -- if a message was provided to this procedure, then write it to the log table
      -- if not, then simply use the default message below
      log_msg (NVL (p_msg, 'Number of records selected/affected: ' || p_count));
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_err;
   END log_cnt_msg;
   -- method for returning boolean if the application is registered
   MEMBER FUNCTION is_registered
      RETURN BOOLEAN
   AS
   BEGIN
      RETURN CASE registration
         WHEN 'register'
            THEN TRUE
         ELSE FALSE
      END;
   END is_registered;
   -- GET method for pulling an error code out of the ERR_CD table
   MEMBER FUNCTION get_err_cd (p_name VARCHAR2)
      RETURN NUMBER
   AS
      l_code   err_cd.code%TYPE;
   BEGIN
      SELECT code
        INTO l_code
        FROM err_cd
       WHERE NAME = p_name;

      RETURN l_code;
   END get_err_cd;
   -- GET method for pulling error text out of the ERR_CD table
   MEMBER FUNCTION get_err_msg (p_name VARCHAR2)
      RETURN VARCHAR2
   AS
      l_msg   err_cd.MESSAGE%TYPE;
   BEGIN
      SELECT MESSAGE
        INTO l_msg
        FROM err_cd
       WHERE NAME = p_name;

      RETURN l_msg;
   END get_err_msg;
   MEMBER FUNCTION get_value_vchr (p_name VARCHAR2)
      RETURN VARCHAR2
   AS
      l_value   parameter_conf.VALUE%TYPE;
   BEGIN
      SELECT VALUE
        INTO l_value
        FROM parameter_conf
       WHERE NAME = p_name AND module = SELF.module;

      RETURN l_value;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         SELECT VALUE
           INTO l_value
           FROM parameter_conf
          WHERE NAME = p_name AND module = 'system';

         RETURN l_value;
   END get_value_vchr;
   MEMBER FUNCTION get_value_num (p_name VARCHAR2)
      RETURN NUMBER
   AS
      l_value   parameter_conf.VALUE%TYPE;
   BEGIN
      SELECT VALUE
        INTO l_value
        FROM parameter_conf
       WHERE NAME = p_name AND module = SELF.module;

      RETURN TO_NUMBER (l_value);
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         SELECT VALUE
           INTO l_value
           FROM parameter_conf
          WHERE NAME = p_name AND module = 'system';

         RETURN TO_NUMBER (l_value);
   END get_value_num;
END;
/