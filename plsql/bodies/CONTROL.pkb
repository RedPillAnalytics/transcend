CREATE OR REPLACE PACKAGE BODY tdinc.control
IS
   PROCEDURE set_logging_level (
      p_module          VARCHAR2 DEFAULT 'default',
      p_action          VARCHAR2 DEFAULT 'default',
      p_logging_level   NUMBER DEFAULT 2,
      p_debug_level     NUMBER DEFAULT 4)
   IS
   BEGIN
      UPDATE logging_conf
         SET logging_level = p_logging_level,
             debug_level = p_debug_level,
             modified_user = SYS_CONTEXT ('USERENV', 'SESSION_USER'),
             modified_dt = SYSDATE
       WHERE module = p_module AND action = p_action;

      IF SQL%ROWCOUNT = 0
      THEN
         INSERT INTO logging_conf
                     (logging_id,
                      logging_level,
                      debug_level,
                      module,
                      action)
              VALUES (logging_conf_seq.NEXTVAL,
                      p_logging_level,
                      p_debug_level,
                      p_module,
                      p_action);
      END IF;
   END set_logging_level;

   PROCEDURE set_runmode (
      p_module            VARCHAR2 DEFAULT 'default',
      p_action            VARCHAR2 DEFAULT 'default',
      p_default_runmode   VARCHAR2 DEFAULT 'runtime')
   IS
   BEGIN
      UPDATE runmode_conf
         SET default_runmode = p_default_runmode,
             modified_user = SYS_CONTEXT ('USERENV', 'SESSION_USER'),
             modified_dt = SYSDATE
       WHERE module = p_module AND action = p_action;

      IF SQL%ROWCOUNT = 0
      THEN
         INSERT INTO runmode_conf
                     (runmode_id,
                      default_runmode,
                      module,
                      action)
              VALUES (runmode_conf_seq.NEXTVAL,
                      p_default_runmode,
                      p_module,
                      p_action);
      END IF;
   END set_runmode;

   PROCEDURE set_registration (
      p_module         VARCHAR2 DEFAULT 'default',
      p_action         VARCHAR2 DEFAULT 'default',
      p_registration   VARCHAR2 DEFAULT 'register')
   IS
   BEGIN
      UPDATE registration_conf
         SET registration = p_registration,
             modified_user = SYS_CONTEXT ('USERENV', 'SESSION_USER'),
             modified_dt = SYSDATE
       WHERE module = p_module AND action = p_action;

      IF SQL%ROWCOUNT = 0
      THEN
         INSERT INTO registration_conf
                     (registration_id,
                      registration,
                      module,
                      action)
              VALUES (registration_conf_seq.NEXTVAL,
                      p_registration,
                      p_module,
                      p_action);
      END IF;
   END set_registration;

   PROCEDURE feed_config (
      p_filehub_name       VARCHAR2,
      p_filehub_group      VARCHAR2,
      p_object_owner       VARCHAR2 DEFAULT NULL,
      p_object_name        VARCHAR2 DEFAULT NULL,
      p_directory          VARCHAR2 DEFAULT NULL,
      p_filename           VARCHAR2 DEFAULT NULL,
      p_arch_directory     VARCHAR2 DEFAULT NULL,
      p_arch_filename      VARCHAR2 DEFAULT NULL,
      p_file_datestamp     VARCHAR2 DEFAULT NULL,
      p_min_bytes          NUMBER DEFAULT NULL,
      p_max_bytes          NUMBER DEFAULT NULL,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_passphrase         VARCHAR2 DEFAULT NULL,
      p_source_directory   VARCHAR2 DEFAULT NULL,
      p_source_regexp      VARCHAR2 DEFAULT NULL,
      p_regexp_options     VARCHAR2 DEFAULT NULL,
      p_source_policy      VARCHAR2 DEFAULT NULL,
      p_required           VARCHAR2 DEFAULT NULL,
      p_reject_limit       NUMBER DEFAULT NULL)
   IS
   BEGIN
      UPDATE registration_conf
         SET registration = p_registration,
             modified_user = SYS_CONTEXT ('USERENV', 'SESSION_USER'),
             modified_dt = SYSDATE
       WHERE module = p_module AND action = p_action;

      IF SQL%ROWCOUNT = 0
      THEN
         INSERT INTO registration_conf
                     (registration_id,
                      registration,
                      module,
                      action)
              VALUES (registration_conf_seq.NEXTVAL,
                      p_registration,
                      p_module,
                      p_action);
      END IF;
   END feed_config;
END control;
/