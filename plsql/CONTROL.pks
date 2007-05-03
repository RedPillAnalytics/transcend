CREATE OR REPLACE PACKAGE tdinc.control
IS
   PROCEDURE set_logging_level (
      p_module          VARCHAR2 DEFAULT 'default',
      p_action          VARCHAR2 DEFAULT 'default',
      p_logging_level   NUMBER DEFAULT 2,
      p_debug_level     NUMBER DEFAULT 4);

   PROCEDURE set_runmode (
      p_module            VARCHAR2 DEFAULT 'default',
      p_action            VARCHAR2 DEFAULT 'default',
      p_default_runmode   VARCHAR2 DEFAULT 'runtime');

   PROCEDURE set_registration (
      p_module         VARCHAR2 DEFAULT 'default',
      p_action         VARCHAR2 DEFAULT 'default',
				p_registration   VARCHAR2 DEFAULT 'register');
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
      p_reject_limit       NUMBER DEFAULT NULL);

END control;
/