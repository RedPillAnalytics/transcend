CREATE OR REPLACE PACKAGE evolve_adm AUTHID CURRENT_USER
IS

   all_modules	CONSTANT VARCHAR2(13) := '*all_modules*';

   null_value   CONSTANT VARCHAR2 (6) := '*null*';

   PROCEDURE set_module_conf(
      p_module          VARCHAR2 DEFAULT all_modules,
      p_logging_level   NUMBER   DEFAULT 1,
      p_debug_level     NUMBER   DEFAULT 3,
      p_default_runmode VARCHAR2 DEFAULT 'runtime',
      p_registration    VARCHAR2 DEFAULT 'appinfo',
      p_consistent_name VARCHAR2 DEFAULT 'no',
      p_mode            VARCHAR2 DEFAULT 'upsert'
   );

   PROCEDURE set_notification_event(
      p_module		VARCHAR2,
      p_action 		VARCHAR2,
      p_subject		VARCHAR2 DEFAULT NULL,
      p_message         VARCHAR2 DEFAULT NULL,
      p_mode		VARCHAR2 DEFAULT 'upsert'
   );

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
   );

   PROCEDURE set_error_conf(
      p_name         VARCHAR2 DEFAULT NULL,
      p_message      VARCHAR2 DEFAULT NULL,
      p_comments     VARCHAR2 DEFAULT NULL,
      p_mode         VARCHAR2 DEFAULT 'upsert'
   );

   PROCEDURE set_session_parameter(
      p_name         VARCHAR2,
      p_value        VARCHAR2,
      p_module       VARCHAR2 DEFAULT all_modules,
      p_mode	     VARCHAR2 DEFAULT 'upsert'
   );

   PROCEDURE set_command_conf(
      p_name     VARCHAR2,
      p_value    VARCHAR2 DEFAULT NULL,
      p_path     VARCHAR2 DEFAULT NULL,
      p_flags    VARCHAR2 DEFAULT NULL,
      p_mode     VARCHAR2 DEFAULT 'upsert'
   );

   PROCEDURE set_default_configs(
      p_config   VARCHAR2 DEFAULT 'all',
      p_reset	 VARCHAR2 DEFAULT 'no'
   );

   PROCEDURE clear_log(
      p_runmode      VARCHAR2 DEFAULT NULL,
      p_session_id   NUMBER DEFAULT SYS_CONTEXT( 'USERENV', 'SESSIONID' )
   );
END evolve_adm;
/