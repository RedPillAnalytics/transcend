CREATE OR REPLACE PACKAGE td_control AUTHID CURRENT_USER
IS
   PROCEDURE set_logging_level(
      p_module          VARCHAR2 DEFAULT 'default',
      p_logging_level   NUMBER DEFAULT 2,
      p_debug_level     NUMBER DEFAULT 4
   );

   PROCEDURE set_runmode(
      p_module            VARCHAR2 DEFAULT 'default',
      p_default_runmode   VARCHAR2 DEFAULT 'runtime'
   );

   PROCEDURE set_registration(
      p_module         VARCHAR2 DEFAULT 'default',
      p_registration   VARCHAR2 DEFAULT 'appinfo'
   );

   PROCEDURE add_notification_event(
      p_module		VARCHAR2,
      p_action 		VARCHAR2,
      p_subject		VARCHAR2,
      p_message         VARCHAR2
   );

   PROCEDURE add_notification(
      p_label		VARCHAR2,
      p_module		VARCHAR2,
      p_action 		VARCHAR2,
      p_method		VARCHAR2,
      p_enabled         VARCHAR2,
      p_required        VARCHAR2,
      p_sender		VARCHAR2,
      p_recipients	VARCHAR2
   );

   PROCEDURE set_session_parameter( p_module VARCHAR2, p_name VARCHAR2, p_value VARCHAR2 );

   PROCEDURE clear_log(
      p_runmode      VARCHAR2 DEFAULT NULL,
      p_session_id   NUMBER DEFAULT SYS_CONTEXT( 'USERENV', 'SESSIONID' )
   );
END td_control;
/