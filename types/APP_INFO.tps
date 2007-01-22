CREATE OR REPLACE TYPE common.app_info AS OBJECT(
   prev_client_info   VARCHAR2( 64 ),
   prev_module        VARCHAR2( 48 ),
   prev_action        VARCHAR2( 32 ),
   CONSTRUCTOR FUNCTION app_info(
      p_action        VARCHAR2 DEFAULT 'Begin Procedure/Function',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL,
      p_debug         BOOLEAN DEFAULT FALSE )
      RETURN SELF AS RESULT,
   MEMBER PROCEDURE set_action(
      p_action   VARCHAR2 ),
   MEMBER PROCEDURE clear_app_info
);
/