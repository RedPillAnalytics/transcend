CREATE OR REPLACE TYPE evolve_ot UNDER app_ot(
   CONSTRUCTOR FUNCTION evolve_ot(
      p_action        VARCHAR2 DEFAULT 'begin module',
      p_module        VARCHAR2 DEFAULT NULL,
      p_client_info   VARCHAR2 DEFAULT NULL
   )
      RETURN SELF AS RESULT,
   OVERRIDING MEMBER PROCEDURE change_action( p_action VARCHAR2 ),
   OVERRIDING MEMBER PROCEDURE clear_app_info,
   MEMBER PROCEDURE send( p_label VARCHAR2, p_message VARCHAR2 DEFAULT NULL )
)
;
/