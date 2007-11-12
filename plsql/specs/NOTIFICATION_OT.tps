CREATE OR REPLACE TYPE notification_ot AS OBJECT(
   notification_label      VARCHAR2( 40 ),
   module                  VARCHAR2( 48 ),
   action                  VARCHAR2( 32 ),
   notification_method     VARCHAR2( 20 ),
   notification_enabled    VARCHAR( 3 ),
   subject                 VARCHAR2( 100 ),
   message                 VARCHAR2( 2000 ),
   sender                  VARCHAR2( 1024 ),
   recipients              VARCHAR2( 2000 ),
   notification_required   VARCHAR2( 3 ),
   CONSTRUCTOR FUNCTION notification_ot
      RETURN SELF AS RESULT,
   MEMBER PROCEDURE send( p_message VARCHAR2 DEFAULT NULL )
)
NOT FINAL;
/