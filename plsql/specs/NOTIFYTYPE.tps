CREATE OR REPLACE TYPE notifytype AUTHID CURRENT_USER AS object(
   notify_id        NUMBER,
   notify_enabled   VARCHAR2( 3 ),
   action           VARCHAR2( 32 ),
   module           VARCHAR2( 48 ),
   module_id        NUMBER,
   MESSAGE          VARCHAR2( 2000 ),
   subject          VARCHAR2( 100 )
)
NOT FINAL;
/