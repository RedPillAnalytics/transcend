CREATE OR REPLACE TYPE insttype AUTHID CURRENT_USER AS object(
   prev_client_info        VARCHAR2( 64 ),
   prev_module             VARCHAR2( 48 ),
   prev_action             VARCHAR2( 32 ),
   prev_registration	   VARCHAR2( 20 ),
   prev_logging_level	   NUMBER,
   prev_runmode	           VARCHAR2( 10 ),
   prev_batch_id	   NUMBER
)
NOT FINAL;
/