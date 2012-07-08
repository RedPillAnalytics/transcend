
CREATE OR REPLACE TYPE cdc_global_ot 
AUTHID CURRENT_USER AS object
(
  cdc_name               VARCHAR2(30),
  
  CONSTRUCTOR FUNCTION cdc_global_ot 
  ( 
    p_name             cdc_global.cdc_name%type
  )
  RETURN SELF AS RESULT,

  MEMBER PROCEDURE verify,

  MEMBER PROCEDURE extend_window
)
NOT FINAL;
/