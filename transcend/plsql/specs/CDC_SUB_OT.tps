
CREATE OR REPLACE TYPE cdc_global_ot 
AUTHID CURRENT_USER AS object
(
  cdc_name               VARCHAR2(30),
  cdc_type               VARCHAR2(10),
  external_source        VARCHAR2(100),
  external_name          VARCHAR2(30),
  
  CONSTRUCTOR FUNCTION cdc_global_ot 
  ( 
    p_name             VARCHAR2
  )
  RETURN SELF AS RESULT,

  MEMBER PROCEDURE verify,

  MEMBER PROCEDURE extend_window
)
NOT FINAL;
/