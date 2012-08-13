
CREATE OR REPLACE TYPE cdc_sub_ot 
AUTHID CURRENT_USER AS object
(
  sub_name               VARCHAR2(30),
  sub_type               VARCHAR2(10),
  group_id               NUMBER,
  source_reference       VARCHAR2(61),
  source_group_name      VARCHAR2(8),
  effective_scn          NUMBER,
  expiration_scn         NUMBER,
  
  CONSTRUCTOR FUNCTION cdc_sub_ot 
  ( 
    p_name             VARCHAR2
  )
  RETURN SELF AS RESULT,

  MEMBER PROCEDURE verify,
  
  MEMBER PROCEDURE build_views,

  MEMBER PROCEDURE extend_window
)
NOT FINAL;
/