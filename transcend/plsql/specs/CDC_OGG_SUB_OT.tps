
CREATE OR REPLACE TYPE cdc_ogg_sub_ot 
UNDER cdc_sub_ot
(
  checkpoint_table      VARCHAR2(30),
  group_name            VARCHAR2(8),
  
  CONSTRUCTOR FUNCTION cdc_ogg_sub_ot 
  ( 
    p_name             VARCHAR2
  )
  RETURN SELF AS RESULT,

  MEMBER PROCEDURE verify
)
;
/