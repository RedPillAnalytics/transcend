
CREATE OR REPLACE TYPE ogg_sub_ot 
FORCE UNDER cdc_sub_ot
(
  ogg_group_name         VARCHAR2(8),
  ogg_group_key          NUMBER,
  ogg_check_table        VARCHAR2(61),
  ogg_check_column       VARCHAR2(30), 
  
  CONSTRUCTOR FUNCTION ogg_sub_ot 
  ( 
     p_sub_name VARCHAR2
  )
  RETURN SELF AS RESULT,
  
  OVERRIDING MEMBER FUNCTION get_source_scn
  RETURN NUMBER

)
NOT FINAL
/