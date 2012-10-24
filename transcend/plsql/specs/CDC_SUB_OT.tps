
CREATE OR REPLACE TYPE cdc_sub_ot 
FORCE UNDER cdc_group_ot
(
  sub_name               VARCHAR2(30),
  scn_min                NUMBER,
  scn_max                NUMBER,
  
  CONSTRUCTOR FUNCTION cdc_sub_ot 
  ( 
     p_sub_name VARCHAR2
  )
  RETURN SELF AS RESULT,
 
  MEMBER PROCEDURE initialize 
   ( 
     p_sub_name VARCHAR2
   ),
 
  MEMBER PROCEDURE extend_window

)
NOT FINAL
/