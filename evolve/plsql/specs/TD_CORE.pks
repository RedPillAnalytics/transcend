CREATE OR REPLACE PACKAGE td_core AUTHID CURRENT_USER
AS
   FUNCTION is_true( p_parm VARCHAR2, p_allownulls BOOLEAN DEFAULT FALSE )
      RETURN BOOLEAN;

   FUNCTION get_yn_ind( p_parm VARCHAR2 )
      RETURN VARCHAR2;
   
   FUNCTION format_list( p_list VARCHAR2, p_delimiter VARCHAR2 DEFAULT ',' )
      RETURN VARCHAR2;

   FUNCTION SPLIT( p_list VARCHAR2, p_delimiter VARCHAR2 DEFAULT ',', p_format VARCHAR2 DEFAULT 'no' )
      RETURN split_ot PIPELINED;

END td_core;
/