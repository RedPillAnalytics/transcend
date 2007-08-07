CREATE OR REPLACE PACKAGE td_ext AUTHID CURRENT_USER
AS
   FUNCTION is_true( p_parm VARCHAR2, p_allownulls BOOLEAN DEFAULT FALSE )
      RETURN BOOLEAN;

   FUNCTION get_yn_ind( p_parm VARCHAR2 )
      RETURN VARCHAR2;

   FUNCTION get_err_cd( p_name VARCHAR2 )
      RETURN NUMBER;

   FUNCTION get_err_msg( p_name VARCHAR2 )
      RETURN VARCHAR2;
END td_ext;
/