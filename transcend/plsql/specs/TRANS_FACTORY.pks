CREATE OR REPLACE PACKAGE trans_factory AUTHID CURRENT_USER
IS
   FUNCTION get_mapping_ot(
      p_mapping VARCHAR2
   )
      RETURN mapping_ot;

END trans_factory;
/
