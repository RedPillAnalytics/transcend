CREATE OR REPLACE PACKAGE trans_factory AUTHID CURRENT_USER
IS
   FUNCTION get_mapping_ot(
      p_mapping VARCHAR2,
      p_batch_id NUMBER DEFAULT NULL
   )
      RETURN mapping_ot;
      
   FUNCTION get_mapping_ot(
      p_owner	VARCHAR2,
      p_table	VARCHAR2
   )
      RETURN mapping_ot;

END trans_factory;
/
