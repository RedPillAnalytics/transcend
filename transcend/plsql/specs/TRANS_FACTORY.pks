
CREATE OR REPLACE PACKAGE trans_factory AUTHID CURRENT_USER
IS
   FUNCTION get_mapping_ot
   (
      p_mapping VARCHAR2,
      p_batch_id NUMBER DEFAULT NULL
   )
      RETURN mapping_ot;
   
   FUNCTION get_file_label_ot( 
      p_file_label VARCHAR2,
      p_directory  VARCHAR2 DEFAULT NULL
   )
      RETURN file_label_ot;

   FUNCTION get_file_detail_ot( 
      p_file_detail_id NUMBER,
      p_directory      VARCHAR2 DEFAULT NULL
   )
      RETURN file_detail_ot;
      
   FUNCTION get_cdc_sub_ot
      (
        p_name      cdc_subscription.sub_name%type
      )
      RETURN cdc_sub_ot;

END trans_factory;
/
