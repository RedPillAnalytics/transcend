CREATE OR REPLACE PACKAGE BODY trans_factory
IS
   -- takes a mapping name and returns a subtype of MAPPING_OT
   FUNCTION get_mapping_ot( p_mapping VARCHAR2, p_batch_id NUMBER DEFAULT NULL )
      RETURN mapping_ot
   IS
      l_map_type   mapping_conf.mapping_type%TYPE;

      -- there is an owb constant that can be used to get the mapping name
      -- however, the constant puts double quotes around it
      -- need to strip these double quotes just in case
      l_mapping    mapping_conf.mapping_name%TYPE  := LOWER( regexp_replace(p_mapping,'^"|"$',NULL));
      -- need object for the parent type
      o_map        mapping_ot;

      -- also need an object for any subtypes
      o_dim        dimension_ot;
      o_ev         evolve_ot                        := evolve_ot( p_module => 'trans_factory.get_mapping_ot' );
   BEGIN
      
      -- instantiate the base MAPPING_OT first
      o_map        := mapping_ot( p_mapping    => lower( p_mapping ));

      -- let's register what the mapping_type is
      evolve.log_variable( 'o_map.mapping_type', o_map.mapping_type );
      
      -- simply check the mapping_type attribute to tell us whether this is dimensional or not
      CASE o_map.mapping_type
      WHEN 'dimension'
      THEN

         -- instantiate the subtype
         o_dim    := dimension_ot( p_mapping => l_mapping, p_batch_id => p_batch_id );

         -- now polymorph the type
         o_map    := o_dim;

         evolve.log_msg( 'TRANS_FACTORY returned a DIMENSION_OT', 5 );
         
      WHEN 'table'         
      THEN
         
         evolve.log_msg( 'TRANS_FACTOY returned a MAPPING_OT', 5 );
         
      END CASE;

      -- now simply return the type
      o_ev.clear_app_info;

      RETURN o_map;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         RAISE;
   END get_mapping_ot;

   -- takes a table name and table_owner and returns a subtype of file_ot
   FUNCTION get_file_label_ot( 
      p_file_label VARCHAR2,
      p_directory  VARCHAR2 DEFAULT NULL
   )
      RETURN file_label_ot
   IS
      l_label_type      file_conf.label_type%TYPE;
      -- need object for the parent type
      o_label     file_label_ot;
      -- also need an object for any subtypes
      o_extract   extract_ot;
      o_feed      feed_ot;
      -- and then the basic Evolve instrumentation object
      o_ev        evolve_ot                        := evolve_ot( p_module => 'trans_factory.get_file_label_ot' );
   BEGIN
      BEGIN

         -- get the file name and file type
         SELECT lower( label_type )
           INTO l_label_type
           FROM file_conf
          WHERE LOWER( file_label ) = LOWER( p_file_label );
         
      EXCEPTION
         WHEN no_data_found
         THEN 
            -- if there is no record found for this file_lable, raise an exception
            evolve.raise_err ('no_feed', '"'||p_file_label||'"');
      END;

      -- instantiate an object based on label_type
      -- polymorph the file_detail_ot based on the label_type
      CASE l_label_type
      WHEN 'feed'
      THEN
         o_feed := feed_ot( p_file_label => p_file_label,
                            p_source_directory  => p_directory );
         o_label := o_feed;
      WHEN 'extract'
      THEN
         o_extract := extract_ot( p_file_label => p_file_label,
                                  p_directory  => p_directory );
         o_label := o_extract;
      END CASE;

      -- now simply return the type
      o_ev.clear_app_info;
      RETURN o_label;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         RAISE;
   END get_file_label_ot;

   -- take a FILE_DETAIL_ID and return a FILE_DETAIL_OT
   FUNCTION get_file_detail_ot( 
      p_file_detail_id NUMBER,
      p_directory      VARCHAR2 DEFAULT NULL
   )
      RETURN file_detail_ot
   IS
      o_detail     file_detail_ot;
      -- and then the basic Evolve instrumentation object
      o_ev        evolve_ot                        := evolve_ot( p_module => 'trans_factory.get_file_detail_ot' );
   BEGIN

     o_detail := file_detail_ot( p_file_detail_id => p_file_detail_id,
                                 p_directory      => p_directory );

      -- now simply return the type
      o_ev.clear_app_info;
      RETURN o_detail;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END get_file_detail_ot;
   
   FUNCTION get_cdc_group_ot
      (
        p_group_name      cdc_group.group_name%type
      )
      RETURN cdc_group_ot
   IS
      -- base object class for inheritance
      o_group          cdc_group_ot;
      -- inherited types
      -- o_ogg         cdc_ogg_group_ot;
      -- and then the basic Evolve instrumentation object
      o_ev          evolve_ot := evolve_ot( p_module => 'get_cdc_group_ot' );
   BEGIN
      
      o_group := cdc_group_ot( p_group_name => p_group_name );

      -- now simply return the type
      o_ev.clear_app_info;
      RETURN o_group;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END get_cdc_group_ot;
   
   FUNCTION get_cdc_sub_ot
      (
        p_sub_name      cdc_subscription.sub_name%type
      )
      RETURN cdc_sub_ot
   IS
      -- base object class for inheritance
      o_cdc          cdc_sub_ot;
      -- inherited types
      o_ogg          ogg_sub_ot;
      -- and then the basic Evolve instrumentation object
      o_ev          evolve_ot := evolve_ot( p_module => 'get_cdc_sub_ot' );
   BEGIN
      
      o_cdc := cdc_sub_ot( p_sub_name => p_sub_name );
      
      CASE o_cdc.source_type
      -- check to see if this is a special case
      -- at this point, we only support one special case
      WHEN 'goldengate'
      THEN
         -- instantiate the new GoldenGate object
         o_ogg := ogg_sub_ot( p_sub_name => p_sub_name );
         -- polymorph the base class into the GoldenGate class
         o_cdc := o_ogg;
         evolve.log_msg( 'CDC_SUB_OT polymorphed to OGG_SUB_OT', 4 );
      END CASE;

      -- now simply return the type
      o_ev.clear_app_info;
      RETURN o_cdc;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END get_cdc_sub_ot;

END trans_factory;
/

SHOW errors