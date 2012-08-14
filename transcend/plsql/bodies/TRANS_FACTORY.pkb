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
      
      -- get the mapping type
      BEGIN

         SELECT mapping_type
           INTO l_map_type
           FROM mapping_conf
          WHERE lower ( mapping_name ) = lower ( l_mapping );
         
      EXCEPTION
         WHEN no_data_found
         THEN 
            evolve.raise_err( 'no_mapping');
      END;

      -- let's register what the mapping_type is
      evolve.log_variable( 'l_map_type',l_map_type );
      
      -- simply check the mapping_type attribute to tell us whether this is dimensional or not
      IF l_map_type = 'dimension'
      THEN
         -- instantiate the subtype
         o_dim    := dimension_ot( p_mapping => l_mapping, p_batch_id => p_batch_id );
         -- now polymorph the type
         o_map    := o_dim;
         evolve.log_msg( 'TRANS_FACTORY returned a DIMENSION_OT', 5 );
      ELSE 

         -- instantiate the subtype
         o_map    := mapping_ot( p_mapping => l_mapping, p_batch_id => p_batch_id );

         evolve.log_msg( 'TRANS_FACTOY returned a MAPPING_OT', 5 );
         
      END IF;

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
   
   FUNCTION get_cdc_sub_ot
      (
        p_name      cdc_subscription.sub_name%type
      )
      RETURN cdc_sub_ot
   IS
      l_sub_type    cdc_source.sub_type%type;
      -- base object class for inheritance
      o_sub         cdc_sub_ot;
      -- inherited types
      o_ogg         cdc_ogg_sub_ot;
      -- and then the basic Evolve instrumentation object
      o_ev          evolve_ot := evolve_ot( p_module => 'get_cdc_sub_ot' );
   BEGIN
      
      SELECT sub_type
        INTO l_sub_type
        FROM cdc_source
        JOIN cdc_group
             USING (source_id)
        JOIN cdc_subscription
             USING (group_id)
       WHERE lower( sub_name ) = lower( p_name );
      
      -- instantiate an object based on sub_type
      -- polymorph the cdc_sub_ot based on the sub_type
      CASE l_sub_type
      WHEN 'goldengate'
      THEN
         o_ogg := cdc_ogg_sub_ot( p_name => p_name );
         o_sub := o_ogg;
      END CASE;

      -- now simply return the type
      o_ev.clear_app_info;
      RETURN o_sub;
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