CREATE OR REPLACE PACKAGE BODY trans_factory
IS
   -- takes a mapping name and returns a subtype of MAPPING_OT
   FUNCTION get_mapping_ot( p_mapping VARCHAR2, p_batch_id NUMBER DEFAULT NULL )
      RETURN mapping_ot
   IS
      l_map_type   mapping_conf.mapping_type%TYPE;
      -- need object for the parent type
      o_map        mapping_ot                       := mapping_ot( p_mapping => p_mapping, p_batch_id => p_batch_id );
      -- also need an object for any subtypes
      o_dim        dimension_ot;
      o_ev         evolve_ot                        := evolve_ot( p_module => 'get_mapping_ot' );
   BEGIN
      -- simply check the mapping_type attribute to tell us whether this is dimensional or not
      IF o_map.mapping_type = 'dimension'
      THEN
         -- instantiate the subtype
         o_dim    := dimension_ot( p_mapping => p_mapping, p_batch_id => p_batch_id );
         -- now polymorph the type
         o_map    := o_dim;
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

   -- takes a table name and table_owner and returns a subtype of mapping_ot
   FUNCTION get_mapping_ot( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN mapping_ot
   IS
      l_mapping   mapping_conf.mapping_name%TYPE;
      -- need object for the parent type
      o_map       mapping_ot;
      -- also need an object for any subtypes
      o_dim       dimension_ot;
      -- and then the basic Evolve instrumentation object
      o_ev        evolve_ot                        := evolve_ot( p_module => 'get_mapping_ot' );
   BEGIN
      -- get the mapping name and mapping type
      SELECT mapping_name
        INTO l_mapping
        FROM mapping_conf
       WHERE LOWER( table_name ) = LOWER( p_table ) AND LOWER( table_owner ) = LOWER( p_owner );

      -- use another GET_MAPPING_OT function
      o_map    := get_mapping_ot( l_mapping );
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
      o_ev        evolve_ot                        := evolve_ot( p_module => 'get_file_label_ot' );
   BEGIN
      -- get the file name and file type
      SELECT lower( label_type )
        INTO l_label_type
        FROM file_conf
       WHERE LOWER( file_label ) = LOWER( p_file_label );

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

END trans_factory;
/

SHOW errors