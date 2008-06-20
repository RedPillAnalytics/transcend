CREATE OR REPLACE PACKAGE BODY trans_factory
IS
   -- takes a mapping name and returns a subtype of MAPPING_OT
   FUNCTION get_mapping_ot(
      p_mapping VARCHAR2,
      p_batch_id NUMBER DEFAULT NULL
   )
      RETURN mapping_ot
   IS
      l_map_type   mapping_conf.mapping_type%type;
      -- need object for the parent type
      o_map   	   mapping_ot := mapping_ot( p_mapping => p_mapping,
					     p_batch_id => p_batch_id );
      -- also need an object for any subtypes
      o_dim   	   dimension_ot;
      o_ev         evolve_ot			     := evolve_ot( p_module => 'get_mapping_ot' );
   BEGIN
      
      -- simply check the mapping_type attribute to tell us whether this is dimensional or not
      IF o_map.mapping_type = 'dimension'
      THEN
	 -- instantiate the subtype
	 o_dim := dimension_ot( p_mapping => p_mapping,
				p_batch_id => p_batch_id );
	 
	 -- now polymorph the type
	 o_map := o_dim;
      END IF;
      
      -- now simply return the type
      RETURN o_map;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
         RAISE;
   END get_mapping_ot;
  
   -- takes a table name and table_owner and returns a subtype of mapping_ot 
   FUNCTION get_mapping_ot(
      p_owner	VARCHAR2,
      p_table	VARCHAR2
   )
      RETURN mapping_ot
   IS
      l_mapping	   mapping_conf.mapping_name%type;
      -- need object for the parent type
      o_map   	   mapping_ot;
      -- also need an object for any subtypes
      o_dim   	   dimension_ot;
      
      -- and then the basic Evolve instrumentation object
      o_ev         evolve_ot	   := evolve_ot( p_module => 'get_mapping_ot' );
   BEGIN
      
      -- get the mapping name and mapping type
      SELECT mapping_name
	INTO l_mapping
	FROM mapping_conf
       WHERE lower( table_name ) = lower( p_table )
	 AND lower( table_owner ) = lower( p_owner );
      
      -- use another GET_MAPPING_OT function
      o_map := get_mapping_ot( l_mapping );
      
      -- now simply return the type
      RETURN o_map;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve_log.log_err;
         RAISE;
   END get_mapping_ot;

END trans_factory;
/

SHOW errors