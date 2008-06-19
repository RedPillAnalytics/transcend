CREATE OR REPLACE PACKAGE BODY trans_factory
IS
-- returns a concrete type inherited from the MAPPING_OT type
   FUNCTION get_mapping_ot(
      p_mapping VARCHAR2
   )
      RETURN mapping_ot
   IS
      l_map_type   mapping_conf.mapping_type%type;
      -- need object for the parent type
      o_map   	   mapping_ot := mapping_ot( p_mapping => p_mapping );
      -- also need an object for any subtypes
      o_dim   	   dimension_ot;
      o_ev         evolve_ot			     := evolve_ot( p_module => 'get_mapping_ot' );
   BEGIN
      
      -- simply check the mapping_type attribute to tell us whether this is dimensional or not
      IF o_map.mapping_type = 'dimension'
      THEN
	 -- instantiate the subtype
	 o_dim := dimension_ot( p_mapping => p_mapping );
	 
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

END trans_factory;
/

SHOW errors