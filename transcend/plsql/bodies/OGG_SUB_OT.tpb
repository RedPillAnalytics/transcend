
CREATE OR REPLACE TYPE BODY ogg_sub_ot
AS

   -- constructor function for the OGG_SUB_OT object type
   CONSTRUCTOR FUNCTION ogg_sub_ot 
   ( 
     p_sub_name VARCHAR2
   )
      RETURN SELF AS RESULT
   AS
   BEGIN      

      self.initialize ( p_sub_name => p_sub_name );
      
      SELECT lower( ogg_check_table ),
             lower( ogg_check_column ),
             ogg_group_key,
             ogg_group_name
        INTO self.ogg_check_table,
             self.ogg_check_column,
             self.ogg_group_key,
             self.ogg_group_name
        FROM cdc_source
        JOIN cdc_source_external
             USING (source_id)
        JOIN cdc_group
             USING (source_id)
        JOIN cdc_subscription
             USING (group_id)
       WHERE lower( sub_name ) = lower( p_sub_name );
      
      -- return the self reference
      RETURN;

   END ogg_sub_ot;
   
  OVERRIDING MEMBER FUNCTION get_source_scn
      RETURN NUMBER
   AS

      l_scn_sql    VARCHAR2(4000);
      l_scn        NUMBER;
   
      o_ev         evolve_ot           := evolve_ot (p_module => 'cdc_group_ot.get_source_scn');
   BEGIN

      l_scn_sql := 
                'SELECT TO_NUMBER( '
             || self.ogg_check_column
             || ' ) from '
             || self.ogg_check_table
             || ' where group_name = '''
             || self.ogg_group_name
             || ''' and group_key = '
             || self.ogg_group_key;

      evolve.log_variable( 'l_scn_sql',l_scn_sql );

      EXECUTE IMMEDIATE l_scn_sql
      INTO l_scn;

      evolve.log_variable( 'l_scn',l_scn );

      -- reset the evolve_object
      o_ev.clear_app_info;

      RETURN l_scn;

   END get_source_scn;
      
END;
/

SHOW errors
