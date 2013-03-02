
CREATE OR REPLACE TYPE BODY cdc_sub_ot
AS

   -- constructor function for the CDC_SUB_OT object type
   CONSTRUCTOR FUNCTION cdc_sub_ot 
   ( 
     p_sub_name VARCHAR2
   )
      RETURN SELF AS RESULT
   AS
   BEGIN
      
      -- populate the sub information
      self.initialize ( p_sub_name => p_sub_name );
      
      -- return the self reference
      RETURN;

   END cdc_sub_ot;

   MEMBER PROCEDURE initialize 
   ( 
     p_sub_name VARCHAR2
   )
   AS
   BEGIN      
      BEGIN
         -- load all the feed attributes
         SELECT group_name,
                sub_name,
                effective_scn,
                expiration_scn
           INTO self.group_name,
                self.sub_name,
                self.scn_min,
                self.scn_max
           FROM cdc_group
           JOIN cdc_subscription
                USING (group_name)
          WHERE lower( sub_name ) = lower( p_sub_name );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            -- if there is no record found for this file_lable, raise an exception
            evolve.raise_err ('no_cdc_sub', p_sub_name );
      END;
            
      evolve.log_variable( 'self.sub_name', self.sub_name );      

      evolve.log_variable( 'self.group_name', self.group_name );      
      
       self.initialize( p_group_name => self.group_name );
      
   END initialize;

   MEMBER PROCEDURE extend_window
   IS
   
     l_scn      NUMBER;
     l_oldscn   NUMBER;
      
     o_ev    evolve_ot   := evolve_ot (p_module => 'cdc_group_ot.load_fnd_table');
   BEGIN

      evolve.log_variable( 'self.sub_name', self.sub_name );      

      l_scn := self.get_source_scn;
      
      SELECT expiration_scn
        INTO l_oldscn
        FROM cdc_subscription
       WHERE sub_name = self.sub_name;
      
      IF l_scn > l_oldscn
      THEN
   
         UPDATE cdc_subscription
            SET effective_scn = expiration_scn,
                expiration_scn = l_scn
          WHERE sub_name = self.sub_name;
         
      END IF;
         
      -- reset the evolve_object
      o_ev.clear_app_info;
   END extend_window;

END;
/

SHOW errors
