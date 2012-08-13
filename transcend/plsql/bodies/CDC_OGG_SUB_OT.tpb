CREATE OR REPLACE TYPE BODY cdc_ogg_sub_ot
AS

   -- constructor function for the CDC_OGG_SUB_OT object type
   CONSTRUCTOR FUNCTION cdc_ogg_sub_ot 
   ( 
     p_name VARCHAR2
   )
      RETURN SELF AS RESULT
   AS
   BEGIN
      BEGIN
         -- load all the feed attributes
         SELECT sub_type,
                CASE sub_type
                WHEN 'goldengate' THEN checkpoint_table
                WHEN 'flashback' THEN dblink_name
                END,
                group_id,
                ogg_group_name,
                fnd_schema,
                stg_schema,
                effective_scn,
                expiration_scn
           INTO self.sub_type,
                self.source_reference,
                self.group_id,
                self.ogg_group_name,
                self.fnd_schema,
                self.stg_schema,
                self.effective_scn,
                self.expiration_scn
           FROM cdc_source
           JOIN cdc_group
                USING (source_id)
           JOIN cdc_subscription
                USING (group_id)
          WHERE lower( sub_name ) = lower( p_name );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            -- if there is no record found for this file_lable, raise an exception
            evolve.raise_err ('no_cdc_subscription', p_name );
      END;

      -- run the business logic to make sure everything works out fine
      verify;

      -- return the self reference
      RETURN;
   END cdc_ogg_sub_ot;

   MEMBER PROCEDURE verify
   IS
      o_ev    evolve_ot   := evolve_ot (p_module => 'cdc_ogg_sub_ot.verify');
   BEGIN

      -- do checks to make sure all the provided information is legitimate
      NULL;
      
      -- reset the evolve_object
      o_ev.clear_app_info;
   END verify;

   -- extend the CDC window
   MEMBER PROCEDURE extend_window
   AS
      o_ev              evolve_ot               := evolve_ot( p_module => 'extend_window' );
   BEGIN
      
      NULL;

      o_ev.clear_app_info;
   
   END extend_window;
   
   -- extend the CDC window
   MEMBER PROCEDURE build_views
   AS
      o_ev              evolve_ot               := evolve_ot( p_module => 'extend_window' );
   BEGIN
      
      NULL;

      o_ev.clear_app_info;
   
   END build_views;

END;
/

SHOW errors
