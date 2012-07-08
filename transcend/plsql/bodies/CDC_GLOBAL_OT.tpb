CREATE OR REPLACE TYPE BODY cdc_global_ot
AS

   -- constructor function for the CDC_GLOBAL_OT object type
   CONSTRUCTOR FUNCTION cdc_global_ot 
   ( 
     p_name cdc_global.name%type 
   )
      RETURN SELF AS RESULT
   AS
   BEGIN
      BEGIN
         -- load all the feed attributes
         SELECT cdc_type,
                cdc_external_source,
                cdc_external_name
           INTO self.cdc_name,
                self.cdc_type,
                self.external_source,
                self.external_name
           FROM cdc_global
          WHERE lower( cdc_name ) = lower( p_name );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            -- if there is no record found for this file_lable, raise an exception
            evolve.raise_err ('no_cdc_global', p_file_detail_id);
      END;

      -- run the business logic to make sure everything works out fine
      verify;

      -- return the self reference
      RETURN;
   END cdc_global_ot;

   MEMBER PROCEDURE verify
   IS
      o_ev    evolve_ot   := evolve_ot (p_module => 'cdc_global_ot.verify');
   BEGIN

      -- do checks to make sure all the provided information is legitimate
      NULL;
      
      -- reset the evolve_object
      o_ev.clear_app_info;
   END verify;

   -- extend the CDC window
   MEMBER PROCEDURE extend_window
   AS
      o_ev              evolve_ot               := evolve_ot( p_module => 'unarchive_file_detail' );
   BEGIN
      
      NULL;

      o_ev.clear_app_info;
   
   END unarchive;

END;
/

SHOW errors
