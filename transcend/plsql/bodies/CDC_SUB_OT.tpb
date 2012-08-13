CREATE OR REPLACE TYPE BODY cdc_sub_ot
AS

   -- extend the CDC window
   MEMBER PROCEDURE extend_window
   AS
      o_ev              evolve_ot               := evolve_ot( p_module => 'extend_window' );
   BEGIN
      
      NULL;

      o_ev.clear_app_info;
   
   END extend_window;
   
   -- create change views for the staging layer
   MEMBER PROCEDURE build_stg_views
   AS
      o_ev              evolve_ot               := evolve_ot( p_module => 'extend_window' );
   BEGIN
      
      NULL;

      o_ev.clear_app_info;
   
   END build_stg_views;
   
   -- create change views for the foundation layer
   MEMBER PROCEDURE build_fnd_views
   AS
      o_ev              evolve_ot               := evolve_ot( p_module => 'extend_window' );
   BEGIN
      
      NULL;

      o_ev.clear_app_info;
   
   END build_fnd_views;

END;
/

SHOW errors
