
CREATE OR REPLACE PACKAGE BODY trans_cdc
IS

   PROCEDURE build_interface
      (
        p_group_name   cdc_group.group_name%type
      )
   IS
      l_rows      BOOLEAN      := FALSE;
      o_group     cdc_group_ot := trans_factory.get_cdc_group_ot( p_group_name => p_group_name );
      o_ev        evolve_ot    := evolve_ot( p_module => 'trans_cdc.build_interface' );
   BEGIN
      
      o_group.build_interface;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END build_interface;
      
   PROCEDURE build_subscription
      (
        p_group_name   cdc_group.group_name%type,
        p_scn          NUMBER                      DEFAULT NULL
      )
   IS
      l_rows      BOOLEAN      := FALSE;
      o_group     cdc_group_ot := trans_factory.get_cdc_group_ot( p_group_name => p_group_name );
      o_ev        evolve_ot    := evolve_ot( p_module => 'trans_cdc.build_subscription' );
   BEGIN
      
      o_group.build_subscription;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END build_subscription;
   
   PROCEDURE load_subscription
      (
        p_group_name   cdc_group.group_name%type
      )
   IS
      l_rows      BOOLEAN      := FALSE;
      o_group     cdc_group_ot := trans_factory.get_cdc_group_ot( p_group_name => p_group_name );
      o_ev        evolve_ot    := evolve_ot( p_module => 'trans_cdc.load_subscription' );
   BEGIN
      
      o_group.load_subscription;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END load_subscription;

   PROCEDURE extend_window
      (
        p_sub_name   cdc_subscription.sub_name%type
      )
   IS
      l_rows      BOOLEAN      := FALSE;
      o_sub       cdc_sub_ot   := trans_factory.get_cdc_sub_ot( p_sub_name => p_sub_name );
      o_ev        evolve_ot    := evolve_ot( p_module => 'trans_cdc.extend_window' );
   BEGIN
      
      o_sub.extend_window;

      o_ev.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         evolve.log_err;
         o_ev.clear_app_info;
         RAISE;
   END extend_window;

END trans_cdc;
/

SHOW errors