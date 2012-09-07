
CREATE OR REPLACE PACKAGE trans_cdc AUTHID CURRENT_USER
IS

   PROCEDURE build_subscription
      (
        p_group_name   cdc_group.group_name%type
      );
      
   PROCEDURE build_foundation
      (
        p_group_name   cdc_group.group_name%type,
        p_scn          NUMBER                      DEFAULT NULL
      );

   PROCEDURE load_foundation
      (
        p_group_name   cdc_group.group_name%type
      );
      
   PROCEDURE extend_window
      (
        p_sub_name   cdc_subscription.sub_name%type
      );

END trans_cdc;
/

SHOW errors