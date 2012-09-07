CREATE OR REPLACE PACKAGE trans_adm AUTHID CURRENT_USER
IS
   -- used to set configuration parameters to null
   null_value             CONSTANT VARCHAR2 (6)  := '*null*';

   all_modules	CONSTANT VARCHAR2(13) := '*all_modules*';

   -- constants from the TD_ADM package
   product_version      CONSTANT   NUMBER         := tdsys.td_adm.product_version;
   transcend_product    CONSTANT   VARCHAR2(9)    := tdsys.td_adm.transcend_product;
   evolve_product       CONSTANT   VARCHAR2(9)    := tdsys.td_adm.evolve_product;

   -- constants for EXPAND_FILE
   gzip_method          CONSTANT   VARCHAR2(15)   := td_utils.gzip_method;
   compress_method      CONSTANT   VARCHAR2(15)   := td_utils.compress_method;
   bzip2_method         CONSTANT   VARCHAR2(15)   := td_utils.bzip2_method;
   zip_method           CONSTANT   VARCHAR2(15)   := td_utils.zip_method;

   -- constants used for DECRYPT_FILE
   gpg_method           CONSTANT   VARCHAR2(15)   := td_utils.gpg_method;

   PROCEDURE set_default_configs;
      
   PROCEDURE create_feed (
      p_file_label         VARCHAR2,
      p_file_group         VARCHAR2,
      p_directory	   VARCHAR2,
      p_source_directory   VARCHAR2,
      p_source_regexp      VARCHAR2,
      p_work_directory     VARCHAR2 DEFAULT NULL,
      p_owner              VARCHAR2 DEFAULT NULL,
      p_table              VARCHAR2 DEFAULT NULL,
      p_filename           VARCHAR2 DEFAULT NULL,
      p_match_parameter    VARCHAR2 DEFAULT 'i',
      p_source_policy      VARCHAR2 DEFAULT 'newest',
      p_store_original     VARCHAR2 DEFAULT 'no',
      p_compress_method	   VARCHAR2 DEFAULT NULL,
      p_encrypt_method	   VARCHAR2 DEFAULT NULL,
      p_passphrase         VARCHAR2 DEFAULT NULL,
      p_required           VARCHAR2 DEFAULT 'yes',
      p_min_bytes          NUMBER   DEFAULT NULL,
      p_max_bytes          NUMBER   DEFAULT NULL,
      p_reject_limit       NUMBER   DEFAULT 100,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_delete_source      VARCHAR2 DEFAULT 'yes',
      p_description        VARCHAR2 DEFAULT NULL
   );

   PROCEDURE modify_feed (
      p_file_label         VARCHAR2,
      p_file_group         VARCHAR2 DEFAULT NULL,
      p_directory	   VARCHAR2 DEFAULT NULL,
      p_source_directory   VARCHAR2 DEFAULT NULL,
      p_source_regexp      VARCHAR2 DEFAULT NULL,
      p_work_directory     VARCHAR2 DEFAULT NULL,
      p_owner              VARCHAR2 DEFAULT NULL,
      p_table              VARCHAR2 DEFAULT NULL,
      p_filename           VARCHAR2 DEFAULT NULL,
      p_match_parameter    VARCHAR2 DEFAULT NULL,
      p_source_policy      VARCHAR2 DEFAULT NULL,
      p_store_original     VARCHAR2 DEFAULT NULL,
      p_compress_method	   VARCHAR2 DEFAULT NULL,
      p_encrypt_method	   VARCHAR2 DEFAULT NULL,
      p_passphrase         VARCHAR2 DEFAULT NULL,
      p_required           VARCHAR2 DEFAULT NULL,
      p_min_bytes          NUMBER   DEFAULT NULL,
      p_max_bytes          NUMBER   DEFAULT NULL,
      p_reject_limit       NUMBER   DEFAULT NULL,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_delete_source      VARCHAR2 DEFAULT NULL,
      p_description        VARCHAR2 DEFAULT NULL
   );
      
   PROCEDURE delete_feed (
      p_file_label         VARCHAR2
   );

   PROCEDURE create_extract (
      p_file_label         VARCHAR2,
      p_file_group         VARCHAR2,
      p_filename           VARCHAR2,
      p_object_owner       VARCHAR2,
      p_object_name        VARCHAR2,
      p_directory          VARCHAR2,
      p_work_directory     VARCHAR2 DEFAULT NULL,
      p_file_datestamp     VARCHAR2 DEFAULT NULL,
      p_dateformat         VARCHAR2 DEFAULT 'mm/dd/yyyy hh:mi:ss am',
      p_tsformat           VARCHAR2 DEFAULT 'mm/dd/yyyy hh:mi:ss:x:ff am',
      p_delimiter          VARCHAR2 DEFAULT ',',
      p_quotechar          VARCHAR2 DEFAULT NULL,
      p_headers            VARCHAR2 DEFAULT 'yes',
      p_min_bytes          NUMBER   DEFAULT 0,
      p_max_bytes          NUMBER   DEFAULT 0,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_reject_limit	   NUMBER   DEFAULT 0,
      p_description        VARCHAR2 DEFAULT NULL
   );
      
   
   PROCEDURE modify_extract (
      p_file_label         VARCHAR2,
      p_file_group         VARCHAR2 DEFAULT NULL,
      p_filename           VARCHAR2 DEFAULT NULL,
      p_object_owner       VARCHAR2 DEFAULT NULL,
      p_object_name        VARCHAR2 DEFAULT NULL,
      p_directory          VARCHAR2 DEFAULT NULL,
      p_work_directory     VARCHAR2 DEFAULT NULL,
      p_file_datestamp     VARCHAR2 DEFAULT NULL,
      p_dateformat         VARCHAR2 DEFAULT NULL,
      p_tsformat           VARCHAR2 DEFAULT NULL,
      p_delimiter          VARCHAR2 DEFAULT NULL,
      p_quotechar          VARCHAR2 DEFAULT NULL,
      p_headers            VARCHAR2 DEFAULT NULL,
      p_min_bytes          NUMBER   DEFAULT NULL,
      p_max_bytes          NUMBER   DEFAULT NULL,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_reject_limit	   NUMBER   DEFAULT NULL,
      p_description        VARCHAR2 DEFAULT NULL
   );   
      
   PROCEDURE delete_extract (
      p_file_label         VARCHAR2
   );
   
   PROCEDURE create_map_control (
      p_mapping             VARCHAR2,
      p_description         VARCHAR2 DEFAULT NULL
   );
      
   PROCEDURE delete_map_control (
      p_mapping             VARCHAR2
   );

   PROCEDURE create_mapping (
      p_mapping             VARCHAR2,
      p_owner               VARCHAR2 DEFAULT NULL,
      p_table               VARCHAR2 DEFAULT NULL,
      p_partname            VARCHAR2 DEFAULT NULL,
      p_indexes             VARCHAR2 DEFAULT 'ignore',
      p_index_regexp        VARCHAR2 DEFAULT NULL,
      p_index_type          VARCHAR2 DEFAULT NULL,
      p_part_type           VARCHAR2 DEFAULT 'all',
      p_idx_concurrency     VARCHAR2 DEFAULT 'no',
      p_constraints         VARCHAR2 DEFAULT 'ignore',
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_con_concurrency     VARCHAR2 DEFAULT 'no',
      p_drop_dep            VARCHAR2 DEFAULT 'yes',
      p_staging_owner       VARCHAR2 DEFAULT NULL,
      p_staging_table       VARCHAR2 DEFAULT NULL,
      p_staging_column      VARCHAR2 DEFAULT NULL,
      p_replace_method      VARCHAR2 DEFAULT NULL,
      p_statistics          VARCHAR2 DEFAULT 'ignore',
      p_description         VARCHAR2 DEFAULT NULL
   );

   PROCEDURE modify_mapping (
      p_mapping             VARCHAR2,
      p_owner               VARCHAR2 DEFAULT NULL,
      p_table               VARCHAR2 DEFAULT NULL,
      p_partname            VARCHAR2 DEFAULT NULL,
      p_indexes             VARCHAR2 DEFAULT NULL,
      p_index_regexp        VARCHAR2 DEFAULT NULL,
      p_index_type          VARCHAR2 DEFAULT NULL,
      p_part_type           VARCHAR2 DEFAULT NULL,
      p_idx_concurrency     VARCHAR2 DEFAULT NULL,
      p_constraints         VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_con_concurrency     VARCHAR2 DEFAULT NULL,
      p_drop_dep            VARCHAR2 DEFAULT NULL,
      p_staging_owner       VARCHAR2 DEFAULT NULL,
      p_staging_table       VARCHAR2 DEFAULT NULL,
      p_staging_column      VARCHAR2 DEFAULT NULL,
      p_replace_method      VARCHAR2 DEFAULT NULL,
      p_statistics          VARCHAR2 DEFAULT NULL,
      p_description         VARCHAR2 DEFAULT NULL
   );

   PROCEDURE delete_mapping (
      p_mapping             VARCHAR2
   );

   PROCEDURE create_dimension (
      p_mapping            VARCHAR2,
      p_owner              VARCHAR2,
      p_table              VARCHAR2,
      p_source_owner       VARCHAR2,
      p_source_table       VARCHAR2,
      p_sequence_owner     VARCHAR2,
      p_sequence_name      VARCHAR2,
      p_staging_owner      VARCHAR2 DEFAULT NULL,
      p_staging_table      VARCHAR2 DEFAULT NULL,
      p_default_scd_type   NUMBER   DEFAULT 2,
      p_direct_load        VARCHAR2 DEFAULT 'yes',
      p_replace_method     VARCHAR2 DEFAULT 'merge',
      p_statistics         VARCHAR2 DEFAULT 'transfer',
      p_indexes            VARCHAR2 DEFAULT 'ignore',
      p_index_regexp       VARCHAR2 DEFAULT NULL,
      p_index_type         VARCHAR2 DEFAULT NULL,
      p_idx_concurrency    VARCHAR2 DEFAULT 'no',
      p_constraints        VARCHAR2 DEFAULT 'ignore',
      p_constraint_regexp  VARCHAR2 DEFAULT NULL,
      p_constraint_type    VARCHAR2 DEFAULT NULL,
      p_con_concurrency    VARCHAR2 DEFAULT 'no',
      p_stage_key_def      NUMBER   DEFAULT -.01,
      p_char_nvl_def       VARCHAR2 DEFAULT '~',
      p_date_nvl_def       DATE     DEFAULT TO_DATE ('01/01/9999','mm/dd/yyyy'),
      p_num_nvl_def        NUMBER   DEFAULT -.01,
      p_description        VARCHAR2 DEFAULT NULL
   );

   PROCEDURE modify_dimension (
      p_mapping            VARCHAR2,
      p_owner              VARCHAR2 DEFAULT NULL,
      p_table              VARCHAR2 DEFAULT NULL,
      p_source_owner       VARCHAR2 DEFAULT NULL,
      p_source_table       VARCHAR2 DEFAULT NULL,
      p_sequence_owner     VARCHAR2 DEFAULT NULL,
      p_sequence_name      VARCHAR2 DEFAULT NULL,
      p_staging_owner      VARCHAR2 DEFAULT NULL,
      p_staging_table      VARCHAR2 DEFAULT NULL,
      p_default_scd_type   NUMBER   DEFAULT NULL,
      p_direct_load        VARCHAR2 DEFAULT NULL,
      p_replace_method     VARCHAR2 DEFAULT NULL,
      p_statistics         VARCHAR2 DEFAULT NULL,
      p_indexes            VARCHAR2 DEFAULT 'ignore',
      p_index_regexp       VARCHAR2 DEFAULT NULL,
      p_index_type         VARCHAR2 DEFAULT NULL,
      p_idx_concurrency    VARCHAR2 DEFAULT 'no',
      p_constraints        VARCHAR2 DEFAULT 'ignore',
      p_constraint_regexp  VARCHAR2 DEFAULT NULL,
      p_constraint_type    VARCHAR2 DEFAULT NULL,
      p_con_concurrency    VARCHAR2 DEFAULT 'no',
      p_stage_key_def      NUMBER   DEFAULT NULL,
      p_char_nvl_def       VARCHAR2 DEFAULT NULL,
      p_date_nvl_def       DATE     DEFAULT NULL,
      p_num_nvl_def        NUMBER   DEFAULT NULL,
      p_description        VARCHAR2 DEFAULT NULL
  );
   
   PROCEDURE delete_dimension ( p_mapping VARCHAR2 );
   
   PROCEDURE create_dim_attribs (
      p_mapping         VARCHAR2,
      p_surrogate       VARCHAR2,
      p_effective_dt    VARCHAR2,
      p_expiration_dt   VARCHAR2,
      p_current_ind     VARCHAR2,
      p_nat_key         VARCHAR2,
      p_audit           VARCHAR2 DEFAULT NULL,
      p_scd1            VARCHAR2 DEFAULT NULL,
      p_scd2            VARCHAR2 DEFAULT NULL
   );

   PROCEDURE modify_dim_attrib (
      p_mapping         VARCHAR2,
      p_column		VARCHAR2,
      p_column_type	VARCHAR2
   );

   PROCEDURE delete_dim_attribs (
      p_mapping      VARCHAR2
   );

   PROCEDURE create_cdc_source 
      (
        p_source_type        cdc_source.source_type%TYPE,
        p_service_name       cdc_source.service_name%TYPE,
        p_hostname           cdc_source.hostname%TYPE,
        p_port               cdc_source.port%TYPE,
        p_dblink             cdc_source.dblink_name%TYPE,
        p_ogg_group_key      cdc_source_external.ogg_group_key%TYPE     DEFAULT NULL,
        p_ogg_group_name     cdc_source_external.ogg_group_name%TYPE    DEFAULT NULL,
        p_ogg_check_table    cdc_source_external.ogg_check_table%TYPE   DEFAULT NULL,
        p_ogg_check_column   cdc_source_external.ogg_check_column%TYPE  DEFAULT NULL
      );      
      
   PROCEDURE modify_cdc_source 
      (
        p_source_id          cdc_source.source_id%TYPE,
        p_source_type        cdc_source.source_type%TYPE                DEFAULT NULL,
        p_service_name       cdc_source.service_name%TYPE               DEFAULT NULL,
        p_hostname           cdc_source.hostname%TYPE                   DEFAULT NULL,
        p_port               cdc_source.port%TYPE                       DEFAULT NULL,
        p_dblink             cdc_source.dblink_name%TYPE                DEFAULT NULL,
        p_ogg_group_key      cdc_source_external.ogg_group_key%TYPE     DEFAULT NULL,
        p_ogg_group_name     cdc_source_external.ogg_group_name%TYPE    DEFAULT NULL,
        p_ogg_check_table    cdc_source_external.ogg_check_table%TYPE   DEFAULT NULL,
        p_ogg_check_column   cdc_source_external.ogg_check_column%TYPE  DEFAULT NULL
      );
      
   PROCEDURE delete_cdc_source 
      (
        p_source_id          cdc_source.source_id%TYPE
      );
      
   PROCEDURE create_cdc_group 
      (
        p_group_name         cdc_group.group_name%TYPE,
        p_source_id          cdc_group.source_id%TYPE,
        p_filter_policy      cdc_group.filter_policy%TYPE,
        p_foundation         cdc_group.foundation%TYPE,
        p_subscription       cdc_group.subscription%TYPE               DEFAULT NULL,
        p_sub_prefix         cdc_group.sub_prefix%TYPE                 DEFAULT NULL
      );      
      
   PROCEDURE modify_cdc_group 
      (
        p_group_name         cdc_group.group_name%TYPE,
        p_source_id          cdc_group.source_id%TYPE                  DEFAULT NULL,
        p_filter_policy      cdc_group.filter_policy%TYPE              DEFAULT NULL,
        p_foundation         cdc_group.foundation%TYPE                 DEFAULT NULL,
        p_subscription       cdc_group.subscription%TYPE               DEFAULT NULL,
        p_sub_prefix         cdc_group.sub_prefix%TYPE                 DEFAULT NULL
      );
      
   PROCEDURE delete_cdc_group 
      (
        p_group_name         cdc_group.group_name%TYPE
      );
      
   PROCEDURE create_cdc_entity
      (
        p_source_owner  cdc_entity.source_owner%TYPE, 
        p_source_table  cdc_entity.source_table%TYPE, 
        p_group_id      cdc_entity.group_id%TYPE, 
        p_natkey_list   cdc_entity.natkey_list%TYPE, 
        p_table_name    cdc_entity.table_name%TYPE      DEFAULT NULL
      );
      
   PROCEDURE modify_cdc_entity
      (
        p_entity_id     cdc_entity.entity_id%TYPE,
        p_source_owner  cdc_entity.source_owner%TYPE    DEFAULT NULL, 
        p_source_table  cdc_entity.source_table%TYPE    DEFAULT NULL,
        p_group_id      cdc_entity.group_id%TYPE        DEFAULT NULL,
        p_natkey_list   cdc_entity.natkey_list%TYPE     DEFAULT NULL,
        p_table_name    cdc_entity.table_name%TYPE      DEFAULT NULL
      );
      
   PROCEDURE delete_cdc_entity 
      (
        p_entity_id     cdc_entity.entity_id%TYPE
      );
      
   PROCEDURE create_cdc_subscription
      (
        p_sub_name      cdc_subscription.sub_name%TYPE, 
        p_group_name    cdc_group.group_name%TYPE
      );
      
   PROCEDURE modify_cdc_subscription
      (
        p_sub_name        cdc_subscription.sub_name%TYPE,
        p_effect_scn      cdc_subscription.effective_scn%TYPE,
        p_expire_scn      cdc_subscription.expiration_scn%TYPE
      );
      
   PROCEDURE delete_cdc_subscription 
      (
        p_sub_name     cdc_subscription.sub_name%TYPE
      );
      
   PROCEDURE create_cdc_audit_datatype
      (
        p_group_name       cdc_group.group_name%TYPE, 
        p_column_name      cdc_audit_datatype.column_name%TYPE, 
        p_column_type      cdc_audit_datatype.column_type%TYPE,
        p_datatype         cdc_audit_datatype.datatype%TYPE
      );
      
   PROCEDURE delete_cdc_audit_datatype 
      (
        p_column_name     cdc_audit_datatype.column_name%TYPE,
        p_group_name      cdc_group.group_name%TYPE
      );

   PROCEDURE set_module_conf(
      p_module          VARCHAR2 DEFAULT all_modules,
      p_logging_level   NUMBER   DEFAULT 2,
      p_debug_level     NUMBER   DEFAULT 3,
      p_default_runmode VARCHAR2 DEFAULT 'runtime',
      p_registration    VARCHAR2 DEFAULT 'appinfo'
   );
      
   PROCEDURE set_logging_level(
      p_logging_level   NUMBER   DEFAULT 2,
      p_debug_level     NUMBER   DEFAULT 4
   );

   PROCEDURE set_session_parameter(
      p_name         VARCHAR2,
      p_value        VARCHAR2,
      p_module       VARCHAR2 DEFAULT all_modules 
   );

   PROCEDURE start_debug;

   PROCEDURE stop_debug;

END trans_adm;
/