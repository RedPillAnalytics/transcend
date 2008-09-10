CREATE OR REPLACE PACKAGE trans_adm AUTHID CURRENT_USER
IS
   null_value   CONSTANT VARCHAR2 (10) := '*null*';

   PROCEDURE set_default_configs;

   PROCEDURE create_feed (
      p_file_group         VARCHAR2,
      p_file_label         VARCHAR2,
      p_filename           VARCHAR2,
      p_directory	   VARCHAR2,
      p_arch_directory     VARCHAR2,
      p_source_directory   VARCHAR2,
      p_source_regexp      VARCHAR2,
      p_owner              VARCHAR2,
      p_table              VARCHAR2,
      p_match_parameter    VARCHAR2 DEFAULT 'i',
      p_source_policy      VARCHAR2 DEFAULT 'newest',
      p_required           VARCHAR2 DEFAULT 'yes',
      p_min_bytes          NUMBER   DEFAULT 0,
      p_max_bytes          NUMBER   DEFAULT 0,
      p_reject_limit       NUMBER   DEFAULT 100,
      p_file_datestamp     VARCHAR2 DEFAULT NULL,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_passphrase         VARCHAR2 DEFAULT NULL,
      p_delete_source      VARCHAR2 DEFAULT 'yes',
      p_file_description   VARCHAR2 DEFAULT NULL
   );
      
   PROCEDURE modify_feed (
      p_file_group         VARCHAR2,
      p_file_label         VARCHAR2,
      p_filename           VARCHAR2 DEFAULT NULL,
      p_directory	   VARCHAR2 DEFAULT NULL,
      p_arch_directory     VARCHAR2 DEFAULT NULL,
      p_source_directory   VARCHAR2 DEFAULT NULL,
      p_source_regexp      VARCHAR2 DEFAULT NULL,
      p_owner              VARCHAR2 DEFAULT NULL,
      p_table              VARCHAR2 DEFAULT NULL,
      p_match_parameter    VARCHAR2 DEFAULT NULL,
      p_source_policy      VARCHAR2 DEFAULT NULL,
      p_required           VARCHAR2 DEFAULT NULL,
      p_min_bytes          NUMBER   DEFAULT NULL,
      p_max_bytes          NUMBER   DEFAULT NULL,
      p_reject_limit       NUMBER   DEFAULT NULL,
      p_file_datestamp     VARCHAR2 DEFAULT NULL,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_passphrase         VARCHAR2 DEFAULT NULL,
      p_delete_source      VARCHAR2 DEFAULT NULL,
      p_file_description   VARCHAR2 DEFAULT NULL
   );
      
   PROCEDURE delete_feed (
      p_file_group         VARCHAR2,
      p_file_label         VARCHAR2
   );

   PROCEDURE create_extract (
      p_file_group         VARCHAR2,
      p_file_label         VARCHAR2,
      p_filename           VARCHAR2,
      p_object_owner       VARCHAR2,
      p_object_name        VARCHAR2,
      p_directory          VARCHAR2,
      p_arch_directory     VARCHAR2,
      p_min_bytes          NUMBER   DEFAULT 0,
      p_max_bytes          NUMBER   DEFAULT 0,
      p_file_datestamp     VARCHAR2 DEFAULT NULL,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_passphrase         VARCHAR2 DEFAULT NULL,
      p_dateformat         VARCHAR2 DEFAULT 'mm/dd/yyyy hh:mi:ss am',
      p_timestampformat    VARCHAR2 DEFAULT 'mm/dd/yyyy hh:mi:ss:x:ff am',
      p_delimiter          VARCHAR2 DEFAULT ',',
      p_quotechar          VARCHAR2 DEFAULT NULL,
      p_headers            VARCHAR2 DEFAULT 'yes',
      p_file_description   VARCHAR2 DEFAULT NULL
   );
      
   
   PROCEDURE modify_extract (
      p_file_group         VARCHAR2,
      p_file_label         VARCHAR2,
      p_filename           VARCHAR2 DEFAULT NULL,
      p_object_owner       VARCHAR2 DEFAULT NULL,
      p_object_name        VARCHAR2 DEFAULT NULL,
      p_directory          VARCHAR2 DEFAULT NULL,
      p_arch_directory     VARCHAR2 DEFAULT NULL,
      p_min_bytes          NUMBER DEFAULT NULL,
      p_max_bytes          NUMBER DEFAULT NULL,
      p_file_datestamp     VARCHAR2 DEFAULT NULL,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_passphrase         VARCHAR2 DEFAULT NULL,
      p_dateformat         VARCHAR2 DEFAULT NULL,
      p_timestampformat    VARCHAR2 DEFAULT NULL,
      p_delimiter          VARCHAR2 DEFAULT NULL,
      p_quotechar          VARCHAR2 DEFAULT NULL,
      p_headers            VARCHAR2 DEFAULT NULL,
      p_file_description   VARCHAR2 DEFAULT NULL,
      p_mode               VARCHAR2 DEFAULT 'upsert'
   );   
      
   PROCEDURE delete_extract (
      p_file_group         VARCHAR2,
      p_file_label         VARCHAR2
   );
   
   PROCEDURE create_mapping (
      p_mapping             VARCHAR2,
      p_owner               VARCHAR2 DEFAULT NULL,
      p_table               VARCHAR2 DEFAULT NULL,
      p_partname            VARCHAR2 DEFAULT NULL,
      p_indexes             VARCHAR2 DEFAULT 'no',
      p_constraints         VARCHAR2 DEFAULT 'no',
      p_source_owner        VARCHAR2 DEFAULT NULL,
      p_source_object       VARCHAR2 DEFAULT NULL,
      p_source_column       VARCHAR2 DEFAULT NULL,
      p_replace_method      VARCHAR2 DEFAULT NULL,
      p_statistics          VARCHAR2 DEFAULT 'transfer',
      p_concurrent          VARCHAR2 DEFAULT 'no',
      p_index_regexp        VARCHAR2 DEFAULT NULL,
      p_index_type          VARCHAR2 DEFAULT NULL,
      p_part_type           VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_description         VARCHAR2 DEFAULT NULL
   );

   PROCEDURE modify_mapping (
      p_mapping             VARCHAR2,
      p_owner               VARCHAR2 DEFAULT NULL,
      p_table               VARCHAR2 DEFAULT NULL,
      p_partname            VARCHAR2 DEFAULT NULL,
      p_indexes             VARCHAR2 DEFAULT 'no',
      p_constraints         VARCHAR2 DEFAULT 'no',
      p_source_owner        VARCHAR2 DEFAULT NULL,
      p_source_object       VARCHAR2 DEFAULT NULL,
      p_source_column       VARCHAR2 DEFAULT NULL,
      p_replace_method      VARCHAR2 DEFAULT NULL,
      p_statistics          VARCHAR2 DEFAULT 'transfer',
      p_concurrent          VARCHAR2 DEFAULT 'no',
      p_index_regexp        VARCHAR2 DEFAULT NULL,
      p_index_type          VARCHAR2 DEFAULT NULL,
      p_part_type           VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
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
      p_source_object      VARCHAR2,
      p_sequence_owner     VARCHAR2,
      p_sequence_name      VARCHAR2,
      p_staging_owner      VARCHAR2 DEFAULT NULL,
      p_staging_table      VARCHAR2 DEFAULT NULL,
      p_default_scd_type   NUMBER DEFAULT 2,
      p_direct_load        VARCHAR2 DEFAULT 'yes',
      p_replace_method     VARCHAR2 DEFAULT 'rename',
      p_statistics         VARCHAR2 DEFAULT 'transfer',
      p_concurrent         VARCHAR2 DEFAULT 'no',
      p_stage_key_def      NUMBER DEFAULT -.01,
      p_char_nvl_def       VARCHAR2 DEFAULT '~',
      p_date_nvl_def       DATE DEFAULT TO_DATE ('01/01/9999'),
      p_num_nvl_def        NUMBER DEFAULT -.01,
      p_description        VARCHAR2 DEFAULT NULL
   );

   PROCEDURE modify_dimension (
      p_owner              VARCHAR2,
      p_table              VARCHAR2,
      p_mapping            VARCHAR2 DEFAULT NULL,
      p_source_owner       VARCHAR2 DEFAULT NULL,
      p_source_object      VARCHAR2 DEFAULT NULL,
      p_sequence_owner     VARCHAR2 DEFAULT NULL,
      p_sequence_name      VARCHAR2 DEFAULT NULL,
      p_staging_owner      VARCHAR2 DEFAULT NULL,
      p_staging_table      VARCHAR2 DEFAULT NULL,
      p_default_scd_type   NUMBER DEFAULT NULL,
      p_direct_load        VARCHAR2 DEFAULT NULL,
      p_replace_method     VARCHAR2 DEFAULT NULL,
      p_statistics         VARCHAR2 DEFAULT NULL,
      p_concurrent         VARCHAR2 DEFAULT NULL,
      p_stage_key_def      NUMBER DEFAULT NULL,
      p_char_nvl_def       VARCHAR2 DEFAULT NULL,
      p_date_nvl_def       DATE DEFAULT NULL,
      p_num_nvl_def        NUMBER DEFAULT NULL,
      p_description        VARCHAR2 DEFAULT NULL
  );
   
   PROCEDURE delete_dimension ( p_owner VARCHAR2, p_table VARCHAR2 );
   
   PROCEDURE create_dim_attribs (
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_surrogate       VARCHAR2,
      p_effective_dt    VARCHAR2,
      p_expiration_dt   VARCHAR2,
      p_current_ind     VARCHAR2,
      p_nat_key         VARCHAR2,
      p_scd1            VARCHAR2 DEFAULT NULL,
      p_scd2            VARCHAR2 DEFAULT NULL
   );

   PROCEDURE modify_dim_attrib (
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_column		VARCHAR2,
      p_column_type	VARCHAR2
   );

   PROCEDURE delete_dim_attribs (
      p_owner           VARCHAR2,
      p_table           VARCHAR2
   );

END trans_adm;
/