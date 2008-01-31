CREATE OR REPLACE PACKAGE trans_adm AUTHID CURRENT_USER
IS
   PROCEDURE set_default_configs;

   PROCEDURE configure_feed(
      p_file_group         VARCHAR2,
      p_file_label         VARCHAR2,
      p_filename           VARCHAR2 DEFAULT NULL,
      p_table_owner        VARCHAR2 DEFAULT NULL,
      p_table_name         VARCHAR2 DEFAULT NULL,
      p_arch_directory     VARCHAR2 DEFAULT NULL,
      p_min_bytes          NUMBER DEFAULT NULL,
      p_max_bytes          NUMBER DEFAULT NULL,
      p_file_datestamp     VARCHAR2 DEFAULT NULL,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_passphrase         VARCHAR2 DEFAULT NULL,
      p_source_directory   VARCHAR2 DEFAULT NULL,
      p_source_regexp      VARCHAR2 DEFAULT NULL,
      p_regexp_options     VARCHAR2 DEFAULT NULL,
      p_source_policy      VARCHAR2 DEFAULT NULL,
      p_required           VARCHAR2 DEFAULT NULL,
      p_delete_source      VARCHAR2 DEFAULT NULL,
      p_reject_limit       NUMBER DEFAULT NULL,
      p_file_description   VARCHAR2 DEFAULT NULL,
      p_mode               VARCHAR2 DEFAULT 'upsert'
   );

   PROCEDURE configure_extract(
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

   PROCEDURE configure_mapping(
      p_mapping             VARCHAR2,
      p_owner               VARCHAR2 DEFAULT NULL,
      p_table               VARCHAR2 DEFAULT NULL,
      p_partname            VARCHAR2 DEFAULT NULL,
      p_indexes             VARCHAR2 DEFAULT 'no',
      p_constraints         VARCHAR2 DEFAULT 'no',
      p_source_owner        VARCHAR2 DEFAULT NULL,
      p_source_object       VARCHAR2 DEFAULT NULL,
      p_source_column       VARCHAR2 DEFAULT NULL,
      p_exchange            VARCHAR2 DEFAULT 'no',
      p_statistics          VARCHAR2 DEFAULT 'transfer',
      p_concurrent          VARCHAR2 DEFAULT 'no',
      p_index_regexp        VARCHAR2 DEFAULT NULL,
      p_index_type          VARCHAR2 DEFAULT NULL,
      p_part_type           VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_description         VARCHAR2 DEFAULT NULL,
      p_mode                VARCHAR2 DEFAULT 'upsert'
   );

   PROCEDURE configure_dim(
      p_owner              VARCHAR2,
      p_table              VARCHAR2,
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
      p_description        VARCHAR2 DEFAULT NULL,
      p_mode               VARCHAR2 DEFAULT 'upsert'
   );

   PROCEDURE configure_dim_cols(
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_surrogate       VARCHAR2 DEFAULT NULL,
      p_nat_key         VARCHAR2 DEFAULT NULL,
      p_scd1            VARCHAR2 DEFAULT NULL,
      p_scd2            VARCHAR2 DEFAULT NULL,
      p_effective_dt    VARCHAR2 DEFAULT NULL,
      p_expiration_dt   VARCHAR2 DEFAULT NULL,
      p_current_ind     VARCHAR2 DEFAULT NULL
   );
END trans_adm;
/