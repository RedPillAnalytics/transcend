CREATE OR REPLACE PACKAGE trans_adm AUTHID CURRENT_USER
IS
   PROCEDURE set_default_configs;

   PROCEDURE configure_file(
      p_file_label         VARCHAR2,
      p_file_group         VARCHAR2,
      p_file_description   VARCHAR2 DEFAULT NULL,
      p_object_owner       VARCHAR2 DEFAULT NULL,
      p_object_name        VARCHAR2 DEFAULT NULL,
      p_directory          VARCHAR2 DEFAULT NULL,
      p_filename           VARCHAR2 DEFAULT NULL,
      p_arch_directory     VARCHAR2 DEFAULT NULL,
      p_min_bytes          NUMBER DEFAULT 0,
      p_max_bytes          NUMBER DEFAULT 0,
      p_file_datestamp     VARCHAR2 DEFAULT NULL,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_passphrase         VARCHAR2 DEFAULT NULL,
      p_source_directory   VARCHAR2 DEFAULT NULL,
      p_source_regexp      VARCHAR2 DEFAULT NULL,
      p_regexp_options     VARCHAR2 DEFAULT 'i',
      p_source_policy      VARCHAR2 DEFAULT 'newest',
      p_required           VARCHAR2 DEFAULT 'yes',
      p_delete_source      VARCHAR2 DEFAULT 'yes',
      p_reject_limit       NUMBER   DEFAULT 100,
      p_mode               VARCHAR2 DEFAULT 'upsert'
   );

   PROCEDURE configure_file(
      p_file_label         VARCHAR2,
      p_file_group         VARCHAR2,
      p_file_description   VARCHAR2 DEFAULT NULL,
      p_object_owner       VARCHAR2 DEFAULT NULL,
      p_object_name        VARCHAR2 DEFAULT NULL,
      p_directory          VARCHAR2 DEFAULT NULL,
      p_filename           VARCHAR2 DEFAULT NULL,
      p_arch_directory     VARCHAR2 DEFAULT NULL,
      p_min_bytes          NUMBER DEFAULT 0,
      p_max_bytes          NUMBER DEFAULT 0,
      p_file_datestamp     VARCHAR2 DEFAULT NULL,
      p_baseurl            VARCHAR2 DEFAULT NULL,
      p_dateformat         VARCHAR2 DEFAULT 'mm/dd/yyyy hh:mi:ss am',
      p_timestampformat    VARCHAR2 DEFAULT 'mm/dd/yyyy hh:mi:ss:x:ff am',
      p_delimiter          VARCHAR2 DEFAULT ',',
      p_quotechar          VARCHAR2 DEFAULT NULL,
      p_headers            VARCHAR2 DEFAULT 'yes',
      p_mode		   VARCHAR2 DEFAULT 'upsert'
   );
END trans_adm;
/