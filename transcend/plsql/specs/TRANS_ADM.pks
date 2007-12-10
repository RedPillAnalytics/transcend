CREATE OR REPLACE PACKAGE trans_adm AUTHID CURRENT_USER
IS
   PROCEDURE set_default_configs;

   PROCEDURE configure_file(
      p_file_label         VARCHAR2,
      p_file_group         VARCHAR2,
      p_file_description   VARCHAR2,
      p_object_owner       VARCHAR2,
      p_object_name        VARCHAR2,
      p_directory          VARCHAR2,
      p_filename           VARCHAR2,
      p_arch_directory     VARCHAR2,
      p_min_bytes          NUMBER,
      p_max_bytes          NUMBER,
      p_file_datestamp     VARCHAR2,
      p_baseurl            VARCHAR2,
      p_passphrase         VARCHAR2,
      p_source_directory   VARCHAR2,
      p_source_regexp      VARCHAR2,
      p_regexp_options     VARCHAR2,
      p_source_policy      VARCHAR2,
      p_required           VARCHAR2,
      p_delete_source      VARCHAR2,
      p_reject_limit       NUMBER
   );

   PROCEDURE configure_file(
      p_file_label         VARCHAR2,
      p_file_group         VARCHAR2,
      p_file_description   VARCHAR2,
      p_object_owner       VARCHAR2,
      p_object_name        VARCHAR2,
      p_directory          VARCHAR2,
      p_filename           VARCHAR2,
      p_arch_directory     VARCHAR2,
      p_min_bytes          NUMBER,
      p_max_bytes          NUMBER,
      p_file_datestamp     VARCHAR2,
      p_baseurl            VARCHAR2,
      p_dateformat         VARCHAR2,
      p_timestampformat    VARCHAR2,
      p_delimiter          VARCHAR2,
      p_quotechar          VARCHAR2,
      p_headers            VARCHAR2
   );
END trans_adm;
/