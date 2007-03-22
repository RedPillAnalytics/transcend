CREATE OR REPLACE PACKAGE tdinc.filehub
IS
   FUNCTION audit_file (
      p_filehub_id      filehub_detail.filehub_id%TYPE,
      p_src_filename    filehub_detail.src_filename%TYPE DEFAULT NULL,
      p_trg_filename    filehub_detail.trg_filename%TYPE DEFAULT NULL,
      p_arch_filename   filehub_detail.arch_filename%TYPE,
      p_num_bytes       filehub_detail.num_bytes%TYPE,
      p_num_lines       filehub_detail.num_lines%TYPE DEFAULT NULL,
      p_file_dt         filehub_detail.file_dt%TYPE,
      p_debug           BOOLEAN DEFAULT FALSE)
      RETURN NUMBER;

   FUNCTION extract_query (
      p_query       VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT ',',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_append      BOOLEAN DEFAULT FALSE)
      RETURN NUMBER;

   FUNCTION extract_object (
      p_owner       VARCHAR2,
      p_object      VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT ',',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_headers     VARCHAR2 DEFAULT 'none',
      p_append      BOOLEAN DEFAULT FALSE,
      p_debug       BOOLEAN DEFAULT FALSE)
      RETURN NUMBER;

   PROCEDURE process_extract (
      p_filehub_id        filehub_conf.filehub_id%TYPE DEFAULT NULL,
      p_object_owner      filehub_conf.object_owner%TYPE DEFAULT NULL,
      p_object_name       filehub_conf.object_name%TYPE DEFAULT NULL,
      p_directory         filehub_conf.DIRECTORY%TYPE DEFAULT NULL,
      p_filename          filehub_conf.filename%TYPE DEFAULT NULL,
      p_arch_directory    filehub_conf.arch_directory%TYPE DEFAULT NULL,
      p_min_bytes         filehub_conf.min_bytes%TYPE DEFAULT NULL,
      p_max_bytes         filehub_conf.max_bytes%TYPE DEFAULT NULL,
      p_file_datestamp    filehub_conf.file_datestamp%TYPE DEFAULT NULL,
      p_dateformat        filehub_conf.DATEFORMAT%TYPE DEFAULT NULL,
      p_timestampformat   filehub_conf.timestampformat%TYPE DEFAULT NULL,
      p_notify            filehub_conf.notify%TYPE DEFAULT NULL,
      p_baseurl           filehub_conf.baseurl%TYPE DEFAULT NULL,
      p_delimiter         filehub_conf.delimiter%TYPE DEFAULT NULL,
      p_quotechar         filehub_conf.quotechar%TYPE DEFAULT NULL,
      p_headers           filehub_conf.headers%TYPE DEFAULT NULL,
      p_debug             BOOLEAN DEFAULT FALSE);
END filehub;
/