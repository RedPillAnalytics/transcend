CREATE OR REPLACE PACKAGE tdinc.filehub
IS
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
      p_append      BOOLEAN DEFAULT FALSE,
      p_headers     BOOLEAN DEFAULT FALSE)
      RETURN NUMBER;

   PROCEDURE audit_file (
      p_filehub_id      filehub_detail.filehub_id%TYPE,
      p_src_filename    filehub_detail.src_filename%TYPE DEFAULT NULL,
      p_trg_filename    filehub_detail.trg_filename%TYPE DEFAULT NULL,
      p_arch_filename   filehub_detail.arch_filename%TYPE,
      p_num_bytes       filehub_detail.num_bytes%TYPE,
      p_num_lines       filehub_detail.num_lines%TYPE DEFAULT NULL,
      p_file_dt         filehub_detail.file_dt%TYPE,
      p_debug           BOOLEAN DEFAULT FALSE);
END filehub;
/