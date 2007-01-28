CREATE OR REPLACE PACKAGE tdinc.core_utils
AS
   PROCEDURE log_msg(
      p_msg   log_table.msg%TYPE );

   FUNCTION get_dir_path(
      p_dirname   VARCHAR2 )
      RETURN VARCHAR2;

   FUNCTION get_dir_name(
      p_dir_path   VARCHAR2 )
      RETURN VARCHAR2;

   FUNCTION get_numlines(
      p_dirname    IN   VARCHAR2,
      p_filename   IN   VARCHAR2 )
      RETURN NUMBER;

   FUNCTION unzip_file(
      p_dirpath    VARCHAR2,
      p_filename   VARCHAR2,
      p_debug      BOOLEAN DEFAULT FALSE )
      RETURN VARCHAR2;

   FUNCTION decrypt_file(
      p_dirpath      VARCHAR2,
      p_filename     VARCHAR2,
      p_passphrase   VARCHAR2,
      p_debug        BOOLEAN DEFAULT FALSE )
      RETURN VARCHAR2;
END core_utils;
/