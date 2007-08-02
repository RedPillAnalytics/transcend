CREATE OR REPLACE PACKAGE td_fileapi AUTHID CURRENT_USER
IS
   FUNCTION extract_query(
      p_query       VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT '|',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_append      VARCHAR2 DEFAULT 'no'
   )
      RETURN NUMBER;

   FUNCTION calc_rej_ind(
      p_filehub_group   VARCHAR2,
      p_filehub_name    VARCHAR2,
      p_rej_limit       NUMBER DEFAULT 20
   )
      RETURN VARCHAR2;

   PROCEDURE process_files(
      p_filehub_group   VARCHAR2,
      p_filehub_name    VARCHAR2 DEFAULT NULL,
      p_keep_source     VARCHAR2 DEFAULT 'no',
      p_runmode         VARCHAR2 DEFAULT NULL
   );
END td_fileapi;
/