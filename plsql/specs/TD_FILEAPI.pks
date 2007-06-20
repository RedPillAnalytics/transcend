CREATE OR REPLACE PACKAGE td_fileapi
IS
   FUNCTION calc_rej_ind(
      p_filehub_group   VARCHAR2,
      p_filehub_name    VARCHAR2,
      p_rej_limit       NUMBER DEFAULT 20
   )
      RETURN VARCHAR2;

   PROCEDURE process_file(
      p_filehub_group   VARCHAR2,
      p_filehub_name    VARCHAR2 DEFAULT NULL,
      p_keep_source     VARCHAR2 DEFAULT 'no',
      p_runmode         VARCHAR2 DEFAULT NULL
   );
END td_fileapi;
/