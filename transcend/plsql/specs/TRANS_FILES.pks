CREATE OR REPLACE PACKAGE trans_files AUTHID CURRENT_USER
IS
   FUNCTION calc_rej_ind(
      p_file_group   VARCHAR2,
      p_file_label    VARCHAR2,
      p_rej_limit       NUMBER DEFAULT 20
   )
      RETURN VARCHAR2;

   PROCEDURE extract_object(
      p_owner       VARCHAR2,
      p_object      VARCHAR2,
      p_dirname     VARCHAR2,
      p_filename    VARCHAR2,
      p_delimiter   VARCHAR2 DEFAULT '|',
      p_quotechar   VARCHAR2 DEFAULT '',
      p_headers     VARCHAR2 DEFAULT 'yes',
      p_append      VARCHAR2 DEFAULT 'no'
   );

   PROCEDURE process_files(
      p_file_group   VARCHAR2,
      p_file_label    VARCHAR2 DEFAULT NULL
   );
END trans_files;
/