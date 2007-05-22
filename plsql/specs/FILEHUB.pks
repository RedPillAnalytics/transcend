CREATE OR REPLACE PACKAGE tdinc.filehub
IS
   FUNCTION calc_rej_ind (
      p_filehub_group   VARCHAR2,
      p_filehub_name    VARCHAR2,
      p_rej_limit       NUMBER DEFAULT 20)
      RETURN VARCHAR2;

   PROCEDURE process (
      p_filehub_group   VARCHAR2,
      p_filehub_name    VARCHAR2 DEFAULT NULL,
      p_keep_source     varchar2 DEFAULT 'no',
      p_runmode         varchar2 DEFAULT null);
END filehub;
/