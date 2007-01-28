CREATE OR REPLACE PACKAGE tdinc.job
AS
   PROCEDURE log_msg(
      p_msg   VARCHAR2 );

   PROCEDURE log_err;

   PROCEDURE log_cnt(
      p_count   NUMBER );

   FUNCTION get_cnt(
      p_action   VARCHAR2 )
      RETURN NUMBER;

   PROCEDURE log_cnt_msg(
      p_count   NUMBER,
      p_msg     VARCHAR2 DEFAULT NULL );
END job;
/