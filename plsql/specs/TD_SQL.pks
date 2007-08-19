CREATE OR REPLACE PACKAGE td_sql AUTHID CURRENT_USER
AS
   FUNCTION exec_sql(
      p_sql    VARCHAR2,
      p_auto   VARCHAR2 DEFAULT 'no',
      p_msg    VARCHAR2 DEFAULT NULL
   )
      RETURN NUMBER;

   PROCEDURE check_table(
      p_owner         VARCHAR2,
      p_table         VARCHAR2,
      p_partname      VARCHAR2 DEFAULT NULL,
      p_partitioned   VARCHAR2 DEFAULT NULL,
      p_iot           VARCHAR2 DEFAULT NULL,
      p_compressed    VARCHAR2 DEFAULT NULL
   );

   PROCEDURE check_object(
      p_owner         VARCHAR2,
      p_object        VARCHAR2,
      p_object_type   VARCHAR2 DEFAULT NULL
   );
END td_sql;
/