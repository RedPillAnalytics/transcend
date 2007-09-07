CREATE OR REPLACE PACKAGE td_sql AUTHID CURRENT_USER
AS
   FUNCTION exec_sql(
      p_sql    VARCHAR2,
      p_auto   VARCHAR2 DEFAULT 'no',
      p_msg    VARCHAR2 DEFAULT NULL
   )
      RETURN NUMBER;
      
   PROCEDURE exec_sql(
      p_sql    VARCHAR2,
      p_auto   VARCHAR2 DEFAULT 'no',
      p_msg    VARCHAR2 DEFAULT NULL
   );

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
      
   FUNCTION get_dir_path( p_dirname VARCHAR2 )
      RETURN VARCHAR2;

   FUNCTION get_dir_name( p_dir_path VARCHAR2 )
      RETURN VARCHAR2;

   FUNCTION table_exists( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN;

   FUNCTION is_part_table( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN;

   FUNCTION object_exists( p_owner VARCHAR2, p_object VARCHAR2 )
      RETURN BOOLEAN;

END td_sql;
/