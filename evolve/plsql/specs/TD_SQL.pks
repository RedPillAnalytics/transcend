CREATE OR REPLACE PACKAGE td_sql AUTHID CURRENT_USER
AS
   FUNCTION exec_sql(
      p_sql              VARCHAR2,
      p_auto             VARCHAR2 DEFAULT 'no',
      p_msg              VARCHAR2 DEFAULT NULL,
      p_override_debug   VARCHAR2 DEFAULT 'no'
   )
      RETURN NUMBER;

   PROCEDURE exec_sql(
      p_sql              VARCHAR2,
      p_auto             VARCHAR2 DEFAULT 'no',
      p_msg              VARCHAR2 DEFAULT NULL,
      p_override_debug   VARCHAR2 DEFAULT 'no'
   );

   PROCEDURE submit_sql(
      p_sql         VARCHAR2,
      p_msg         VARCHAR2 DEFAULT NULL,
      p_background  VARCHAR2 DEFAULT 'no',
      p_program	    VARCHAR2 DEFAULT 'consume_sql_job',
      p_job_class   VARCHAR2 DEFAULT 'DEFAULT_JOB_CLASS'
   );

   PROCEDURE consume_sql(
      p_session_id  NUMBER,
      p_module	    VARCHAR2,
      p_action	    VARCHAR2,
      p_sql         VARCHAR2,
      p_msg         VARCHAR2
   );
      
   PROCEDURE coordinate_sql(
      p_sleep    NUMBER DEFAULT 5,
      p_timeout	 NUMBER DEFAULT 0
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