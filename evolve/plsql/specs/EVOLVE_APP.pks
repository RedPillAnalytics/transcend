CREATE OR REPLACE PACKAGE evolve_app AUTHID CURRENT_USER
AS
   FUNCTION exec_sql( p_sql VARCHAR2, p_msg VARCHAR2 DEFAULT NULL, p_auto VARCHAR2 DEFAULT 'no' )
      RETURN NUMBER;

   PROCEDURE exec_sql(
      p_sql          VARCHAR2,
      p_msg          VARCHAR2 DEFAULT NULL,
      p_auto         VARCHAR2 DEFAULT 'no',
      p_background   VARCHAR2 DEFAULT 'no'
   );

   PROCEDURE submit_sql(
      p_sql         VARCHAR2,
      p_job_class   VARCHAR2 DEFAULT 'EVOLVE_DEFAULT_CLASS'
   );

   PROCEDURE coordinate_sql( p_sleep NUMBER DEFAULT 5, p_timeout NUMBER DEFAULT 0 );

   PROCEDURE consume_sql( p_session_id NUMBER, p_module VARCHAR2, p_action VARCHAR2, p_sql VARCHAR2 );
END evolve_app;
/