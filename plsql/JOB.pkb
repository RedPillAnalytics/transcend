CREATE OR REPLACE PACKAGE BODY tdinc.job
AS
   -- used to write a standard message to the LOG_TABLE
   PROCEDURE log_msg(
      p_msg   VARCHAR2 )          -- P_MSG is simply the text that will be written to the LOG_TABLE
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_whence   VARCHAR2( 1024 );
      l_code     NUMBER                        DEFAULT SQLCODE;
      l_msg      log_table.msg%TYPE;
      l_scn      v$database.current_scn%TYPE;
   BEGIN
      -- still write as much to the logfile if we can even if it's too large for the log table
      BEGIN
         l_msg := p_msg;
      EXCEPTION
         WHEN VALUE_ERROR
         THEN
            l_msg := SUBSTR( l_msg,
                             0,
                             1998 ) || '>>';
      END;

      -- find out what called me
      l_whence := utility.whence;

      -- get the current_scn
      SELECT current_scn
        INTO l_scn
        FROM v$database;

      -- write the record to the log table
      INSERT INTO log_table
                  ( entry_ts,
                    session_id,
                    current_scn,
                    instance_name,
                    msg,
                    code,
                    client_info,
                    module,
                    action,
                    call_stack,
                    back_trace )
           VALUES ( CURRENT_TIMESTAMP,
                    SYS_CONTEXT( 'USERENV', 'SESSIONID' ),
                    l_scn,
                    SYS_CONTEXT( 'USERENV', 'INSTANCE_NAME' ),
                    l_msg,
                    l_code,
                    NVL( SYS_CONTEXT( 'USERENV', 'CLIENT_INFO' ), 'Not Set' ),
                    NVL( SYS_CONTEXT( 'USERENV', 'MODULE' ), 'Not Set' ),
                    NVL( SYS_CONTEXT( 'USERENV', 'ACTION' ), 'Not Set' ),
                    l_whence,
                    REGEXP_REPLACE( SUBSTR( DBMS_UTILITY.format_error_backtrace,
                                            1,
                                            4000 ),
                                    '[[:cntrl:]]',
                                    '; ' ));

      COMMIT;
      -- also output the message to the screen
      -- the client can control whether or not they want to see this
      -- in sqlplus, just SET SERVEROUTPUT ON or OFF
      DBMS_OUTPUT.put_line( p_msg );
   END log_msg;

   PROCEDURE log_err
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_msg   VARCHAR2( 1020 ) DEFAULT SQLERRM;
   BEGIN
      log_msg( l_msg );
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         RAISE;
   END log_err;

   PROCEDURE log_cnt_msg(
      p_count   NUMBER,
      p_msg     VARCHAR2 DEFAULT NULL )
   AS
   BEGIN
      log_cnt( p_count );
      -- if a message was provided to this procedure, then write it to the log table
      -- if not, then simply use the default message below
      log_msg( NVL( p_msg, 'Number of records selected/affected: ' || p_count ));
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_err;
   END log_cnt_msg;

   PROCEDURE log_cnt(
      p_count   NUMBER )
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_client_info     VARCHAR2( 64 );
      l_module          VARCHAR2( 48 );
      l_action          VARCHAR2( 32 );
      l_sessionid       NUMBER;
      l_instance_name   VARCHAR2( 30 );
   BEGIN
      DBMS_APPLICATION_INFO.read_client_info( l_client_info );
      DBMS_APPLICATION_INFO.read_module( l_module, l_action );
      l_sessionid := SYS_CONTEXT( 'USERENV', 'SESSIONID' );
      l_instance_name := SYS_CONTEXT( 'USERENV', 'INSTANCE_NAME' );

      -- store in COUNT_TABLE numbers of records affected by particular actions in modules
      INSERT INTO count_table
                  ( entry_ts,
                    session_id,
                    instance_name,
                    row_cnt,
                    client_info,
                    module,
                    action )
           VALUES ( CURRENT_TIMESTAMP,
                    l_sessionid,
                    l_instance_name,
                    p_count,
                    NVL( l_client_info, 'Not Set' ),
                    NVL( l_module, 'Not Set' ),
                    NVL( l_action, 'Not Set' ));

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_err;
   END log_cnt;

   -- this function can be used to get the count of a particular action
   FUNCTION get_cnt(
      p_action   VARCHAR2 )
      RETURN NUMBER
   AS
      l_client_info   VARCHAR2( 64 );
      l_sessionid     NUMBER;
      l_count         NUMBER;
   BEGIN
      DBMS_APPLICATION_INFO.read_client_info( l_client_info );
      l_sessionid := SYS_CONTEXT( 'USERENV', 'SESSIONID' );

      SELECT row_cnt
        INTO l_count
        FROM count_table
       WHERE client_info = l_client_info
         AND session_id = l_sessionid
         AND action = p_action;

      RETURN l_count;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_err;
   END get_cnt;

   PROCEDURE log_header
   AS
      l_output   VARCHAR2( 255 );
      l_app      app_info
                        := app_info( p_module =>      'JOB.LOG_HEADER',
                                     p_action =>      'Write log header' );
   BEGIN
      l_output :=
            'New session starting from '
         || 'MACHINE: '
         || SYS_CONTEXT( 'USERENV', 'HOST' )
         || '['
         || SYS_CONTEXT( 'USERENV', 'IP_ADDRESS' )
         || '], '
         || 'DBUSER: '
         || SYS_CONTEXT( 'USERENV', 'SESSION_USER' )
         || ', '
         || 'OSUSER: '
         || SYS_CONTEXT( 'USERENV', 'OS_USER' );
      log_msg( l_output );
      COMMIT;
      l_app.clear_app_info;
   END log_header;
BEGIN
   log_header;
END job;
/