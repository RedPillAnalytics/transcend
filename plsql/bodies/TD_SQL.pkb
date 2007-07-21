CREATE OR REPLACE PACKAGE BODY td_core
AS
   -- use EXECUTE IMMEDIATE to execute a SQL statement
   -- uses AUTONOMOUS_TRANSACTION, so this will NOT execute within the current transaction
   -- excellent for DDL that where the commit incurred by the DDL will not affect the current transaction
   FUNCTION exec_auto( p_sql VARCHAR2, p_runmode VARCHAR2 DEFAULT NULL )
      RETURN NUMBER
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_results   NUMBER;
      o_app       applog := applog( p_module => 'exec_auto', p_runmode => p_runmode );
   BEGIN
      IF NOT o_app.is_debugmode
      THEN
         EXECUTE IMMEDIATE p_sql;

         l_results := SQL%ROWCOUNT;
      END IF;

      COMMIT;
      o_app.clear_app_info;
      RETURN l_results;
   END exec_auto;

   -- use EXECUTE IMMEDIATE to execute a SQL statement
   -- no AUTONOMOUS_TRANSACTION, so this will execute within the current transaction
   FUNCTION exec_sql(
      p_sql       VARCHAR2,
      p_auto      VARCHAR2 DEFAULT 'no',
      p_msg       VARCHAR2 DEFAULT NULL,
      p_runmode   VARCHAR2 DEFAULT NULL
   )
      RETURN NUMBER
   AS
      l_results   NUMBER;
      o_app       applog := applog( p_module => 'exec_sql', p_runmode => p_runmode );
   BEGIN
      o_app.log_msg( CASE
                        WHEN p_msg IS NULL
                           THEN 'SQL: ' || p_sql
                        ELSE p_msg
                     END, 3 );

      IF NOT o_app.is_debugmode
      THEN
         IF is_true( p_auto )
         THEN
            l_results := exec_auto( p_sql => p_sql, p_runmode => o_app.runmode );
         ELSE
            EXECUTE IMMEDIATE p_sql;

            l_results := SQL%ROWCOUNT;
         END IF;
      END IF;

      o_app.clear_app_info;
      RETURN l_results;
   END exec_sql;

   -- used to get the path associated with a directory location
   FUNCTION get_dir_path( p_dirname VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_path   all_directories.directory_path%TYPE;
   BEGIN
      SELECT directory_path
        INTO l_path
        FROM all_directories
       WHERE directory_name = UPPER( p_dirname );

      RETURN l_path;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         raise_application_error( -20010, 'Directory object does not exist' );
   END get_dir_path;

   -- used to get a directory name associated with a directory path
   -- this assumes that there is a one-to-one of directory names to directory paths
   -- that is not required with oracle... there can be multiple directory objects pointing to the same directory
   FUNCTION get_dir_name( p_dir_path VARCHAR2 )
      RETURN VARCHAR2
   AS
      l_dirname   all_directories.directory_name%TYPE;
   BEGIN
      SELECT directory_name
        INTO l_dirname
        FROM all_directories
       WHERE directory_path = p_dir_path;

      RETURN l_dirname;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         raise_application_error( -20011,
                                  'No directory object defined for the specified path'
                                );
      WHEN TOO_MANY_ROWS
      THEN
         raise_application_error
                        ( -20012,
                          'More than one directory object defined for the specified path'
                        );
   END get_dir_name;

   -- returns a boolean
   -- does a check to see if a table exists
   FUNCTION table_exists( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN
   AS
      l_table   dba_tables.table_name%TYPE;
   BEGIN
      SELECT table_name
        INTO l_table
        FROM dba_tables
       WHERE owner = UPPER( p_owner ) AND table_name = UPPER( p_table );

      RETURN TRUE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN FALSE;
   END table_exists;

   -- returns a boolean
   -- does a check to see if table is partitioned
   FUNCTION is_part_table( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN BOOLEAN
   AS
      l_partitioned   dba_tables.partitioned%TYPE;
   BEGIN
      IF NOT table_exists( UPPER( p_owner ), UPPER( p_table ))
      THEN
         raise_application_error( get_err_cd( 'no_tab' ),
                                  get_err_msg( 'no_tab' ) || ': ' || p_owner || '.'
                                  || p_table
                                );
      END IF;

      SELECT partitioned
        INTO l_partitioned
        FROM dba_tables
       WHERE owner = UPPER( p_owner ) AND table_name = UPPER( p_table );

      CASE
         WHEN is_true( l_partitioned )
         THEN
            RETURN TRUE;
         WHEN NOT is_true( l_partitioned )
         THEN
            RETURN FALSE;
      END CASE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN FALSE;
   END is_part_table;

   -- returns a boolean
   -- does a check to see if a object exists
   FUNCTION object_exists( p_owner VARCHAR2, p_object VARCHAR2 )
      RETURN BOOLEAN
   AS
      l_object   dba_objects.object_name%TYPE;
      o_app      applog                         := applog( p_module => 'object_exists' );
   BEGIN
      SELECT DISTINCT object_name
                 INTO l_object
                 FROM dba_objects
                WHERE owner = UPPER( p_owner ) AND object_name = UPPER( p_object );

      o_app.clear_app_info;
      RETURN TRUE;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         o_app.clear_app_info;
         RETURN FALSE;
   END object_exists;

   -- checks things about a table depending on the parameters passed
   -- raises an exception if the specified things are not true
   PROCEDURE check_table(
      p_owner         VARCHAR2,
      p_table         VARCHAR2,
      p_partname      VARCHAR2 DEFAULT NULL,
      p_partitioned   VARCHAR2 DEFAULT NULL,
      p_iot           VARCHAR2 DEFAULT NULL,
      p_compressed    VARCHAR2 DEFAULT NULL
   )
   AS
      l_tab_name         VARCHAR2( 61 )     := UPPER( p_owner ) || '.'
                                               || UPPER( p_table );
      l_part_name        VARCHAR2( 92 )       := l_tab_name || ':' || UPPER( p_partname );
      l_partitioned      VARCHAR2( 3 );
      l_iot              VARCHAR2( 3 );
      l_compressed       VARCHAR2( 3 );
      l_partition_name   dba_tab_partitions.partition_name%TYPE;
      o_app              applog                  := applog( p_module      => 'object_exists' );
   BEGIN
      BEGIN
         SELECT CASE
                   WHEN compression = 'DISABLED'
                      THEN 'no'
                   WHEN compression = 'N/A'
                      THEN 'no'
                   WHEN compression IS NULL
                      THEN 'no'
                   ELSE 'yes'
                END,
                LOWER( partitioned ) partitioned,
                CASE iot_type
                   WHEN 'IOT'
                      THEN 'yes'
                   ELSE 'no'
                END iot
           INTO l_compressed,
                l_partitioned,
                l_iot
           FROM dba_tables
          WHERE owner = UPPER( p_owner ) AND table_name = UPPER( p_table );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            raise_application_error( get_err_cd( 'no_tab' ),
                                     get_err_msg( 'no_tab' ) || ': ' || l_tab_name
                                   );
      END;

      IF l_partitioned = 'yes' AND p_partname IS NULL AND p_compressed IS NOT NULL
      THEN
         raise_application_error
                        ( get_err_cd( 'parms_not_compatible' ),
                             get_err_msg( 'parms_not_compatible' )
                          || ': '
                          || 'P_COMPRESSED requires P_PARTNAME when the table is partitioned'
                        );
      END IF;

      IF p_partname IS NOT NULL
      THEN
         IF l_partitioned = 'no'
         THEN
            raise_application_error( get_err_cd( 'not_partitioned' ),
                                     get_err_msg( 'not_partitioned' ) || ': '
                                     || l_tab_name
                                   );
         END IF;

         BEGIN
            SELECT CASE
                      WHEN compression = 'DISABLED'
                         THEN 'no'
                      WHEN compression = 'N/A'
                         THEN 'no'
                      WHEN compression IS NULL
                         THEN 'no'
                      ELSE 'yes'
                   END
              INTO l_compressed
              FROM dba_tab_partitions
             WHERE table_owner = UPPER( p_owner )
               AND table_name = UPPER( p_table )
               AND partition_name = UPPER( p_partname );
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               raise_application_error( get_err_cd( 'no_part' ),
                                        get_err_msg( 'no_part' ) || ': ' || l_part_name
                                      );
         END;
      END IF;

      CASE
         WHEN is_true( p_partitioned, TRUE ) AND NOT is_true( l_partitioned )
         THEN
            raise_application_error( get_err_cd( 'not_partitioned' ),
                                     get_err_msg( 'not_partitioned' ) || ': '
                                     || l_tab_name
                                   );
         WHEN NOT is_true( p_partitioned, TRUE ) AND is_true( l_partitioned )
         THEN
            raise_application_error( get_err_cd( 'partitioned' ),
                                     get_err_msg( 'partitioned' ) || ': ' || l_tab_name
                                   );
         WHEN is_true( p_iot, TRUE ) AND NOT is_true( l_iot )
         THEN
            raise_application_error( get_err_cd( 'not_iot' ),
                                     get_err_msg( 'not_iot' ) || ': ' || l_tab_name
                                   );
         WHEN NOT is_true( p_iot, TRUE ) AND is_true( l_iot )
         THEN
            raise_application_error( get_err_cd( 'iot' ),
                                     get_err_msg( 'iot' ) || ': ' || l_tab_name
                                   );
         WHEN is_true( p_compressed, TRUE ) AND NOT is_true( l_compressed )
         THEN
            raise_application_error( get_err_cd( 'not_compressed' ),
                                        get_err_msg( 'not_compressed' )
                                     || ': '
                                     || CASE
                                           WHEN p_partname IS NULL
                                              THEN l_tab_name
                                           ELSE l_part_name
                                        END
                                   );
         WHEN NOT is_true( p_compressed, TRUE ) AND is_true( l_compressed )
         THEN
            raise_application_error( get_err_cd( 'compressed' ),
                                        get_err_msg( 'compressed' )
                                     || ': '
                                     || CASE
                                           WHEN p_partname IS NULL
                                              THEN l_tab_name
                                           ELSE l_part_name
                                        END
                                   );
         ELSE
            NULL;
      END CASE;
   END check_table;
END td_core;
/