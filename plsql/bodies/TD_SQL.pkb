CREATE OR REPLACE PACKAGE BODY td_sql
AS
   -- use EXECUTE IMMEDIATE to execute a SQL statement
   -- uses AUTONOMOUS_TRANSACTION, so this will NOT execute within the current transaction
   -- excellent for DDL that where the commit incurred by the DDL will not affect the current transaction
   FUNCTION exec_auto( p_sql VARCHAR2 )
      RETURN NUMBER
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_results   NUMBER;
   BEGIN
      IF NOT td_inst.is_debugmode
      THEN
         EXECUTE IMMEDIATE p_sql;

         l_results := SQL%ROWCOUNT;
      END IF;

      COMMIT;
      RETURN l_results;
   END exec_auto;

   -- use EXECUTE IMMEDIATE to execute a SQL statement
   -- no AUTONOMOUS_TRANSACTION, so this will execute within the current transaction
   FUNCTION exec_sql(
      p_sql    VARCHAR2,
      p_auto   VARCHAR2 DEFAULT 'no',
      p_msg    VARCHAR2 DEFAULT NULL
   )
      RETURN NUMBER
   AS
      l_results   NUMBER;
   BEGIN
      td_inst.log_msg( CASE
                          WHEN p_msg IS NULL
                             THEN 'SQL: ' || p_sql
                          ELSE p_msg
                       END, 3 );

      IF NOT td_inst.is_debugmode
      THEN
         IF td_ext.is_true( p_auto )
         THEN
            l_results := exec_auto( p_sql => p_sql );
         ELSE
            EXECUTE IMMEDIATE p_sql;

            l_results := SQL%ROWCOUNT;
         END IF;
      END IF;

      RETURN l_results;
   END exec_sql;

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
      l_partition_name   all_tab_partitions.partition_name%TYPE;
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
           FROM all_tables
          WHERE owner = UPPER( p_owner ) AND table_name = UPPER( p_table );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            raise_application_error( td_inst.get_err_cd( 'no_tab' ),
                                     td_inst.get_err_msg( 'no_tab' ) || ': ' || l_tab_name
                                   );
      END;

      IF l_partitioned = 'yes' AND p_partname IS NULL AND p_compressed IS NOT NULL
      THEN
         raise_application_error
                        ( td_inst.get_err_cd( 'parms_not_compatible' ),
                             td_inst.get_err_msg( 'parms_not_compatible' )
                          || ': '
                          || 'P_COMPRESSED requires P_PARTNAME when the table is partitioned'
                        );
      END IF;

      IF p_partname IS NOT NULL
      THEN
         IF l_partitioned = 'no'
         THEN
            raise_application_error( td_inst.get_err_cd( 'not_partitioned' ),
                                        td_inst.get_err_msg( 'not_partitioned' )
                                     || ': '
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
              FROM all_tab_partitions
             WHERE table_owner = UPPER( p_owner )
               AND table_name = UPPER( p_table )
               AND partition_name = UPPER( p_partname );
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               raise_application_error( td_inst.get_err_cd( 'no_part' ),
                                           td_inst.get_err_msg( 'no_part' )
                                        || ': '
                                        || l_part_name
                                      );
         END;
      END IF;

      CASE
         WHEN td_ext.is_true( p_partitioned, TRUE )
              AND NOT td_ext.is_true( l_partitioned )
         THEN
            raise_application_error( td_inst.get_err_cd( 'not_partitioned' ),
                                        td_inst.get_err_msg( 'not_partitioned' )
                                     || ': '
                                     || l_tab_name
                                   );
         WHEN NOT td_ext.is_true( p_partitioned, TRUE )
              AND td_ext.is_true( l_partitioned )
         THEN
            raise_application_error( td_inst.get_err_cd( 'partitioned' ),
                                        td_inst.get_err_msg( 'partitioned' )
                                     || ': '
                                     || l_tab_name
                                   );
         WHEN td_ext.is_true( p_iot, TRUE ) AND NOT td_ext.is_true( l_iot )
         THEN
            raise_application_error( td_inst.get_err_cd( 'not_iot' ),
                                     td_inst.get_err_msg( 'not_iot' ) || ': '
                                     || l_tab_name
                                   );
         WHEN NOT td_ext.is_true( p_iot, TRUE ) AND td_ext.is_true( l_iot )
         THEN
            raise_application_error( td_inst.get_err_cd( 'iot' ),
                                     td_inst.get_err_msg( 'iot' ) || ': ' || l_tab_name
                                   );
         WHEN td_ext.is_true( p_compressed, TRUE ) AND NOT td_ext.is_true( l_compressed )
         THEN
            raise_application_error( td_inst.get_err_cd( 'not_compressed' ),
                                        td_inst.get_err_msg( 'not_compressed' )
                                     || ': '
                                     || CASE
                                           WHEN p_partname IS NULL
                                              THEN l_tab_name
                                           ELSE l_part_name
                                        END
                                   );
         WHEN NOT td_ext.is_true( p_compressed, TRUE ) AND td_ext.is_true( l_compressed )
         THEN
            raise_application_error( td_inst.get_err_cd( 'compressed' ),
                                        td_inst.get_err_msg( 'compressed' )
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

   -- checks things about an object depending on the parameters passed
   -- raises an exception if the specified things are not true
   PROCEDURE check_object(
      p_owner         VARCHAR2,
      p_object        VARCHAR2,
      p_object_type   VARCHAR2 DEFAULT NULL
   )
   AS
      l_obj_name      VARCHAR2( 61 )       := UPPER( p_owner ) || '.'
                                              || UPPER( p_object );
      l_object_type   all_objects.object_type%TYPE;
   BEGIN
      BEGIN
         SELECT DISTINCT object_type
                    INTO l_object_type
                    FROM all_objects
                   WHERE owner = UPPER( p_owner )
                     AND object_name = UPPER( p_object )
                     AND REGEXP_LIKE( object_type, p_object_type, 'i' );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            raise_application_error( td_inst.get_err_cd( 'no_or_wrong_object' ),
                                        td_inst.get_err_msg( 'no_or_wrong_object' )
                                     || ': '
                                     || l_obj_name
                                   );
         WHEN TOO_MANY_ROWS
         THEN
            raise_application_error( td_inst.get_err_cd( 'too_many_objects' ),
                                     td_inst.get_err_msg( 'too_many_objects' )
                                   );
      END;
   END check_object;
END td_sql;
/