CREATE OR REPLACE TYPE BODY dimension_ot
AS
   CONSTRUCTOR FUNCTION dimension_ot( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN SELF AS RESULT
   AS
   BEGIN
      SELECT DISTINCT owner, table_name, owner || '.' || table_name full_table, source_owner, source_object,
                      source_owner || '.' || source_object full_source, staging_owner, staging_table,
                      staging_owner || '.' || staging_table full_stage, constant_staging, direct_load,
                      replace_method, statistics,
                         'insert '
                      || CASE td_core.get_yn_ind( direct_load )
                            WHEN 'yes'
                               THEN '/*+ APPEND */ '
                            ELSE NULL
                         END
                      || 'into '
                      || full_stage
                      || ' SELECT '
                      || sel1
                      || ' from ('
                      || 'SELECT '
                      || sk
                      || ','
                      || nk
                      || ','
                      || scd1_analytics
                      || scd2_list
                      || ','
                      || esd
                      || ','
                      || include_list
                      || ' from '
                      || union_list
                      || ' order by '
                      || nk
                      || ','
                      || esd
                      || ')'
                      || ' where include=''Y''' load_sql
                 INTO owner, table_name, full_table, source_owner, source_object,
                      full_source, staging_owner, staging_table,
                      full_stage, constant_staging, direct_load,
                      replace_method, statistics,
                      load_sql
                 FROM ( SELECT ROWNUM rn, owner, table_name, source_owner, source_object, staging_owner,
                               staging_table, staging_owner || '.' || staging_table full_stage, statistics,
                               CASE
                                  WHEN staging_table IS NULL
                                     THEN 'yes'
                                  ELSE 'no'
                               END constant_staging, direct_load, replace_method, sk, nk, esd, scd1_list,
                               scd2_list, scd_list,
                                  'CASE '
                               || sk
                               || ' when -.1 then '
                               || sequence_owner
                               || '.'
                               || sequence_name
                               || '.nextval else '
                               || sk
                               || ' end '
                               || sk
                               || ','
                               || nk
                               || ','
                               || scd_list
                               || ','
                               || esd
                               || ','
                               || 'nvl( lead('
                               || esd
                               || ') OVER ( partition BY '
                               || nk
                               || ' ORDER BY '
                               || esd
                               || '), to_date(''12/31/9999'',''mm/dd/yyyy'')) '
                               || eed
                               || ','
                               || ' CASE MAX('
                               || esd
                               || ') OVER (partition BY '
                               || nk
                               || ') WHEN '
                               || esd
                               || ' THEN ''Y'' ELSE ''N'' END '
                               || ci sel1,
                               
                               -- use a STRAGG function to aggregate strings
                               ( SELECT    STRAGG
                                              (    'last_value('
                                                || column_name
                                                || ') over (partition by '
                                                || nk
                                                || ' order by '
                                                || esd
                                                || ' ROWS BETWEEN unbounded preceding AND unbounded following) '
                                                || column_name
                                              ) OVER( PARTITION BY column_type )
                                        || ','
                                  FROM column_conf ic
                                 WHERE ic.owner = owner
                                   AND ic.table_name = table_name
                                   AND ic.column_type = 'scd type 1' ) scd1_analytics,
                                  '(select -.1 '
                               || sk
                               || ','
                               || nk
                               || ','
                               || esd
                               || ','
                               || scd_list
                               || ' from '
                               || source_owner
                               || '.'
                               || source_object
                               || ' union select '
                               || sk
                               || ','
                               || nk
                               || ','
                               || esd
                               || ','
                               || scd_list
                               || ' from '
                               || owner
                               || '.'
                               || table_name
                               || ')' union_list,
                                  'case when '
                               || sk
                               || ' <> -.1 then ''Y'' when '
                               || esd
                               || '=LAG(effect_start_dt) over (partition by '
                               || nk
                               || ' order by '
                               || esd
                               || ','
                               || sk
                               || ' desc) then ''N'''
                               -- use the STRAGG function to aggregate strings
                               || ( SELECT REGEXP_REPLACE( STRAGG(    ' WHEN nvl('
                                                                || column_name
                                                                || ',-.01) < > nvl(LAG('
                                                                || column_name
                                                                || ') OVER (partition BY '
                                                                || nk
                                                                || ' ORDER BY '
                                                                || esd
                                                                || '),-.01) THEN ''Y'''
                                                              ),
                                                           ', WHEN',
                                                           ' WHEN'
                                                         )
                                     FROM column_conf ic
                                    WHERE ic.owner = owner
                                      AND ic.table_name = table_name
                                      AND column_type = 'scd type 2' )
                               || ' else ''N'' end include' include_list
                         FROM ( SELECT UPPER( owner ) owner, UPPER( table_name ) table_name, column_type,
                                       column_name, UPPER( source_object ) source_object,
                                       UPPER( source_owner ) source_owner,
                                       UPPER( NVL( staging_owner, owner ) ) staging_owner,
                                       UPPER( NVL( staging_table, 'TD$' || table_name ) ) staging_table,
                                       UPPER( sequence_owner ) sequence_owner,
                                       UPPER( sequence_name ) sequence_name, direct_load, replace_method, statistics,
                                       
                                       -- STRAGG function aggregates strings
                                       ( SELECT STRAGG( column_name )
                                          FROM column_conf ic
                                         WHERE ic.owner = owner
                                           AND ic.table_name = table_name
                                           AND REGEXP_LIKE( ic.column_type, 'scd', 'i' ) ) scd_list,
                                       
                                       -- STRAGG function aggregates strings
                                       ( SELECT STRAGG( column_name )
                                          FROM column_conf ic
                                         WHERE ic.owner = owner
                                           AND ic.table_name = table_name
                                           AND column_type = 'scd type 1' ) scd1_list,
                                       
                                       -- STRAGG function aggregates strings
                                       ( SELECT STRAGG( column_name )
                                          FROM column_conf ic
                                         WHERE ic.owner = owner
                                           AND ic.table_name = table_name
                                           AND column_type = 'scd type 2' ) scd2_list,
                                       ( SELECT column_name
                                          FROM column_conf ic
                                         WHERE ic.owner = owner
                                           AND ic.table_name = table_name
                                           AND ic.column_type = 'surrogate key' ) sk,
                                       ( SELECT column_name
                                          FROM column_conf ic
                                         WHERE ic.owner = owner
                                           AND ic.table_name = table_name
                                           AND ic.column_type = 'natural key' ) nk,
                                       ( SELECT column_name
                                          FROM column_conf ic
                                         WHERE ic.owner = owner
                                           AND ic.table_name = table_name
                                           AND ic.column_type = 'effective start date' ) esd,
                                       ( SELECT column_name
                                          FROM column_conf ic
                                         WHERE ic.owner = owner
                                           AND ic.table_name = table_name
                                           AND ic.column_type = 'effective end date' ) eed,
                                       ( SELECT column_name
                                          FROM column_conf ic
                                         WHERE ic.owner = owner
                                           AND ic.table_name = table_name
                                           AND ic.column_type = 'current indicator' ) ci
                                 FROM column_conf JOIN dimension_conf USING( owner, table_name )
                                WHERE owner = p_owner AND table_name = p_table ) );

      RETURN;
   END dimension_ot;
   MEMBER PROCEDURE LOAD
   IS
      o_ev     evolve_ot := evolve_ot( p_module => 'load' );
      l_rows   BOOLEAN;
   BEGIN
      -- check to see if the dimension table exists
      td_utils.check_table( p_owner => owner, p_table => table_name );
      -- check that the source object exists
      td_utils.check_object( p_owner            => source_owner,
                             p_object           => source_object,
                             p_object_type      => 'table$|view'
                           );

      -- check to see if the staging table is constant
      IF td_core.is_true( constant_staging )
      THEN
         -- if it is, then make sure that it exists
         td_utils.check_table( p_owner => staging_owner, p_table => staging_table );
      ELSE
         -- otherwise, create the table
         o_ev.change_action( 'create staging table' );
         td_dbutils.build_table( p_source_owner      => owner,
                                 p_source_table      => table_name,
                                 p_owner             => owner,
                                 p_table             => staging_table,
                                 -- if the data will be replaced in using an exchange, then need the table to not be partitioned
                                 -- everything else can be created just like the source table
                                 p_partitioning      => CASE replace_method
                                    WHEN 'exchange'
                                       THEN 'no'
                                    ELSE 'yes'
                                 END
                               );
      END IF;

      -- now run the insert statement to load the staging table
      o_ev.change_action( 'load staging table' );
      evolve_app.exec_sql( load_sql );
      -- perform the replace method
      o_ev.change_action( 'replace table' );

      CASE replace_method
         WHEN 'exchange'
         THEN
            td_dbutils.exchange_partition( p_source_owner      => staging_owner,
                                           p_source_table      => staging_table,
                                           p_owner             => owner,
                                           p_table             => table_name,
                                           p_statistics        => statistics
                                         );
         WHEN 'replace'
         THEN
            td_dbutils.replace_table( p_owner             => owner,
                                      p_table             => table_name,
                                      p_source_table      => staging_table,
                                      p_statistics        => statistics
                                    );
         ELSE
            NULL;
      END CASE;
   END LOAD;
END;
/

SHOW errors