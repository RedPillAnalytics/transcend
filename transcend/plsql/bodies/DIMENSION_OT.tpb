CREATE OR REPLACE TYPE BODY dimension_ot
AS
   CONSTRUCTOR FUNCTION dimension_ot( p_owner VARCHAR2, p_table VARCHAR2 )
      RETURN SELF AS RESULT
   AS
      l_tab_name   VARCHAR2( 61 )                   := UPPER( p_owner || '.' || p_table );
      l_owner      dimension_conf.owner%TYPE        := UPPER( p_owner );
      l_table      dimension_conf.table_name%TYPE   := UPPER( p_table );
      l_load_sql   LONG;
      o_ev         evolve_ot                        := evolve_ot( p_module => 'dimension_ot' );
   BEGIN
      SELECT owner, table_name, full_table, source_owner, source_object, full_source, sequence_owner, sequence_name,
             full_sequence, staging_owner, staging_table, staging_owner || '.' || staging_table full_stage,
             constant_staging, direct_load, replace_method, STATISTICS, concurrent
        INTO owner, table_name, full_table, source_owner, source_object, full_source, sequence_owner, sequence_name,
             full_sequence, staging_owner, staging_table, full_stage,
             constant_staging, direct_load, replace_method, STATISTICS, concurrent
        FROM ( SELECT owner, table_name, owner || '.' || table_name full_table, source_owner, source_object,
                      source_owner || '.' || source_object full_source, sequence_owner, sequence_name,
                      sequence_owner || '.' || sequence_name full_sequence, NVL( staging_owner, owner ) staging_owner,
                      NVL( staging_table, 'TD$' || table_name ) staging_table,
                      CASE
                         WHEN staging_table IS NULL
                            THEN 'no'
                         ELSE 'yes'
                      END constant_staging, direct_load, replace_method, STATISTICS, concurrent
                FROM dimension_conf
               WHERE owner = l_owner AND table_name = l_table );

      -- every time the dimension object is loaded, it should confirm the objects
      confirm_objects;
      RETURN;
   END dimension_ot;
   MEMBER PROCEDURE confirm_objects
   IS
      o_ev   evolve_ot := evolve_ot( p_module => 'confirm_objects' );
   BEGIN
      evolve_log.log_msg( 'Constant staging: ' || constant_staging, 5 );
      -- check to see if the dimension table exists
      td_utils.check_table( p_owner => owner, p_table => table_name );
      -- check that the source object exists
      td_utils.check_object( p_owner => source_owner, p_object => source_object, p_object_type => 'table$|view' );
      -- check that the sequence exists
      evolve_log.log_msg( 'The sequence owner: ' || sequence_owner, 5 );
      evolve_log.log_msg( 'The sequence name: ' || sequence_name, 5 );
      td_utils.check_object( p_owner => sequence_owner, p_object => sequence_name, p_object_type => 'sequence' );

      -- check to see if the staging table is constant
      IF td_core.is_true( constant_staging )
      THEN
         evolve_log.log_msg( 'Full stage: ' || full_stage, 5 );
         -- if it is, then make sure that it exists
         td_utils.check_table( p_owner => staging_owner, p_table => staging_table );
      END IF;
   END confirm_objects;
   MEMBER PROCEDURE LOAD
   IS
      l_sql    LONG;
      o_ev     evolve_ot := evolve_ot( p_module => 'load' );
      l_rows   BOOLEAN;
   BEGIN
      SELECT DISTINCT    'insert '
                      || CASE td_core.get_yn_ind( SELF.direct_load )
                            WHEN 'yes'
                               THEN '/*+ APPEND */ '
                            ELSE NULL
                         END
                      || 'into '
                      || SELF.full_stage
                      || ' SELECT '
                      || sel1
                      || ' from ('
                      || 'SELECT '
                      || sk
                      || ','
                      || nk_list
                      || ','
                      || scd1_analytics
                      || scd2_list
                      || ','
                      || efd
                      || ','
                      || include_list
                      || ' from '
                      || union_list
                      || ' order by '
                      || nk_list
                      || ','
                      || efd
                      || ')'
                      || ' where include=''Y'''
                 INTO l_sql
                 FROM ( SELECT sk, nk_list, efd, scd1_list, scd2_list, scd_list,
                                  'CASE '
                               || sk
                               || ' when -.1 then '
                               || SELF.full_sequence
                               || '.nextval else '
                               || sk
                               || ' end '
                               || sk
                               || ','
                               || nk_list
                               || ','
                               || scd_list
                               || ','
                               || efd
                               || ','
                               || 'nvl( lead('
                               || efd
                               || ') OVER ( partition BY '
                               || nk_list
                               || ' ORDER BY '
                               || efd
                               || '), to_date(''12/31/9999'',''mm/dd/yyyy'')) '
                               || exd
                               || ','
                               || ' CASE MAX('
                               || efd
                               || ') OVER (partition BY '
                               || nk_list
                               || ') WHEN '
                               || efd
                               || ' THEN ''Y'' ELSE ''N'' END '
                               || ci sel1,
                               
                               -- use a STRAGG function to aggregate strings
                               ( SELECT    stragg
                                              (    'last_value('
                                                || column_name
                                                || ') over (partition by '
                                                || nk_list
                                                || ' order by '
                                                || efd
                                                || ' ROWS BETWEEN unbounded preceding AND unbounded following) '
                                                || column_name
                                              ) OVER( PARTITION BY column_type )
                                        || ','
                                  FROM column_conf ic
                                 WHERE ic.owner = SELF.owner
                                   AND ic.table_name = SELF.table_name
                                   AND ic.column_type = 'scd type 1' ) scd1_analytics,
                                  '(select -.1 '
                               || sk
                               || ','
                               || nk_list
                               || ','
                               || efd
                               || ','
                               || scd_list
                               || ' from '
                               || SELF.full_source
                               || ' union select '
                               || sk
                               || ','
                               || nk_list
                               || ','
                               || efd
                               || ','
                               || scd_list
                               || ' from '
                               || full_table
                               || ')' union_list,
                                  'case when '
                               || sk
                               || ' <> -.1 then ''Y'' when '
                               || efd
                               || '=LAG('
                               || efd
                               || ') over (partition by '
                               || nk_list
                               || ' order by '
                               || efd
                               || ','
                               || sk
                               || ' desc) then ''N'''
                               -- use the STRAGG function to aggregate strings
                               -- this puts together the scd type 2 case statement
                               -- this generates the include flag
                               || ( SELECT REGEXP_REPLACE( stragg(    ' WHEN nvl('
                                                                || column_name
                                                                || ',-.01) < > nvl(LAG('
                                                                || column_name
                                                                || ') OVER (partition BY '
                                                                || nk_list
                                                                || ' ORDER BY '
                                                                || efd
                                                                || '),-.01) THEN ''Y'''
                                                              ),
                                                           ', WHEN',
                                                           ' WHEN'
                                                         )
                                     FROM column_conf ic
                                    WHERE ic.owner = SELF.owner
                                      AND ic.table_name = SELF.table_name
                                      AND column_type = 'scd type 2' )
                               || ' else ''N'' end include' include_list
                         FROM ( SELECT column_type, column_name,
                                       
                                       -- STRAGG function aggregates strings
                                       ( SELECT stragg( column_name )
                                          FROM column_conf ic
                                         WHERE ic.owner = SELF.owner
                                           AND ic.table_name = SELF.table_name
                                           AND REGEXP_LIKE( ic.column_type, 'scd', 'i' )) scd_list,
                                       
                                       -- STRAGG function aggregates strings
                                       ( SELECT stragg( column_name )
                                          FROM column_conf ic
                                         WHERE ic.owner = SELF.owner
                                           AND ic.table_name = SELF.table_name
                                           AND column_type = 'scd type 1' ) scd1_list,
                                       
                                       -- STRAGG function aggregates strings
                                       -- generate the list of all the different column types
                                       ( SELECT stragg( column_name )
                                          FROM column_conf ic
                                         WHERE ic.owner = SELF.owner
                                           AND ic.table_name = SELF.table_name
                                           AND column_type = 'scd type 2' ) scd2_list,
                                       ( SELECT column_name
                                          FROM column_conf ic
                                         WHERE ic.owner = SELF.owner
                                           AND ic.table_name = SELF.table_name
                                           AND ic.column_type = 'surrogate key' ) sk,
                                       ( SELECT MAX( column_name )
                                          FROM column_conf ic
                                         WHERE ic.owner = SELF.owner
                                           AND ic.table_name = SELF.table_name
                                           AND ic.column_type = 'natural key' ) nk_list,
                                       ( SELECT column_name
                                          FROM column_conf ic
                                         WHERE ic.owner = SELF.owner
                                           AND ic.table_name = SELF.table_name
                                           AND ic.column_type = 'effective date' ) efd,
                                       ( SELECT column_name
                                          FROM column_conf ic
                                         WHERE ic.owner = SELF.owner
                                           AND ic.table_name = SELF.table_name
                                           AND ic.column_type = 'expiration date' ) exd,
                                       ( SELECT column_name
                                          FROM column_conf ic
                                         WHERE ic.owner = SELF.owner
                                           AND ic.table_name = SELF.table_name
                                           AND ic.column_type = 'current indicator' ) ci
                                 FROM column_conf JOIN dimension_conf USING( owner, table_name )
                                WHERE owner = SELF.owner AND table_name = SELF.table_name ));

      -- check to see if the staging table is constant
      IF NOT td_core.is_true( constant_staging )
      THEN
         -- if it isn't, then create the staging table for temporary use
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
      evolve_app.exec_sql( l_sql );
      -- perform the replace method
      o_ev.change_action( 'replace table' );

      CASE replace_method
         WHEN 'exchange'
         THEN
            td_dbutils.exchange_partition( p_source_owner      => staging_owner,
                                           p_source_table      => staging_table,
                                           p_owner             => owner,
                                           p_table             => table_name,
                                           p_statistics        => STATISTICS,
                                           p_concurrent        => concurrent
                                         );
         WHEN 'replace'
         THEN
            td_dbutils.replace_table( p_owner             => owner,
                                      p_table             => table_name,
                                      p_source_table      => staging_table,
                                      p_statistics        => STATISTICS,
                                      p_concurrent        => concurrent
                                    );
         ELSE
            NULL;
      END CASE;
   END LOAD;
END;
/

SHOW errors