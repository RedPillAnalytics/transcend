CREATE OR REPLACE TYPE BODY dimension_ot
AS
   CONSTRUCTOR FUNCTION dimension_ot( p_mapping VARCHAR2, p_batch_id NUMBER DEFAULT NULL )
      RETURN SELF AS RESULT
   AS
      l_load_sql   LONG;
      o_ev         evolve_ot := evolve_ot( p_module => 'dimension_ot.constructor' );
   BEGIN
      -- first register instrumentation details
      SELF.REGISTER( p_mapping => p_mapping, p_batch_id => p_batch_id );

      BEGIN
         -- now load the other attributes
         SELECT mapping_type, table_owner, table_name, full_table, source_owner, source_table,
                full_source, sequence_owner, sequence_name, full_sequence, staging_owner,
                staging_table, staging_owner || '.' || staging_table full_stage, named_staging, direct_load,
                replace_method, STATISTICS, index_concurrency, constraint_concurrency, mapping_name, manage_indexes, 
                manage_constraints, late_arriving, drop_dependent_objects
           INTO SELF.mapping_type, SELF.table_owner, SELF.table_name, SELF.full_table, SELF.source_owner, SELF.source_table,
                SELF.full_source, SELF.sequence_owner, SELF.sequence_name, SELF.full_sequence, SELF.staging_owner,
                SELF.staging_table, SELF.full_stage, SELF.named_staging, SELF.direct_load,
                SELF.replace_method, SELF.STATISTICS, SELF.index_concurrency, 
                SELF.constraint_concurrency, SELF.mapping_name, SELF.manage_indexes,
                SELF.manage_constraints, SELF.late_arriving, SELF.drop_dependent_objects
           FROM ( SELECT table_owner, table_name, table_owner || '.' || table_name full_table, source_owner,
                         source_table, source_owner || '.' || source_table full_source, sequence_owner, sequence_name,
                         sequence_owner || '.' || sequence_name full_sequence,
                         NVL( staging_owner, table_owner ) staging_owner,
                         NVL( staging_table, substr('STG$' || table_name, 1, 30) ) staging_table,
                         CASE
                            WHEN staging_table IS NULL
                               THEN 'no'
                            ELSE 'yes'
                         END named_staging, direct_load, replace_method, STATISTICS, index_concurrency,
                         constraint_concurrency, manage_indexes, manage_constraints, late_arriving, 
                         drop_dependent_objects, mapping_type
                   FROM dimension_conf JOIN mapping_conf USING( mapping_name )
                  WHERE mapping_name = SELF.mapping_name );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve.raise_err( 'no_dim', p_mapping );
      END;
      
      evolve.log_variable( 'SELF.full_table',SELF.full_table );
      evolve.log_variable( 'SELF.full_stage',SELF.full_stage );
      evolve.log_variable( 'SELF.drop_dependent_objects', SELF.drop_dependent_objects );
      evolve.log_variable( 'SELF.mapping_type', SELF.mapping_type );

      -- confirm the objects related to the dimensional configuration
      verify;
      -- reset the evolve_object
      o_ev.clear_app_info;
      RETURN;
   END dimension_ot;
   OVERRIDING MEMBER PROCEDURE verify
   IS
      l_src_part       BOOLEAN;
      l_tab_part       BOOLEAN;
      o_ev   evolve_ot := evolve_ot( p_module => 'dimension_ot.verify' );
   BEGIN
      -- now investigate the dimensional object
      -- check to make sure the dimension table exists
      td_utils.check_table( p_owner => SELF.table_owner, p_table => SELF.table_name );
      
      -- let's find out if it's partitioned
      l_tab_part      := td_utils.is_part_table( table_owner, table_name);
   
      evolve.log_variable('self.named_staging',SELF.named_staging);

      -- check that the sequence exists
      evolve.log_variable('self.sequence_owner',SELF.sequence_owner);
      evolve.log_variable('self.sequence_name',SELF.sequence_name);
      td_utils.check_object( p_owner            => SELF.sequence_owner,
                             p_object           => SELF.sequence_name,
                             p_object_type      => 'sequence'
                           );
      
      -- named staging
      -- this means that we are pre-creating a table and registering it with Transcend
      -- we have already created it, and is managed outside the framework
      IF td_core.is_true( self.named_staging )
      THEN
         
         evolve.log_variable('self.full_stage',SELF.full_stage);

         -- if it is, then make sure that it exists
         td_utils.check_table( p_owner => SELF.staging_owner, p_table => SELF.staging_table );
         
         -- we need to find out whether source object is partitioned or not         
         l_src_part      := td_utils.is_part_table( staging_owner, staging_table);
         
         IF replace_method = 'exchange'
         THEN

            -- if we are doing segment switching, then one of the tables needs to be partitioned
            -- but they both can't be
            -- this is a simple check to make sure that only one of the tables is partitioned, but not both
            IF l_src_part AND l_tab_part
            THEN
               evolve.raise_err( 'both_part' );
            ELSIF (NOT l_src_part AND NOT l_tab_part)
            THEN
               evolve.raise_err( 'neither_part' );
            END IF;
                           
         END IF;
      END IF;
      
      -- a table rename requires that the source and staging schemas are the same
      IF replace_method = 'rename' AND self.staging_owner <> self.table_owner
      THEN
         
         evolve.raise_err( 'rename_owners' );
         
      END IF;
         
         

      evolve.log_msg( 'Dimension confirmation completed successfully', 5 );
      -- reset the evolve_object
      o_ev.clear_app_info;
   END verify;
   MEMBER PROCEDURE initialize_cols
   IS
      o_ev   evolve_ot := evolve_ot( p_module => 'initialize_cols' );
   BEGIN
      -- need to construct the column lists of the different column types
      -- first get the current indicator
      BEGIN
         SELECT column_name
           INTO SELF.current_ind_col
           FROM column_conf
          WHERE table_owner = SELF.table_owner AND table_name = SELF.table_name AND column_type = 'current indicator';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve.raise_err( 'no_curr_ind', SELF.full_table );
         WHEN TOO_MANY_ROWS
         THEN
            evolve.raise_err( 'multiple_curr_ind', SELF.full_table );
      END;

      evolve.log_msg( 'The current indicator: ' || SELF.current_ind_col, 5 );

      -- get an expiration date
      BEGIN
         SELECT column_name
           INTO SELF.expire_dt_col
           FROM column_conf
          WHERE table_owner = SELF.table_owner AND table_name = SELF.table_name AND column_type = 'expiration date';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve.raise_err( 'no_exp_dt', SELF.full_table );
         WHEN TOO_MANY_ROWS
         THEN
            evolve.raise_err( 'multiple_exp_dt', SELF.full_table );
      END;

      evolve.log_msg( 'The expiration date: ' || SELF.expire_dt_col, 5 );

      -- get an effective date
      BEGIN
         SELECT column_name
           INTO SELF.effect_dt_col
           FROM column_conf
          WHERE table_owner = SELF.table_owner AND table_name = SELF.table_name AND column_type = 'effective date';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve.raise_err( 'no_eff_dt', SELF.full_table );
         WHEN TOO_MANY_ROWS
         THEN
            evolve.raise_err( 'multiple_eff_dt', SELF.full_table );
      END;

      evolve.log_msg( 'The effective date: ' || SELF.effect_dt_col, 5 );

      -- get a comma separated list of natural keys
      -- use the STRAGG function for this
      SELECT stragg( column_name )
        INTO SELF.natural_key_list
        FROM column_conf
       WHERE table_owner = SELF.table_owner AND table_name = SELF.table_name AND column_type = 'natural key';

      -- NO_DATA_FOUND exception does not work with STRAGG, as returning a null it fine
      -- have to do the logic programiatically
      IF SELF.natural_key_list IS NULL
      THEN
         evolve.raise_err( 'no_nat_key', full_table );
      END IF;

      evolve.log_msg( 'The natural key list: ' || SELF.natural_key_list, 5 );

      -- get the surrogate key column
      BEGIN
         SELECT column_name
           INTO SELF.surrogate_key_col
           FROM column_conf
          WHERE table_owner = SELF.table_owner AND table_name = SELF.table_name AND column_type = 'surrogate key';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve.raise_err( 'no_surr_key', SELF.full_table );
         WHEN TOO_MANY_ROWS
         THEN
            evolve.raise_err( 'multiple_surr_key', SELF.full_table );
      END;

      evolve.log_msg( 'The surrogate key: ' || SELF.surrogate_key_col, 5 );
      evolve.log_msg( 'Column initialization completed successfully', 5 );
      -- reset the evolve_object
      o_ev.clear_app_info;
   END initialize_cols;
   OVERRIDING MEMBER PROCEDURE confirm_dim_cols
   IS
      l_col_except    VARCHAR2( 1 );
      l_col_name      all_tab_columns.column_name%TYPE;
      l_data_type     all_tab_columns.data_type%TYPE;
      l_data_length   all_tab_columns.data_length%TYPE;
      o_ev            evolve_ot                          := evolve_ot( p_module => 'confirm_dim_cols' );
   BEGIN
      -- first need to initialize the column value attributes
      initialize_cols;

      -- check and make sure that the correct columns exist in the source table compared to the target table
      BEGIN
         SELECT EXCEPT, column_name, data_type, data_length
           INTO l_col_except, l_col_name, l_data_type, l_data_length
           FROM ( SELECT CASE
                            -- these are the only exceptable differences between the two tables
                         WHEN column_name = SELF.surrogate_key_col AND SOURCE = 'D'
                               THEN 'N'
                            WHEN column_name = SELF.expire_dt_col AND SOURCE = 'D'
                               THEN 'N'
                            WHEN column_name = SELF.current_ind_col AND SOURCE = 'D'
                               THEN 'N'
                            -- everything else is a problem
                         ELSE 'Y'
                         END EXCEPT,
                         src.*
                   FROM ( SELECT  column_name, data_type, data_length,
                                  CASE
                                     WHEN COUNT( src1 ) = 1
                                        THEN 'D'
                                     WHEN COUNT( src2 ) = 1
                                        THEN 'S'
                                  END SOURCE, COUNT( src1 ) cnt1, COUNT( src2 ) cnt2
                             FROM ( SELECT column_name, data_type, data_length, 1 src1, TO_NUMBER( NULL ) src2
                                     FROM all_tab_columns
                                    WHERE owner = SELF.table_owner AND table_name = SELF.table_name
                                   UNION ALL
                                   SELECT column_name, data_type, data_length, TO_NUMBER( NULL ) src1, 2 src2
                                     FROM all_tab_columns
                                    WHERE owner = SELF.source_owner AND table_name = SELF.source_table )
                         GROUP BY column_name, data_type, data_length
                           HAVING COUNT( src1 ) <> COUNT( src2 )) src )
          WHERE EXCEPT = 'Y';
      EXCEPTION
         -- no differences is fine
         WHEN NO_DATA_FOUND
         THEN
            NULL;
         -- any differences are too many, so this should raise an error
         WHEN TOO_MANY_ROWS
         THEN
            evolve.log_msg( 'More than one row found while comparing source and target columns', 5 );
            evolve.raise_err( 'dim_mismatch', SELF.full_table );
      END;

      -- if even one difference is found, then it's too many
      IF l_col_except = 'Y'
      THEN
         evolve.log_msg(    'Column '
                             || l_col_name
                             || ' of data_type '
                             || l_data_type
                             || ' and data_length '
                             || l_data_length
                             || ' found as mismatch',
                             5
                           );
         evolve.raise_err( 'dim_mismatch', SELF.full_table );
      END IF;

      evolve.log_msg( 'Dimension column confirmation completed successfully', 5 );
      -- reset the evolve_object
      o_ev.clear_app_info;
   END confirm_dim_cols;
   OVERRIDING MEMBER PROCEDURE LOAD
   IS
      e_dup_tab_name     EXCEPTION;
      PRAGMA             EXCEPTION_INIT( e_dup_tab_name, -955 );
      -- default comparision types
      l_char_nvl         dimension_conf.char_nvl_default%TYPE;
      l_num_nvl          dimension_conf.number_nvl_default%TYPE;
      l_date_nvl         dimension_conf.date_nvl_default%TYPE;
      l_stage_key        dimension_conf.stage_key_default%TYPE;
      l_sql              LONG;
      l_scd2_dates       LONG;
      l_scd2_nums        LONG;
      l_scd2_chars       LONG;
      l_scd2_list        LONG;
      l_scd1_list        LONG;
      l_scd_list         LONG;
      l_all_col_list     LONG;
      l_include_case     LONG;
      l_scd1_analytics   LONG;
      l_ind_filter       LONG;
      l_rep_ind_ins      LONG;
      l_bt_part          VARCHAR2(10);
      l_tab_part         BOOLEAN;
      l_rows             BOOLEAN;
      o_ev               evolve_ot := evolve_ot( p_module => 'mapping '||SELF.mapping_name, p_action => 'start mapping' );

   BEGIN
      
      -- first, confirm that the column values are as they should be
      confirm_dim_cols;
      
      -- let's find out if it's partitioned
      l_tab_part      := td_utils.is_part_table( table_owner, table_name);

      -- need to get some of the default comparision values
      BEGIN
         SELECT char_nvl_default, number_nvl_default, date_nvl_default, stage_key_default
           INTO l_char_nvl, l_num_nvl, l_date_nvl, l_stage_key
           FROM dimension_conf
          WHERE table_owner = SELF.table_owner AND table_name = SELF.table_name;
      EXCEPTION
         -- if there is no current indicator, that's okay
         -- it's not necessary
         WHEN NO_DATA_FOUND
         THEN
            NULL;
      END;

      -- get a comma separated list of scd2 columns that are dates
      -- use the STRAGG function for this
      BEGIN
         SELECT stragg( cc.column_name )
           INTO l_scd2_dates
           FROM column_conf cc 
           JOIN mapping_conf mc
                USING (mapping_name)
           JOIN all_tab_columns atc
                ON mc.table_owner = atc.owner AND mc.table_name = atc.table_name AND cc.column_name = atc.column_name
          WHERE mc.table_owner = SELF.table_owner
            AND mc.table_name = SELF.table_name
            AND column_type = 'scd type 2'
            AND data_type = 'DATE';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            -- if there are no type 2 attributes, that is fine
            NULL;
      END;

      evolve.log_msg( 'The SCD2 date list: ' || l_scd2_dates, 5 );

      -- get a comma separated list of scd2 attributes that are numbers
      -- use the STRAGG function for this
      BEGIN
         SELECT stragg( cc.column_name )
           INTO l_scd2_nums
           FROM column_conf cc 
           JOIN mapping_conf mc
                USING ( mapping_name )
           JOIN all_tab_columns atc
                ON mc.table_owner = atc.owner AND mc.table_name = atc.table_name AND cc.column_name = atc.column_name
          WHERE table_owner = SELF.table_owner
            AND mc.table_name = SELF.table_name
            AND column_type = 'scd type 2'
            AND data_type = 'NUMBER';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            -- if there are no type 2 attributes, that is fine
            NULL;
      END;

      evolve.log_msg( 'The SCD2 number list: ' || l_scd2_nums, 5 );

      -- get a comma separated list of scd2 date columns
      -- use the STRAGG function for this
      BEGIN
         SELECT stragg( cc.column_name )
           INTO l_scd2_chars
           FROM column_conf cc 
           JOIN mapping_conf mc
                USING ( mapping_name )
           JOIN all_tab_columns atc
                ON mc.table_owner = atc.owner AND mc.table_name = atc.table_name AND cc.column_name = atc.column_name
          WHERE table_owner = SELF.table_owner
            AND mc.table_name = SELF.table_name
            AND column_type = 'scd type 2'
            AND data_type NOT IN( 'DATE', 'NUMBER' );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            -- if there are no type 2 attributes, that is fine
            NULL;
      END;

      evolve.log_msg( 'The SCD2 char list: ' || l_scd2_chars, 5 );

      -- get a comma separated list of scd1 columns
      -- use the STRAGG function for this
      BEGIN
         SELECT stragg( column_name )
           INTO l_scd1_list
           FROM column_conf ic
           JOIN mapping_conf mc
                USING ( mapping_name )
          WHERE mc.table_owner = SELF.table_owner 
            AND mc.table_name = SELF.table_name 
            AND ic.column_type = 'scd type 1';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            -- if there are no type 1 attributes, that is fine
            NULL;
      END;

      evolve.log_msg( 'The SCD1 list: ' || l_scd1_list, 5 );
      -- construct a list of all scd2 attributes
      -- if any of the variables are null, we may get a ',,' or a ',' at the end or beginning of the list
      -- use the regexp_replaces to remove that
      l_scd2_list         := td_core.format_list( l_scd2_dates || ',' || l_scd2_nums || ',' || l_scd2_chars );
      evolve.log_msg( 'The SCD2 complete list: ' || l_scd2_list, 5 );
      -- construct a list of all scd attributes
      -- this is a combined list of all scd1 and scd2 attributes
      -- if any of the variables are null, we may get a ',,'
      -- use the regexp_replace to remove that
      -- also need a regexp to remove an extra comma at the end or beginning if they appears
      l_scd_list          := td_core.format_list( l_scd2_list || ',' || l_scd1_list );
      evolve.log_msg( 'The SCD complete list: ' || l_scd_list, 5 );
      -- construct the include case statement
      -- this case statement determines which records from the staging table are included as new rows
      l_include_case      :=
            'CASE WHEN '
         || SELF.surrogate_key_col
         || ' <> '
         || l_stage_key
         || ' THEN ''Y'' WHEN '
         || SELF.effect_dt_col
         || ' = lag('
         || SELF.effect_dt_col
         || ') over (partition by '
         || SELF.natural_key_list
         || ' order by '
         || SELF.effect_dt_col
         || ','
         || SELF.surrogate_key_col
         || ' desc) then ''N'' '
         || REGEXP_REPLACE( l_scd2_nums,
                            '(\w+)(,|$)',
                               'when nvl(\1,'
                            || l_num_nvl
                            || ') <> nvl(lag(\1) over (partition by '
                            || SELF.natural_key_list
                            || ' order by '
                            || SELF.effect_dt_col
                            || '),'
                            || l_num_nvl
                            || ') then ''Y'' '
                          )
         || REGEXP_REPLACE( l_scd2_chars,
                            '(\w+)(,|$)',
                               'when nvl(\1,'''
                            || l_char_nvl
                            || ''') <> nvl(lag(\1) over (partition by '
                            || SELF.natural_key_list
                            || ' order by '
                            || SELF.effect_dt_col
                            || '),'''
                            || l_char_nvl
                            || ''') then ''Y'' '
                          )
         || REGEXP_REPLACE( l_scd2_dates,
                            '(\w+)(,|$)',
                               'when nvl(\1,'''
                            || l_date_nvl
                            || ''') <> nvl(lag(\1) over (partition by '
                            || SELF.natural_key_list
                            || ' order by '
                            || SELF.effect_dt_col
                            || '),'''
                            || l_date_nvl
                            || ''') then ''Y'' '
                          )
         || ' else ''N'' end include';
      evolve.log_msg( 'The include CASE: ' || l_include_case, 5 );
      -- construct the scd1 analytics list
      -- this is a list of all the LAST_VALUE statements needed for the final statement
      l_scd1_analytics    :=
         REGEXP_REPLACE( l_scd1_list,
                         '(\w+)(,|$)',
                            'last_value(\1) over (partition by '
                         || SELF.natural_key_list
                         || ' order by '
                         || SELF.effect_dt_col
                         || ' ROWS BETWEEN unbounded preceding AND unbounded following) \1\2'
                       );
      evolve.log_msg( 'The SCD1 analytics clause: ' || l_scd1_analytics, 5 );
      -- construct a list of all the columns in the table
      l_all_col_list      :=
                             td_core.format_list( SELF.natural_key_list || ',' || l_scd_list || ',' || SELF.effect_dt_col );
      
      -- create a filter clause
      -- this is only needed if LATER_ARRIVING = 'no'
      
      l_ind_filter := CASE self.late_arriving
                      WHEN 'yes' THEN NULL
                      ELSE ' where '
                           ||SELF.current_ind_col
                           ||' = ''Y'''
                      END;
                      
      evolve.log_msg( 'The current indicator filter: ' || l_ind_filter, 5 );
                
      -- now, put the statement together
      l_sql               :=
            'insert '
         || CASE td_core.get_yn_ind( SELF.direct_load )
               WHEN 'yes'
                  THEN '/*+ APPEND */ '
               ELSE NULL
            END
         || 'into '
         || SELF.full_stage
         || '('
         || SELF.surrogate_key_col
         || ','
         || l_all_col_list
         || ','
         || SELF.expire_dt_col
         || ','
         || SELF.current_ind_col
         || ') '
         || ' SELECT case '
         || SELF.surrogate_key_col
         || ' when '
         || l_stage_key
         || ' then '
         || SELF.full_sequence
         || '.nextval else '
         || SELF.surrogate_key_col
         || ' end '
         || SELF.surrogate_key_col
         || ','
         || SELF.natural_key_list
         -- make sure there are no ',,' in the list
         || ','
         || td_core.format_list( l_scd_list || ',' || effect_dt_col )
         || ','
         || 'nvl( lead('
         || SELF.effect_dt_col
         || ') OVER ( partition BY '
         || SELF.natural_key_list
         || ' ORDER BY '
         || SELF.effect_dt_col
         || '), to_date(''12/31/9999'',''mm/dd/yyyy'')) '
         || SELF.expire_dt_col
         || ','
         || ' CASE MAX('
         || SELF.effect_dt_col
         || ') OVER (partition BY '
         || SELF.natural_key_list
         || ') WHEN '
         || SELF.effect_dt_col
         || ' THEN ''Y'' ELSE ''N'' END '
         || SELF.current_ind_col
         || ' from ('
         || 'SELECT '
         -- make sure there are no ',,' in the lists
         || td_core.format_list(    SELF.surrogate_key_col
                                 || ','
                                 || SELF.natural_key_list
                                 || ','
                                 || l_scd1_analytics
                                 || ','
                                 || l_scd2_list
                                 || ','
                                 || SELF.effect_dt_col
                               )
         || ','
         || l_include_case
         || ' from (select '
         || l_stage_key
         || ' '
         || SELF.surrogate_key_col
         || ','
         || l_all_col_list
         || ' from '
         || SELF.full_source
         || ' union select '
         || SELF.surrogate_key_col
         || ','
         || l_all_col_list
         || ' from '
         || SELF.full_table
         || l_ind_filter
         || ')'
         || ' order by '
         || SELF.natural_key_list
         || ','
         || SELF.effect_dt_col
         || ')'
         || ' where include=''Y''';

      -- this statement is needed for a particular "non-merge" situation described below
      l_rep_ind_ins :=  'insert '
                        || CASE td_core.get_yn_ind( SELF.direct_load )
                              WHEN 'yes'
                              THEN '/*+ APPEND */ '
                              ELSE NULL
                           END
                        || 'into '
                        || SELF.full_stage
                        || ' select * from '
                        || SELF.full_table
                        || ' where '
                        || SELF.current_ind_col
                        || ' = ''N''';

      evolve.log_msg( 'The non-merge, non-late-arriving insert: ' || l_rep_ind_ins, 5 );  


      -- create the staging table table
      -- this is a staging table that holds the results of the dimensional analysis
      -- it is then either exchanged in, table-renamed, or the source for a merge
      o_ev.change_action( 'create staging table' );

      BEGIN

         l_bt_part      := CASE
                           WHEN self.replace_method = 'exchange' AND l_tab_part 
                           THEN 'remove'
                           WHEN self.replace_method = 'exchange' AND NOT l_tab_part
                           THEN 'single'
                           ELSE 'keep'
                              END;
                              
         evolve.log_variable( 'L_BT_PART', l_bt_part );

         td_dbutils.build_table( p_source_owner      => SELF.table_owner,
                                 p_source_table      => SELF.table_name,
                                 p_owner             => SELF.staging_owner,
                                 p_table             => SELF.staging_table,
                                 p_partitioning      => l_bt_part
                               );
      EXCEPTION
         -- the table already exists
         WHEN e_dup_tab_name
            THEN
            evolve.log_msg( 'The staging table already exists', 4 );
            -- since the table already exists, we need to "clean" it
            -- drop constraints
            -- drop indexes
            -- truncate

            -- drop constraints on the segment in preparation for loading
            o_ev.change_action( 'drop constraints on staging' );

            BEGIN
               td_dbutils.drop_constraints( p_owner => SELF.staging_owner, p_table => SELF.staging_table );
            EXCEPTION
               WHEN td_dbutils.drop_iot_key
               THEN
                  NULL;
            END;

            -- drop indexes on the segment in preparation for loading
            o_ev.change_action( 'drop indexes on staging' );
            td_dbutils.drop_indexes( p_owner => SELF.staging_owner, p_table => SELF.staging_table );
                  
            -- truncate the staging table to get ready for a new run
            o_ev.change_action( 'truncate staging table' );
            td_dbutils.truncate_table( p_owner => SELF.staging_owner, p_table => SELF.staging_table );

      END;

      -- if later arriving dimensions are disabled, and the replace_method is NOT 'merge'
      -- then I need to get all the records from the dimension table that have a current_ind='N'
      -- this is because with 'rename' and 'exchange', I have to pull all records over into the staging table
      
      -- we are doing full segment-switching
      -- that means that all rows have to make it to the staging table
      IF SELF.replace_method <> 'merge'
      -- we are not doing late arriving
      -- this means that all rows are not coming through the main SCD analysis query
      -- only the CURRENT_INDICATOR='Y' are going through that query
      -- so we need to make sure the CURRENT_INDICATOR='N' rows get into the staging table as well
         AND self.late_arriving = 'no'
      THEN
         -- now run the insert statement to load the staging table
         o_ev.change_action( 'load non current_ind rows' );
         evolve.exec_sql( l_rep_ind_ins );
         evolve.log_results_msg( p_count          => SQL%ROWCOUNT,
                                 p_owner          => staging_owner,
                                 p_object         => staging_table,
                                 p_category       => 'insert',
                                 p_msg            => 'Number of records inserted into '|| full_stage );
         COMMIT; 


      END IF;

      -- now run the insert statement to load the staging table
      o_ev.change_action( 'load staging table' );
      evolve.exec_sql( l_sql );
      evolve.log_results_msg( p_count          => SQL%ROWCOUNT,
                              p_owner          => staging_owner,
                              p_object         => staging_table,
                              p_category       => 'insert',
                              p_msg            => 'Number of records inserted into '|| full_stage ); 

      COMMIT;

      -- reset the evolve_object
      o_ev.clear_app_info;
   END LOAD;
END;
/

SHOW errors