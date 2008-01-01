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
      l_curr_ind         column_conf.column_name%TYPE;
      l_exp_dt           column_conf.column_name%TYPE;
      l_eff_dt           column_conf.column_name%TYPE;
      l_surr_key         column_conf.column_name%TYPE;
      l_nk_list          VARCHAR2( 4000 );
      l_sql              LONG;
      l_scd2_list        LONG;
      l_scd2_date_list   LONG;
      l_scd1_list        LONG;
      l_scd_list         LONG;
      l_include_case     LONG;
      l_scd1_analytics   LONG;
      o_ev               evolve_ot                      := evolve_ot( p_module => 'load' );
      l_rows             BOOLEAN;
   BEGIN
      -- need to construct the column lists of the different column types
      -- first get the current indicator
      BEGIN
         SELECT column_name
           INTO l_curr_ind
           FROM column_conf ic
          WHERE ic.owner = SELF.owner AND ic.table_name = SELF.table_name AND ic.column_type = 'current indicator';
      EXCEPTION
         -- if there is no current indicator, that's okay
         -- it's not necessary
         WHEN NO_DATA_FOUND
         THEN
            NULL;
      END;

      evolve_log.log_msg( 'The current indicator: ' || l_curr_ind, 5 );

      -- get an expiration date
      BEGIN
         SELECT column_name
           INTO l_exp_dt
           FROM column_conf ic
          WHERE ic.owner = SELF.owner AND ic.table_name = SELF.table_name AND ic.column_type = 'expiration date';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve_log.raise_err( 'no_exp_dt' );
      END;

      evolve_log.log_msg( 'The expiration date: ' || l_exp_dt, 5 );

      -- get an effective date
      BEGIN
         SELECT column_name
           INTO l_eff_dt
           FROM column_conf ic
          WHERE ic.owner = SELF.owner AND ic.table_name = SELF.table_name AND ic.column_type = 'effective date';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve_log.raise_err( 'no_eff_dt' );
      END;

      evolve_log.log_msg( 'The effective date: ' || l_eff_dt, 5 );

      -- get a comma separated list of natural keys
      -- use the STRAGG function for this
      BEGIN
         SELECT stragg( column_name )
           INTO l_nk_list
           FROM column_conf ic
          WHERE ic.owner = SELF.owner AND ic.table_name = SELF.table_name AND ic.column_type = 'natural key';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve_log.raise_err( 'no_nat_key' );
      END;

      evolve_log.log_msg( 'The natural key list: ' || l_nk_list, 5 );

      -- get the surrogate key column
      BEGIN
         SELECT column_name
           INTO l_surr_key
           FROM column_conf ic
          WHERE ic.owner = SELF.owner AND ic.table_name = SELF.table_name AND ic.column_type = 'surrogate key';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve_log.raise_err( 'no_surr_key' );
      END;

      evolve_log.log_msg( 'The surrogate key: ' || l_surr_key, 5 );

      -- get a comma separated list of scd2 columns (except those that are dates)
      -- use the STRAGG function for this
      BEGIN
         SELECT stragg( column_name )
           INTO l_scd2_list
           FROM column_conf ic
	   JOIN all_tab_columns
		USING (owner,table_name,column_name)
          WHERE owner = SELF.owner AND table_name = SELF.table_name AND column_type = 'scd type 2'
	    AND data_type <> 'DATE';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            -- if there are no type 2 attributes, that is fine
            NULL;
      END;

      evolve_log.log_msg( 'The SCD 2 list: ' || l_scd2_list, 5 );
      
      -- get a comma separated list of scd2 date columns
      -- use the STRAGG function for this
      BEGIN
         SELECT stragg( column_name )
           INTO l_scd2_date_list
           FROM column_conf ic
	   JOIN all_tab_columns
		USING (owner,table_name,column_name)
          WHERE owner = SELF.owner AND table_name = SELF.table_name AND column_type = 'scd type 2'
	    AND data_type = 'DATE';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            -- if there are no type 2 attributes, that is fine
            NULL;
      END;

      evolve_log.log_msg( 'The SCD 2 DATE list: ' || l_scd2_date_list, 5 );

      -- get a comma separated list of scd1 columns
      -- use the STRAGG function for this
      BEGIN
         SELECT stragg( column_name )
           INTO l_scd1_list
           FROM column_conf ic
          WHERE ic.owner = SELF.owner AND ic.table_name = SELF.table_name AND ic.column_type = 'scd type 1';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            -- if there are no type 1 attributes, that is fine
            NULL;
      END;

      evolve_log.log_msg( 'The SCD 1 list: ' || l_scd1_list, 5 );
      -- construct a list of all scd attributes
      -- this is a combined list of all scd1 and scd2 attributes
      -- if any of the variables are null, we may get a ',,'
      -- use the regexp_replace to remove that
      -- also need the regexp to remove an extra comma at the end if that appears
      l_scd_list          :=
      regexp_replace(l_scd2_list ||','|| l_scd2_date_list||','|| l_scd1_list,'(,)(,|$)','\2');

      evolve_log.log_msg( 'The SCD list: ' || l_scd_list, 5 );
      -- construct the include case statement
      -- this case statement determines which records from the staging table are included as new rows
      l_include_case      :=
            'case '
         || REGEXP_REPLACE( l_scd2_list,
                            '(\w+)(,|$)',
                               'when nvl(\1,-.01) <> nvl(lag(\1) over (partition by '
                            || l_nk_list
                            || ' order by '
                            || l_eff_dt
                            || '),-.01) then ''Y'' '
                          )
      || REGEXP_REPLACE( l_scd2_date_list,
                            '(\w+)(,|$)',
                               'when nvl(\1,''01/02/9999'') <> nvl(lag(\1) over (partition by '
                            || l_nk_list
                            || ' order by '
                            || l_eff_dt
                            || '),''01/02/9999'') then ''Y'' '
                          )
         || ' else ''N'' end include';
      evolve_log.log_msg( 'The include CASE: ' || l_include_case, 5 );
      -- construct the scd1 analytics list
      -- this is a list of all the LAST_VALUE statements needed for the final statement
      l_scd1_analytics    :=
         REGEXP_REPLACE( l_scd1_list,
                         '(\w+)(,|$)',
                            'last_value(\1) over (partition by '
                         || l_nk_list
                         || ' order by '
                         || l_eff_dt
                         || ' ROWS BETWEEN unbounded preceding AND unbounded following) \1'
                       );
      evolve_log.log_msg( 'The scd1 analytics clause: ' || l_scd1_analytics, 5 );
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
         || ' SELECT case '
         || l_surr_key
         || ' when -.1 then '
         || SELF.full_sequence
         || '.nextval else '
         || l_surr_key
         || ' end '
         || l_surr_key
         || ','
         || l_nk_list
         || ','
         || l_scd_list
         || ','
         || l_eff_dt
         || ','
         || 'nvl( lead('
         || l_eff_dt
         || ') OVER ( partition BY '
         || l_nk_list
         || ' ORDER BY '
         || l_eff_dt
         || '), to_date(''12/31/9999'',''mm/dd/yyyy'')) '
         || l_exp_dt
         || ','
         || ' CASE MAX('
         || l_eff_dt
         || ') OVER (partition BY '
         || l_nk_list
         || ') WHEN '
         || l_eff_dt
         || ' THEN ''Y'' ELSE ''N'' END '
         || l_curr_ind
         || ' from ('
         || 'SELECT '
         || l_surr_key
         || ','
         || l_nk_list
         || ','
         || l_scd1_analytics
            || l_scd2_list
	    || ','
	    || l_scd2_date_list
         || ','
         || l_eff_dt
         || ','
         || l_include_case
         || ' from (select -.1 '
         || l_surr_key
         || ','
         || l_nk_list
         || ','
         || l_eff_dt
         || ','
         || l_scd_list
         || ' from '
         || SELF.full_source
         || ' union select '
         || l_surr_key
         || ','
         || l_nk_list
         || ','
         || l_eff_dt
         || ','
         || l_scd_list
         || ' from '
         || SELF.full_table
         || ')'
         || ' order by '
         || l_nk_list
         || ','
         || l_eff_dt
         || ')'
         || ' where include=''Y''';

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