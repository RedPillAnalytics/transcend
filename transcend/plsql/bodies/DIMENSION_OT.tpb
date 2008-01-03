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
      EXCEPTION
	 WHEN no_data_found
	 THEN
	 evolve_log.raise_err( 'no_dim',full_table );
      END;


      -- confirm the objects related to the dimensional configuration
      confirm_dim;
      
      -- reset the evolve_object
      o_ev.clear_app_info;
      RETURN;
   END dimension_ot;
   MEMBER PROCEDURE confirm_dim
   IS
      o_ev          evolve_ot := evolve_ot( p_module => 'confirm_dim' );
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

      -- reset the evolve_object
      o_ev.clear_app_info;

   END confirm_dim;
   
   MEMBER PROCEDURE initialize_cols
   IS
      o_ev          evolve_ot := evolve_ot( p_module => 'initialize_cols' );
   BEGIN
      -- need to construct the column lists of the different column types
      -- first get the current indicator
      BEGIN
	 SELECT column_name
           INTO current_ind_col
           FROM column_conf
	  WHERE owner = SELF.owner 
	    AND table_name = SELF.table_name 
	    AND column_type = 'current indicator';
      EXCEPTION
	 WHEN no_data_found
	 THEN
	    evolve_log.raise_err( 'no_curr_ind', full_table );
	 WHEN too_many_rows
	 THEN
	    evolve_log.raise_err( 'multiple_curr_ind', full_table );
      END;

      evolve_log.log_msg( 'The current indicator: ' || current_ind_col, 5 );

      -- get an expiration date
      BEGIN
         SELECT column_name
           INTO expire_dt_col
           FROM column_conf
          WHERE owner = SELF.owner AND table_name = SELF.table_name AND column_type = 'expiration date';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve_log.raise_err( 'no_exp_dt', full_table );
         WHEN too_many_rows
         THEN
            evolve_log.raise_err( 'multiple_exp_dt', full_table );
      END;

      evolve_log.log_msg( 'The expiration date: ' || expire_dt_col, 5 );

      -- get an effective date
      BEGIN
         SELECT column_name
           INTO effect_dt_col
           FROM column_conf
          WHERE owner = SELF.owner AND table_name = SELF.table_name AND column_type = 'effective date';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve_log.raise_err( 'no_eff_dt', full_table );
         WHEN too_many_rows
         THEN
            evolve_log.raise_err( 'multiple_eff_dt', full_table );
      END;

      evolve_log.log_msg( 'The effective date: ' || effect_dt_col, 5 );

      -- get a comma separated list of natural keys
      -- use the STRAGG function for this
      SELECT stragg( column_name )
        INTO natural_key_list
        FROM column_conf
       WHERE owner = SELF.owner AND table_name = SELF.table_name AND column_type = 'natural key';

      -- NO_DATA_FOUND exception does not work with STRAGG, as returning a null it fine
      -- have to do the logic programiatically
      IF natural_key_list IS NULL
      THEN
	 evolve_log.raise_err( 'no_nat_key', full_table );
      END IF;

      evolve_log.log_msg( 'The natural key list: ' || natural_key_list, 5 );

      -- get the surrogate key column
      BEGIN
         SELECT column_name
           INTO surrogate_key_col
           FROM column_conf
          WHERE owner = SELF.owner AND table_name = SELF.table_name AND column_type = 'surrogate key';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve_log.raise_err( 'no_surr_key', full_table );
         WHEN too_many_rows
         THEN
            evolve_log.raise_err( 'multiple_surr_key', full_table );
      END;

      evolve_log.log_msg( 'The surrogate key: ' || surrogate_key_col, 5 );

      -- reset the evolve_object
      o_ev.clear_app_info;

   END initialize_cols;

   MEMBER PROCEDURE confirm_dim_cols
   IS
      l_col_except  VARCHAR2(1);
      l_col_name    all_tab_columns.column_name%type;
      l_data_type   all_tab_columns.data_type%type;
      l_data_length all_tab_columns.data_length%type;
      o_ev          evolve_ot := evolve_ot( p_module => 'confirm_dim_cols' );
   BEGIN
      -- first need to initialize the column value attributes
      initialize_cols;

      -- check and make sure that the correct columns exist in the source table compared to the target table
      BEGIN
	 SELECT EXCEPT,
		column_name,
		data_type,
		data_length
	   INTO l_col_except,
		l_col_name,
		l_data_type,
		l_data_length
	   FROM ( SELECT CASE
			 -- these are the only exceptable differences between the two tables
			 WHEN column_name =self.surrogate_key_col AND source='D'
			 THEN 'N'
			 WHEN column_name=self.expire_dt_col AND source='D'
			 THEN 'N'
			 WHEN column_name=self.current_ind_col AND source='D'
			 THEN 'N'
			 -- everything else is a problem
			 ELSE 'Y'
			 END EXCEPT,
			 src.*
		    FROM (SELECT column_name,
				 data_type,
				 data_length,
				 CASE
				 WHEN count(src1) = 1 THEN 'D'
				 WHEN count(src2) = 1 THEN 'S'
				 END source,
				 count(src1) cnt1, 
				 count(src2) cnt2
			    FROM 
				 ( SELECT column_name,
					  data_type,
					  data_length,
					  1 src1, 
					  to_number(NULL) src2 
				     FROM all_tab_columns
				    WHERE owner=self.owner
				      AND table_name=self.table_name
					  UNION ALL
				   SELECT column_name,
					  data_type,
					  data_length, 
					  to_number(NULL) src1,
					  2 src2
				     FROM all_tab_columns
				    WHERE owner=self.source_owner
				      AND table_name=self.source_object
				 )
			   GROUP BY column_name,
				 data_type,
				 data_length
			  HAVING count(src1) <> count(src2)) src )
	  WHERE EXCEPT='Y';
      EXCEPTION
	 -- no differences is fine
	 WHEN no_data_found
	 THEN
	 NULL;
	 -- any differences are too many, so this should raise an error
	 WHEN too_many_rows
	 THEN
	 evolve_log.log_msg('More than one row found while comparing source and target columns',5);
	 evolve_log.raise_err( 'dim_mismatch',full_table );
      END;
      
      -- if even one difference is found, then it's too many
      IF l_col_except = 'Y'
      THEN
	 evolve_log.log_msg('Column '||l_col_name||' of data_type '||l_data_type||' and data_length '||l_data_length||' found as mismatch',5);
	 evolve_log.raise_err( 'dim_mismatch',full_table );
      END IF;

      -- reset the evolve_object
      o_ev.clear_app_info;

   END confirm_dim_cols;

   MEMBER PROCEDURE LOAD
   IS
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
      l_include_case     LONG;
      l_scd1_analytics   LONG;
      l_rows             BOOLEAN;
      o_ev               evolve_ot                      := evolve_ot( p_module => 'load' );
   BEGIN
      -- first, confirm that the column values are as they should be
      confirm_dim_cols;

      -- need to get some of the default comparision values
      BEGIN
         SELECT char_nvl_default,
		number_nvl_default,
		date_nvl_default,
		stage_key_default
           INTO l_char_nvl,
		l_num_nvl,
		l_date_nvl,
		l_stage_key
           FROM dimension_conf
          WHERE owner = SELF.owner AND table_name = SELF.table_name;
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
         SELECT stragg( column_name )
           INTO l_scd2_dates
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

      evolve_log.log_msg( 'The SCD2 date list: ' || l_scd2_dates, 5 );

      -- get a comma separated list of scd2 attributes that are numbers
      -- use the STRAGG function for this
      BEGIN
         SELECT stragg( column_name )
           INTO l_scd2_nums
           FROM column_conf ic
	   JOIN all_tab_columns
		USING (owner,table_name,column_name)
          WHERE owner = SELF.owner AND table_name = SELF.table_name AND column_type = 'scd type 2'
	    AND data_type = 'NUMBER';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            -- if there are no type 2 attributes, that is fine
            NULL;
      END;

      evolve_log.log_msg( 'The SCD 2 number list: ' || l_scd2_nums, 5 );
      
      -- get a comma separated list of scd2 date columns
      -- use the STRAGG function for this
      BEGIN
         SELECT stragg( column_name )
           INTO l_scd2_chars
           FROM column_conf ic
	   JOIN all_tab_columns
		USING (owner,table_name,column_name)
          WHERE owner = SELF.owner AND table_name = SELF.table_name AND column_type = 'scd type 2'
	    AND data_type NOT IN ('DATE','NUMBER');
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            -- if there are no type 2 attributes, that is fine
            NULL;
      END;

      evolve_log.log_msg( 'The SCD 2 DATE list: ' || l_scd2_chars, 5 );

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

      evolve_log.log_msg( 'The SCD 2 complete list: ' || l_scd1_list, 5 );

      -- construct a list of all scd2 attributes
      -- if any of the variables are null, we may get a ',,' or a ',' at the end of the list
      -- use the regexp_replace to remove that
      l_scd2_list          :=
      regexp_replace(l_scd2_dates ||','|| l_scd2_nums||','|| l_scd2_chars,'(,)(,|$)','\2');


      -- construct a list of all scd attributes
      -- this is a combined list of all scd1 and scd2 attributes
      -- if any of the variables are null, we may get a ',,'
      -- use the regexp_replace to remove that
      -- also need the regexp to remove an extra comma at the end if that appears
      l_scd_list          :=
      regexp_replace(l_scd2_list ||','|| l_scd1_list,'(,)(,|$)','\2');

      evolve_log.log_msg( 'The SCD list: ' || l_scd_list, 5 );
      -- construct the include case statement
      -- this case statement determines which records from the staging table are included as new rows
      l_include_case      :=
            'CASE WHEN '||surrogate_key_col||' <> '||l_stage_key||' THEN ''Y'''
         || REGEXP_REPLACE( l_scd2_nums,
                            '(\w+)(,|$)',
                            'when nvl(\1,'||l_num_nvl||') <> nvl(lag(\1) over (partition by '
                            || natural_key_list
                            || ' order by '
                            || effect_dt_col
                            || '),'||l_num_nvl||') then ''Y'' '
                          )
         || REGEXP_REPLACE( l_scd2_chars,
                            '(\w+)(,|$)',
                            'when nvl(\1,'''||l_char_nvl||''') <> nvl(lag(\1) over (partition by '
                            || natural_key_list
                            || ' order by '
                            || effect_dt_col
                            || '),'''||l_char_nvl||''') then ''Y'' '
                          )
      || REGEXP_REPLACE( l_scd2_dates,
                            '(\w+)(,|$)',
                         'when nvl(\1,'''||l_date_nvl||''') <> nvl(lag(\1) over (partition by '
                            || natural_key_list
                            || ' order by '
                            || effect_dt_col
                            || '),'''||l_date_nvl||''') then ''Y'' '
                          )
         || ' else ''N'' end include';
      evolve_log.log_msg( 'The include CASE: ' || l_include_case, 5 );
      -- construct the scd1 analytics list
      -- this is a list of all the LAST_VALUE statements needed for the final statement
      l_scd1_analytics    :=
         REGEXP_REPLACE( l_scd1_list,
                         '(\w+)(,|$)',
                            'last_value(\1) over (partition by '
                         || natural_key_list
                         || ' order by '
                         || effect_dt_col
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
	 || '('
         || surrogate_key_col
         || ','
         || natural_key_list
         || ','
         || l_scd_list
         || ','
         || effect_dt_col
         || ','
         || expire_dt_col
         || ','
         || current_ind_col
	 ||') '
         || ' SELECT case '
         || surrogate_key_col
         || ' when '||l_stage_key||' then '
         || SELF.full_sequence
         || '.nextval else '
         || surrogate_key_col
         || ' end '
         || surrogate_key_col
         || ','
         || natural_key_list
         || ','
         || l_scd_list
         || ','
         || effect_dt_col
         || ','
         || 'nvl( lead('
         || effect_dt_col
         || ') OVER ( partition BY '
         || natural_key_list
         || ' ORDER BY '
         || effect_dt_col
         || '), to_date(''12/31/9999'',''mm/dd/yyyy'')) '
         || expire_dt_col
         || ','
         || ' CASE MAX('
         || effect_dt_col
         || ') OVER (partition BY '
         || natural_key_list
         || ') WHEN '
         || effect_dt_col
         || ' THEN ''Y'' ELSE ''N'' END '
         || current_ind_col
         || ' from ('
         || 'SELECT '
         || surrogate_key_col
         || ','
         || natural_key_list
         || ','
         || l_scd1_analytics
         || l_scd2_list
         || ','
         || effect_dt_col
         || ','
         || l_include_case
         || ' from (select '
	 || l_stage_key
	 ||' '
         || surrogate_key_col
         || ','
         || natural_key_list
         || ','
         || effect_dt_col
         || ','
         || l_scd_list
         || ' from '
         || SELF.full_source
         || ' union select '
         || surrogate_key_col
         || ','
         || natural_key_list
         || ','
         || effect_dt_col
         || ','
         || l_scd_list
         || ' from '
         || SELF.full_table
         || ')'
         || ' order by '
         || natural_key_list
         || ','
         || effect_dt_col
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
	    -- partition exchange the staging table into the max partition of the target table
	    -- this requires that the dimension table is a single partition table
            td_dbutils.exchange_partition( p_source_owner      => staging_owner,
                                           p_source_table      => staging_table,
                                           p_owner             => owner,
                                           p_table             => table_name,
                                           p_statistics        => STATISTICS,
                                           p_concurrent        => concurrent
                                         );
         WHEN 'replace'
         THEN
	    -- switch the two tables using rename
	    -- requires that the tables both exist in the same schema
            td_dbutils.replace_table( p_owner             => owner,
                                      p_table             => table_name,
                                      p_source_table      => staging_table,
                                      p_statistics        => STATISTICS,
                                      p_concurrent        => concurrent
                                    );
	    
	    -- now drop the source table, which is now the previous target table
            td_dbutils.drop_table( p_owner => staging_owner,
				   p_table => staging_table );
         ELSE
            NULL;
   END CASE;
   
   -- reset the evolve_object
   o_ev.clear_app_info;

   END LOAD;
END;
/

SHOW errors