SELECT CASE test_key 
       WHEN -1 
       THEN test_dim_seq.nextval 
       ELSE test_key 
       END test_key,
       nat_key,
       birthdate,
       name,
       zip,
       effect_start_dt,
       nvl(lead(effect_start_dt) OVER (partition BY nat_key ORDER BY effect_start_dt), to_date('12/31/9999','mm/dd/yyyy')) effect_end_dt,
       CASE MAX(effect_start_dt) OVER (partition BY nat_key)
       WHEN effect_start_dt
       THEN 'Y'
       ELSE 'N'
       END current_ind
  FROM (SELECT * 
	  FROM ( SELECT complete_data.*,
			-- SET the max record based on dt for both source data and dimension data
			CASE MAX(effect_start_dt) OVER (partition BY nat_key, source)
			WHEN effect_start_dt THEN 'Y'
			ELSE 'N'
			END max_source_dt_ind,
			'Y' include
		   FROM (SELECT nat_key,
				effect_start_dt,
				-- FOR now, use a -1 for the surrogate key
				-- selecting from a sequence is not allowed inside analytics statements
				-- it'll be cased later
				-1 test_key,
				birthdate,
				name,
				zip,
				-- need to identify where each record came from 
				'S' source
			   FROM test_stg
				UNION
			 SELECT nat_key,
				effect_start_dt,
				test_key,
				birthdate,
				name,
				zip,
				-- need to identify where each record came from 
				'D' source
			   FROM test_dim) complete_data)
	 MODEL partition BY ( nat_key)
	       -- dimension by the source of the data, and whether each record is max within that source
	       dimension BY ( source,
			      max_source_dt_ind
			    )
	       -- measures have to include everything not in partition and dimension expressions
	       measures ( test_key,
			  birthdate,
			  name,
			  zip,
			  include,
			  effect_start_dt)
	       UNIQUE single reference
	       rules ( -- SET the include flag to 'Y' or 'N' to determine whether to keep the MAX value from source
		       -- decision is made based on comparing all the type2 attributes
		       include['S','Y'] = CASE
		       WHEN name['S','Y'] <> name['D','Y']
		       OR zip['S','Y'] <> zip['D','Y']
		       THEN 'Y'
		       ELSE 'N'
		       END,
		       -- SET the include flag to 'Y' or 'N' to determine whether to keep other values from source
		       -- decision is made based on comparing all the type2 attributes
		       include['S','N'] = CASE
		       WHEN name['S','N'] <> name['S','Y']
		       OR zip['S','N'] <> zip['S','Y']
		       THEN 'Y'
		       ELSE 'N'
		       END,
		       -- SET the TYPE 1 attribute to the value of the max source record if it exists
		       -- otherwise set it to the value for the max dimension record (which it should have been to begin with)
		       -- there needs to be a rule here for every type1 attribute
		       birthdate[ANY,ANY] = CASE WHEN birthdate['S','Y'] IS present THEN birthdate['S','Y'] ELSE birthdate['D','Y'] END 
		     )
       ) include_data
       -- ONLY get records that we've decided to include
 WHERE include='Y'
/
