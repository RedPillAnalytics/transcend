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
       -- effective end date is always based on EFFECTIVE_START_DT of proceeding record
       nvl( lead(effect_start_dt) OVER 
	    ( partition BY nat_key 
	      ORDER BY effect_start_dt), to_date('12/31/9999','mm/dd/yyyy')) effect_end_dt,
       CASE MAX(effect_start_dt) 
       OVER (partition BY nat_key)
       WHEN effect_start_dt
       THEN 'Y'
       ELSE 'N'
       END current_ind
  FROM (SELECT test_key,
	       nat_key,
	       -- will need a LAST_VALUE function for each Type1 attribute
	       -- notice the LAST_VALUE looks at all records, not just staging records
	       -- if the staging table is filled with records predating the last dimension record,
	       -- then we would want the value of the dimension record to pervade
	       -- we also do this prior to excluding records
	       -- that's because we may want a type1 attribute from an excluded record
	       last_value(birthdate) OVER ( partition BY nat_key 
					    ORDER BY effect_start_dt 
					    ROWS BETWEEN unbounded preceding AND unbounded following) birthdate,
	       name,
	       zip,
	       effect_start_dt,
	       source,
	       -- we now exclude records based on what the value is directly preceeding the record
	       -- the list of requirements in the CASE statement needs to include all Type 2 attributes
	       CASE 
	       WHEN 
	       ( zip = lag(zip) OVER (partition BY nat_key ORDER BY effect_start_dt) 
		 and name = lag(name) OVER (partition BY nat_key ORDER BY effect_start_dt))
	       -- only consider excluding staging records... never dimension records
	       AND source='S'
	       THEN 'N'
	       ELSE 'Y'
	       END include
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
		  FROM test_dim)
	 ORDER BY nat_key, effect_start_dt)
 WHERE include='Y'
/
