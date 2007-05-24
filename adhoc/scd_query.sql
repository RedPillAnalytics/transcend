SELECT CASE test_key 
       WHEN -1 
       THEN test_dim_seq.nextval 
       ELSE test_key 
       END test_key,
       nat_key,
       birthdate,
       name,
       zip,
       zip_plus4,
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
	       -- all non-SCD columns (or SCD type 0) records just need to update the most recent, surviving record
	       last_value(zip_plus4) OVER ( partition BY nat_key  
					    ORDER BY effect_start_dt
					    ROWS BETWEEN CURRENT ROW AND CASE test_key WHEN -1 THEN 1 ELSE 0 end following) zip_plus4,
	       effect_start_dt,
	       -- we now exclude records based on what the value is directly preceeding the record
	       -- the list of requirements in the CASE statement needs to include all Type 2 attributes
	       CASE
	       WHEN zip <> LAG(zip) OVER (partition BY nat_key ORDER BY effect_start_dt)
	       THEN 'Y'
	       WHEN name <> LAG(name) OVER (partition BY nat_key ORDER BY effect_start_dt)
	       THEN 'Y'
	       ELSE
	       CASE test_key
	       WHEN -1 THEN 'N'
	       END
	       END include
	  FROM (SELECT nat_key,
		       effect_start_dt,
		       -- FOR now, use a -1 for the surrogate key
		       -- selecting from a sequence is not allowed inside analytics statements
		       -- it'll be cased later
		       -- the -1 also tells us which records are new
		       -1 test_key,
		       birthdate,
		       name,
		       zip,
		       zip_plus4
		  FROM test_stg
		       UNION
		SELECT nat_key,
		       effect_start_dt,
		       test_key,
		       birthdate,
		       name,
		       zip,
		       zip_plus4 
		  FROM test_dim)
	 ORDER BY nat_key, effect_start_dt)
 WHERE include='Y'
/
