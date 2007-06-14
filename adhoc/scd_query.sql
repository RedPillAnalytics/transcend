COL name format a13
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
	       -- IF the staging table is filled with records predating the last dimension record,
	       -- THEN we would want the value of the dimension record to pervade
	       -- we also do this prior to excluding records
	       -- that's because we may want a type1 attribute from an excluded record
	       last_value(birthdate) OVER ( partition BY nat_key
					    ORDER BY effect_start_dt
					    ROWS BETWEEN unbounded preceding AND unbounded following) birthdate,
	       name,
	       zip,
	       zip_plus4,
	       effect_start_dt,
	       -- now do a series of comparisons of each record to see whether the value of the INCLUDE column is 'Y' or 'N'
	       CASE
	       -- if a record is an existing DIM record, then we know we want to include it
	       WHEN test_key <> -1
	       THEN 'Y'
	       -- if we ever have a record where the effective date and expiry date are the same, they should be excluded
	       -- this situation makes no logical sense, and usually wouldn't happen
	       -- this can sometimes occur on a rerun of a dimensional load
	       WHEN effect_start_dt=LAG(effect_start_dt) OVER (partition BY nat_key ORDER BY effect_start_dt,test_key desc)
	       THEN 'N'
	       -- we now exclude records based on comparing all type2 attributes with the preceeding record
	       -- if any of these attributes are different, then the record would get excluded
	       -- want to include any record that has a change in a type2 field compared to the previous record
	       -- have to do nvl's based on datatypes of both operands to make sure that we are not catching an 'unknown'
	       -- nvl'ing to values that don't naturally exist in the data
	       WHEN nvl(zip,-.01) <> nvl(LAG(zip) OVER (partition BY nat_key ORDER BY effect_start_dt),-.01)
	       THEN 'Y'
	       WHEN nvl(zip_plus4,-.01) <> nvl(LAG(zip_plus4) OVER (partition BY nat_key ORDER BY effect_start_dt),-.01)
	       THEN 'Y'
	       WHEN nvl(name,-.01) <> nvl(LAG(name) OVER (partition BY nat_key ORDER BY effect_start_dt),-.01)
	       THEN 'Y'
	       -- any record that hasn't been caught yet has no type2 changes
	       -- that record should not be included
	       ELSE 'N'
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