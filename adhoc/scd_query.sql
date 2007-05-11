SELECT -- the staging records don't have a surrogate key yet
       -- the '-1' is a constant for all records from the staging table
       CASE test_key 
         WHEN -1 
            THEN test_dim_seq.nextval 
            ELSE test_key 
       END test_key,
       -- now just select out the rest of the records
       nat_key,
       birthdate,
       name,
       zip,
       effect_start_dt,
       effect_end_dt,
       new_current_ind current_ind
  FROM ( SELECT *
           FROM (SELECT nat_key,
                        'S' source,
			nvl(effect_start_dt,SYSDATE) effect_start_dt,
			to_date('12/31/9999','mm/dd/yyyy') effect_end_dt,
                        'N' current_ind,
                        'Y' new_current_ind,
                        -1 test_key,
                        birthdate,
                        name,
			zip,
                        'Y' include
                   FROM test_stg
                        UNION
                 SELECT nat_key,
                        'D' source,
			effect_start_dt,
			effect_end_dt,
                        current_ind,
                        current_ind new_current_ind,
                        test_key,
                        birthdate,
                        name,
			zip,
                        'Y' include
                   FROM test_dim)
          MODEL partition BY ( nat_key)
                dimension BY ( source,
                               current_ind)
                measures ( test_key,
                           birthdate,
                           name,
			   zip,
                           include,
			   effect_start_dt,
			   effect_end_dt,
                           new_current_ind)
                UNIQUE single reference
                rules ( -- SET the include flag to 'Y' or 'N' to determine whether to include the row from STG table
                        include['S','N'] = CASE 
			                        WHEN name['S','N'] <> name['D','Y'] 
		                                  OR zip['S','N'] <> zip['D','Y'] 
			                        THEN 'Y' 
			                        ELSE 'N' 
					   END,
                        -- SET the CURRENT_IND flag of the current record to 'Y' or 'N'
                        effect_end_dt['D','Y'] = CASE 
			                              WHEN name['S','N'] <> name['D','Y'] 
			                                OR zip['S','N'] <> zip['D','Y'] 
			                              THEN effect_start_dt['S','N'] 
			                              ELSE effect_end_dt['S','N'] 
						 END,
                        -- SET the effect dates
                        new_current_ind['D','Y'] = CASE 
			                              WHEN name['S','N'] <> name['D','Y'] 
			                                OR zip['S','N'] <> zip['D','Y'] 
			                              THEN 'N' 
			                              ELSE 'Y' 
						 END,
                        -- SET the TYPE 1 attribute to the new value for all records
                        birthdate['D',ANY] = birthdate['S','N']
                      )
          ORDER BY nat_key,source,effect_start_dt)
 WHERE include='Y'
/