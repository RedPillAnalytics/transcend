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
  FROM (SELECT * FROM (
SELECT complete_data.*,
       CASE MAX(effect_start_dt) OVER (partition BY nat_key, source)
       WHEN effect_start_dt THEN 'Y'
       ELSE 'N'
       END max_source_dt_ind,
       'Y' include
  FROM (SELECT nat_key,
               effect_start_dt,
               -1 test_key,
               birthdate,
               name,
               zip,
               'S' source
          FROM test_stg
               UNION
        SELECT nat_key,
               effect_start_dt,
               test_key,
               birthdate,
               name,
               zip,
               'D' source
          FROM test_dim) complete_data)
 MODEL partition BY ( nat_key)
       dimension BY ( source,
                      max_source_dt_ind
                    )
       measures ( test_key,
                  birthdate,
                  name,
                  zip,
                  include,
                  effect_start_dt)
       UNIQUE single reference
       rules ( -- SET the include flag to 'Y' or 'N' to determine whether to include the row from STG table
               include['S','Y'] = CASE
               WHEN name['S','Y'] <> name['D','Y']
               OR zip['S','Y'] <> zip['D','Y']
               THEN 'Y'
               ELSE 'N'
               END,
               include['S','N'] = CASE
               WHEN name['S','N'] <> name['S','Y']
               OR zip['S','N'] <> zip['S','Y']
               THEN 'Y'
               ELSE 'N'
               END,
               -- SET the TYPE 1 attribute to the new value for all records
               birthdate[ANY,ANY] = CASE WHEN birthdate['S','Y'] IS present then birthdate['S','N'] ELSE birthdate['D','Y'] end 
             )
       ) include_data
 WHERE include='Y'
/
