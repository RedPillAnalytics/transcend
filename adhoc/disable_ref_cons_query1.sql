SELECT *
  FROM all_constraints
 WHERE constraint_type = 'R'
   AND status = 'ENABLED'
   AND REGEXP_LIKE( constraint_name,
                    nvl( 'dim', '.' ),
                    'i'
		  )
   AND r_constraint_name IN(
                             SELECT constraint_name
                               FROM all_constraints
                              WHERE table_name = upper( 'customer_dim' )
                                AND owner = upper( 'whdata' )
                                AND constraint_type = 'P' );