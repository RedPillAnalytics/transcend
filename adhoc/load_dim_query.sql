SELECT DISTINCT owner,
       table_name,
       (SELECT stragg('last_value('||column_name||') over (partition by '||nk||' order by '||esd||' ROWS BETWEEN unbounded preceding AND unbounded following)') OVER ( partition BY column_type)
	  FROM column_conf ic
	 WHERE ic.owner=owner
	   AND ic.table_name=table_name
	   AND ic.column_type='scd type 1') scd1
  FROM (SELECT owner,
	       table_name,
	       column_type,
	       (SELECT column_name
		  FROM column_conf ic
		 WHERE ic.owner=owner
		   AND ic.table_name=table_name
		   AND ic.column_type='surrogate key') sk,
	       (SELECT column_name
		  FROM column_conf ic
		 WHERE ic.owner=owner
		   AND ic.table_name=table_name
		   AND ic.column_type='natural key') nk,
	       (SELECT column_name
		  FROM column_conf ic
		 WHERE ic.owner=owner
		   AND ic.table_name=table_name
		   AND ic.column_type='effective start date') esd,
	       (SELECT column_name
		  FROM column_conf ic
		 WHERE ic.owner=owner
		   AND ic.table_name=table_name
		   AND ic.column_type='effective end date') eed,
	       (SELECT column_name
		  FROM column_conf ic
		 WHERE ic.owner=owner
		   AND ic.table_name=table_name
		   AND ic.column_type='current indicator') ci
	  FROM column_conf
	  JOIN dimension_conf
	       USING (owner,table_name))
/