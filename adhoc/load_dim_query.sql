SELECT DISTINCT owner,
       table_name,
       (SELECT stragg('last_value('||column_name||') over (partition by '||nk||' order by '||esd||' ROWS BETWEEN unbounded preceding AND unbounded following)') OVER ( partition BY column_type)
	  FROM column_conf ic
	 WHERE ic.owner=owner
	   AND ic.table_name=table_name
	   AND ic.column_type='scd type 1') scd1_list,
       '(select -.1 '
       ||sk||','
       ||nk||','
       ||esd||','
       ||(SELECT stragg(column_name)
	  FROM column_conf ic
	 WHERE ic.owner=owner
	   AND ic.table_name=table_name
	     AND REGEXP_LIKE(ic.column_type,'scd','i'))
       ||' from '||source_owner||'.'||source_object
       ||' union select '
       ||sk||','
       ||nk||','
       ||esd||','
       ||(SELECT stragg(column_name)
	  FROM column_conf ic
	 WHERE ic.owner=owner
	   AND ic.table_name=table_name
	     AND REGEXP_LIKE(ic.column_type,'scd','i'))
       ||' from '||owner||'.'||table_name||')' union_list,
       'case when '||nk||' <> -.1 then ''Y'' when '||esd||'=LAG(effect_start_dt) over (partition by '||nk||' order by '||esd||','||sk||' desc) then ''N'' when ' scd2_list
  FROM (SELECT owner,
	       table_name,
	       column_type,
	       column_name,
	       source_object,
	       source_owner,
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