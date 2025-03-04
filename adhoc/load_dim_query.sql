SELECT 'SELECT '||sel1||' from ('
       ||'SELECT '||sk||','||nk||','
       ||scd1_analytics||','
       ||scd2_list||','
       ||efd||','
       ||include_list
       ||' from '
       ||union_list
       ||' order by '||nk||','||efd
       ||')'
       ||' where include=''Y''' dim_sql
  FROM (SELECT DISTINCT owner,
	       table_name,
	       sk,
	       nk,
	       efd,
	       scd1_list,
	       scd2_list,
	       scd_list,
	       'CASE '||sk||' when -.1 then '||sequence_owner||'.'||sequence_name||'.nextval else '||sk||' end '||sk||','
	       ||nk||','||scd_list||','
	       ||efd||',' 
	       || 'nvl( lead('||efd||') OVER ( partition BY '||nk||' ORDER BY '||efd||'), to_date(''12/31/9999'',''mm/dd/yyyy'')) '||exd||',' 
	       || ' CASE MAX('||efd||') OVER (partition BY '||nk||') WHEN '||efd||' THEN ''Y'' ELSE ''N'' END '||ci sel1,
	       (SELECT stragg('last_value('||column_name||') over (partition by '||nk||' order by '||efd||' ROWS BETWEEN unbounded preceding AND unbounded following) '||column_name) OVER ( partition BY column_type)
		  FROM column_conf ic
		 WHERE ic.owner=owner
		   AND ic.table_name=table_name
		   AND ic.column_type='scd type 1') scd1_analytics,
	       '(select -.1 '
	       ||sk||','
	       ||nk||','
	       ||efd||','
	       ||scd_list
	       ||' from '||source_owner||'.'||source_object
	       ||' union select '
	       ||sk||','
	       ||nk||','
	       ||efd||','
	       ||scd_list
	       ||' from '||owner||'.'||table_name||')' union_list,
	       'case when '||sk||' <> -.1 then ''Y'' when '||efd||'=LAG('||efd||') over (partition by '||nk||' order by '||efd||','||sk||' desc) then ''N'''
	       ||(SELECT regexp_replace(stragg(' WHEN nvl('||column_name||',-.01) < > nvl(LAG('||column_name||') OVER (partition BY '||nk||' ORDER BY '||efd||'),-.01) THEN ''Y'''),', WHEN',' WHEN')
		    FROM column_conf ic
		   WHERE ic.owner=owner
		     AND ic.table_name=table_name
		     AND column_type='scd type 2') 
	       ||' else ''N'' end include' include_list
	  FROM (SELECT owner,
		       table_name,
		       column_type,
		       column_name,
		       source_object,
		       source_owner,
		       sequence_owner,
		       sequence_name,
		       (SELECT stragg(column_name)
			  FROM column_conf ic
			 WHERE ic.owner=owner
			   AND ic.table_name=table_name
			   AND REGEXP_LIKE(ic.column_type,'scd','i')) scd_list,
		       (SELECT stragg(column_name)
			  FROM column_conf ic
			 WHERE ic.owner=owner
			   AND ic.table_name=table_name
			   AND column_type = 'scd type 1') scd1_list,
		       (SELECT stragg(column_name)
			  FROM column_conf ic
			 WHERE ic.owner=owner
			   AND ic.table_name=table_name
			   AND column_type = 'scd type 2') scd2_list,
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
			   AND ic.column_type='effective date') efd,
		       (SELECT column_name
			  FROM column_conf ic
			 WHERE ic.owner=owner
			   AND ic.table_name=table_name
			   AND ic.column_type='expiration date') exd,
		       (SELECT column_name
			  FROM column_conf ic
			 WHERE ic.owner=owner
			   AND ic.table_name=table_name
			   AND ic.column_type='current indicator') ci
		  FROM column_conf
		  JOIN dimension_conf
		       USING (owner,table_name)))
/