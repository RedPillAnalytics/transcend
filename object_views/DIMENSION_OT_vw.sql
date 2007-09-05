CREATE OR REPLACE VIEW dimension_ot
OF dimensiontype
WITH object identifier (owner, table_name)
as
SELECT DISTINCT owner,
       table_name,
       owner||'.'||table_name full_table,
       source_owner,
       source_object,
       source_owner||'.'||source_object full_source,
       staging_owner,
       staging_table,
       staging_owner||'.'||staging_table full_stage,
       constant_staging,
       direct_load,
       replace_method,
       'insert /*+ APPEND */ into '
       ||full_stage
       ||' SELECT '||sel1||' from ('
       ||'SELECT '||sk||','||nk||','
       ||scd1_analytics||','
       ||scd2_list||','
       ||esd||','
       ||include_list
       ||' from '
       ||union_list
       ||' order by '||nk||','||esd
       ||')'
       ||' where include=''Y''' load_sql
  FROM (SELECT ROWNUM rn,
	       owner,
	       table_name,
	       source_owner,
	       source_object,
	       staging_owner,
	       staging_table,
	       staging_owner||'.'||staging_table full_stage,
	       CASE
	       WHEN staging_table IS NULL
	       THEN
	       'yes'
	       ELSE
	       'no'
	       END constant_staging,
	       direct_load,
	       replace_method,
	       sk,
	       nk,
	       esd,
	       scd1_list,
	       scd2_list,
	       scd_list,
	       'CASE '||sk||' when -.1 then '||sequence_owner||'.'||sequence_name||'.nextval else '||sk||' end '||sk||','
	       ||nk||','||scd_list||','
	       ||esd||',' 
	       || 'nvl( lead('||esd||') OVER ( partition BY '||nk||' ORDER BY '||esd||'), to_date(''12/31/9999'',''mm/dd/yyyy'')) '||eed||',' 
	       || ' CASE MAX('||esd||') OVER (partition BY '||nk||') WHEN '||esd||' THEN ''Y'' ELSE ''N'' END '||ci sel1,
	       (SELECT stragg('last_value('||column_name||') over (partition by '||nk||' order by '||esd||' ROWS BETWEEN unbounded preceding AND unbounded following) '||column_name) OVER ( partition BY column_type)
		  FROM column_conf ic
		 WHERE ic.owner=owner
		   AND ic.table_name=table_name
		   AND ic.column_type='scd type 1') scd1_analytics,
	       '(select -.1 '
	       ||sk||','
	       ||nk||','
	       ||esd||','
	       ||scd_list
	       ||' from '||source_owner||'.'||source_object
	       ||' union select '
	       ||sk||','
	       ||nk||','
	       ||esd||','
	       ||scd_list
	       ||' from '||owner||'.'||table_name||')' union_list,
	       'case when '||sk||' <> -.1 then ''Y'' when '||esd||'=LAG(effect_start_dt) over (partition by '||nk||' order by '||esd||','||sk||' desc) then ''N'''
	       ||(SELECT regexp_replace(stragg(' WHEN nvl('||column_name||',-.01) < > nvl(LAG('||column_name||') OVER (partition BY '||nk||' ORDER BY '||esd||'),-.01) THEN ''Y'''),', WHEN',' WHEN')
		    FROM column_conf ic
		   WHERE ic.owner=owner
		     AND ic.table_name=table_name
		     AND column_type='scd type 2') 
	       ||' else ''N'' end include' include_list
	  FROM (SELECT upper(owner) owner,
		       upper(table_name) table_name,
		       column_type,
		       column_name,
		       upper(source_object) source_object,
		       upper(source_owner) source_owner,
		       upper(nvl(staging_owner,owner)) staging_owner,
		       upper(nvl(staging_table,'TD$' || table_name)) staging_table,
		       upper(sequence_owner) sequence_owner,
		       upper(sequence_name) sequence_name,
		       direct_load,
		       replace_method,
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
		       USING (owner,table_name)))
/