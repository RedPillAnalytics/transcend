var p_table VARCHAR2(30)
var p_tablespace VARCHAR2(30)
var p_source_table VARCHAR2(30)
var p_source_owner VARCHAR2(30)
var p_seg_attributes VARCHAR2(3)
var p_owner VARCHAR2(30)
var p_table VARCHAR2(30)
var p_partitioning VARCHAR2(3)
var p_tablespace VARCHAR2(30)

EXEC :p_tablespace := NULL;
EXEC :p_owner := NULL;
EXEC :p_table := NULL;
EXEC :p_source_owner := 'whdata';
EXEC :p_source_table := 'ar_transaction_fact';
EXEC :p_seg_attributes := 'no';
EXEC :p_partitioning := 'no';
EXEC :p_tablespace := 'whimportd';

-- want constraints as alters
EXEC dbms_metadata.set_transform_param(dbms_metadata.session_transform,'CONSTRAINTS',FALSE);
EXEC dbms_metadata.set_transform_param(dbms_metadata.session_transform,'REF_CONSTRAINTS',FALSE);
-- execute immediate doesn't like ";" on the end
EXEC DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform,'SQLTERMINATOR',FALSE);
-- we need the segment attributes so things go where we want them to
EXEC DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform,'SEGMENT_ATTRIBUTES',TRUE);
-- don't want all the other storage aspects though
EXEC DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform, 'STORAGE',FALSE );


SELECT    regexp_replace(REGEXP_REPLACE(DBMS_METADATA.get_ddl( 'TABLE', table_name, owner ),
                 CASE
                 -- don't want partitioning
                 WHEN :p_partitioning = 'no'
                 -- remove all partitioning
                 THEN '(\(\s*partition.+\))[[:space:]]*|(partition by).+\)[[:space:]]*'
                 ELSE NULL
                 END,
                 NULL,
                 1,
					 0,'in'),'(tablespace)(\s*)([^ ]+)([[:space:]]*)','\1\2'||:p_tablespace||'\4',1,0,'i') table_ddl
  from all_tables
 where owner=upper(:p_source_owner)
   AND table_name=upper(:p_source_table)

