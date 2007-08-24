var p_table VARCHAR2(30)
var p_tablespace VARCHAR2(30)
var p_source_table VARCHAR2(30)
var p_source_owner VARCHAR2(30)
var p_seg_attributes VARCHAR2(3)
var p_owner VARCHAR2(30)
var p_table VARCHAR2(30)
var p_partitioning VARCHAR2(3)

EXEC :p_tablespace := NULL;
EXEC :p_owner := NULL;
EXEC :p_table := NULL;
EXEC :p_source_owner := 'whdata';
EXEC :p_source_table := 'ar_transaction_fact';
EXEC :p_seg_attributes := 'no';
EXEC :p_partitioning := 'yes';

-- want constraints as alters
EXEC dbms_metadata.set_transform_param(dbms_metadata.session_transform,'CONSTRAINTS',FALSE);
EXEC dbms_metadata.set_transform_param(dbms_metadata.session_transform,'REF_CONSTRAINTS',FALSE);
-- execute immediate doesn't like ";" on the end
EXEC DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform,'SQLTERMINATOR',FALSE);
-- we need the segment attributes so things go where we want them to
EXEC DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform,'SEGMENT_ATTRIBUTES',TRUE);
-- don't want all the other storage aspects though
EXEC DBMS_METADATA.set_transform_param( DBMS_METADATA.session_transform, 'STORAGE',FALSE );


select  SELECT    REGEXP_REPLACE
               -- dbms_metadata pulls the metadata for the source object out of the dictionary
               (    DBMS_METADATA.get_ddl( 'TABLE', index_name, owner ),
                 CASE
                 -- target is not partitioned and no tablespace provided
                 WHEN p_partitioning = 'no' AND p_tablespace IS NULL
                 -- remove all partitioning and the local keyword
                 THEN '(\(\s*partition.+\))|local'
                 -- target is not partitioned but tablespace is provided
                 WHEN p_partitioning = 'no' AND p_tablespace IS NOT NULL
                 -- strip out partitioned info and local keyword and tablespace clause
                 THEN '(\(\s*partition.+\))|local|(tablespace)\s*[^ ]+'
                 -- target is partitioned and tablespace is provided
                 WHEN p_partitioning = 'yes' AND p_tablespace IS NOT NULL
                 -- strip out partitioned info keeping local keyword and remove tablespace clause
                 THEN '(\(\s*partition.+\))|(tablespace)\s*[^ ]+'
                 ELSE NULL
                 END,
                 NULL,
                 1,
                 0,
                 'in'
               )
  from all_tables
 where owner=upper(:p_source_owner)
   AND table_name=upper(:p_source_table)

