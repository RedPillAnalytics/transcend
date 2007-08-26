VAR p_table VARCHAR2(30)
VAR p_tablespace VARCHAR2(30)
VAR p_source_table VARCHAR2(30)
VAR p_source_owner VARCHAR2(30)
VAR p_seg_attributes VARCHAR2(3)
VAR p_owner VARCHAR2(30)
VAR p_table VARCHAR2(30)
VAR p_partitioning VARCHAR2(3)
VAR p_tablespace VARCHAR2(30)

EXEC :p_tablespace := NULL;
EXEC :p_owner := 'whimport';
EXEC :p_table := 'ar_transaction_stg';
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


SELECT REGEXP_REPLACE
          ( REGEXP_REPLACE
               ( REGEXP_REPLACE
                    ( REGEXP_REPLACE
                         ( DBMS_METADATA.get_ddl( 'TABLE', table_name, owner ),
                           CASE
                              -- don't want partitioning
                           WHEN td_ext.get_yn_ind(:p_partitioning) = 'no'
                                 -- remove all partitioning
                           THEN '(\(\s*partition.+\))[[:space:]]*|(partition by).+\)[[:space:]]*'
                              ELSE NULL
                           END,
                           NULL,
                           1,
                           0,
                           'in'
                         ),
                      '(tablespace)(\s*)([^ ]+)([[:space:]]*)',
                      '\1\2' || :p_tablespace || '\4',
                      1,
                      0,
                      'i'
                    ),
                 '(\."?)(' || :p_source_table || ')(\w*)("?)',
                 '.' || :p_table || '\3',
                 1,
                 0,
                 'i'
               ),
            '(")?(' || :p_source_owner || ')("?\.)',
            :p_owner || '.',
            1,
            0,
            'i'
          ) table_ddl
  FROM all_tables
 WHERE owner = UPPER( :p_source_owner ) AND table_name = UPPER( :p_source_table )