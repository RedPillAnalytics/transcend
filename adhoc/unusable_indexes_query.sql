var p_table VARCHAR2(30)
var p_tablespace VARCHAR2(30)
var p_source_table VARCHAR2(30)
var p_source_owner VARCHAR2(30)
var p_constraint_regexp VARCHAR2(30)
var p_constraint_type VARCHAR2(30)
var p_seg_attributes VARCHAR2 (3)
var l_targ_part VARCHAR2(3)
var p_owner VARCHAR2(30)
var p_table VARCHAR2(30)
var p_handle_fkeys VARCHAR2(3)
var p_part_type VARCHAR2(30)
var p_index_type VARCHAR2(30)
var p_index_regexp VARCHAR2(30)

EXEC :p_tablespace := NULL;
EXEC :p_constraint_regexp := NULL;
EXEC :p_owner := 'stewart';
EXEC :p_table := 'test1_prd';
EXEC :p_source_owner := 'stewart';
EXEC :p_source_table := 'test2_prd';
EXEC :p_constraint_type := NULL;
EXEC :p_seg_attributes := 'no';
EXEC :l_targ_part := 'no';
EXEC :p_handle_fkeys := 'yes';
--EXEC :p_part_type := 'local';
EXEC :p_index_type := NULL;
EXEC :p_index_regexp := NULL;

SELECT DISTINCT    'alter index '
       || owner
       || '.'
       || index_name
       || CASE idx_ddl_type
       WHEN 'I'
       THEN NULL
       ELSE ' modify partition ' || partition_name
       END
       || ' unusable' ddl,
       idx_ddl_type, partition_name, partition_position,
       SUM( CASE idx_ddl_type
            WHEN 'I'
            THEN 1
            ELSE 0
            END ) OVER( partition BY 1 ) num_indexes,
       SUM( CASE idx_ddl_type
            WHEN 'P'
            THEN 1
            ELSE 0
            END ) OVER( partition BY 1 ) num_partitions
  FROM ( SELECT index_type, owner, ai.index_name, partition_name,
                partition_position, partitioned,
                CASE
                WHEN partition_name IS NULL
             OR partitioned = 'NO'
                THEN 'I'
                ELSE 'P'
                END idx_ddl_type
           FROM partname right JOIN all_ind_partitions aip USING( partition_name )
                right JOIN all_indexes ai
                ON ai.index_name = aip.index_name
            AND ai.owner = aip.index_owner
          WHERE table_name = upper( :p_table )
            AND table_owner = upper( :p_owner )
            AND ( ai.status = 'VALID' OR aip.status = 'USABLE' ))
 WHERE REGEXP_LIKE( index_type, '^' || :p_index_type, 'i' )
   AND REGEXP_LIKE( partitioned,
                    CASE
                    WHEN REGEXP_LIKE( 'global', :p_part_type, 'i' )
                    THEN 'NO'
                    WHEN REGEXP_LIKE( 'local', :p_part_type, 'i' )
                    THEN 'YES'
                    ELSE '.'
                    END,
                    'i'
                  )
       -- USE an NVL'd regular expression to determine specific indexes to work on
   AND REGEXP_LIKE( index_name, nvl( :p_index_regexp, '.' ), 'i' )
   AND NOT REGEXP_LIKE( index_type, 'iot', 'i' )
 ORDER BY idx_ddl_type, partition_position