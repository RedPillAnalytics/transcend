SET echo off
SET feedback off
SET timing off

var p_table VARCHAR2(30)
var p_owner VARCHAR2(30)
var p_table VARCHAR2(30)
var p_index_type VARCHAR2(30)
var p_index_regexp VARCHAR2(30)
var p_part_type VARCHAR2(30)
var p_partname VARCHAR2(30)
var p_source_object VARCHAR2(30)
var l_part_type VARCHAR2(10)

EXEC :p_owner := 'eusb009';
EXEC :p_table := 'insurquote_policy';
EXEC :p_part_type := NULL;
EXEC :p_index_type := NULL;
EXEC :p_index_regexp := NULL;
EXEC :p_partname := NULL;
EXEC :p_source_object := 'insurquote_policy_stg';
EXEC :l_part_type := 'subpart';

SET feedback on
SET echo on
SET timing on

SELECT *
  FROM ( SELECT DISTINCT    'alter index '
                || owner
                || '.'
                || index_name
                || CASE idx_ddl_type
                WHEN 'I'
                THEN NULL
                ELSE ' modify partition ' || partition_name
                END
                || ' unusable' DDL,
                idx_ddl_type, partition_name,
                SUM( CASE idx_ddl_type
                     WHEN 'I'
                     THEN 1
                     ELSE 0
                     END ) OVER( PARTITION BY 1 ) num_indexes,
                SUM( CASE idx_ddl_type
                     WHEN 'P'
                     THEN 1
                     ELSE 0
                     END ) OVER( PARTITION BY 1 ) num_partitions,
                CASE idx_ddl_type
                WHEN 'I'
                THEN ai_status
                ELSE aip_status
                END status, include, partition_position
           FROM ( SELECT partition_position, index_type, owner, ai.index_name, partition_name,
                         partitioned, aip.status aip_status,
                         ai.status ai_status,
                         CASE
                         WHEN partition_name IS NULL OR partitioned = 'NO'
                         THEN 'I'
                         ELSE 'P'
                         END idx_ddl_type,
                         CASE
                         WHEN( p_source_object IS NOT NULL OR p_partname IS NOT NULL
                             )
                     AND ( partitioned = 'YES' )
                     AND partition_name IS NULL
                         THEN 'N'
                         ELSE 'Y'
                         END include
                    FROM td_part_gtt JOIN 
                         (  SELECT DISTINCT CASE l_part_type WHEN 'subpart' THEN subpartition_name ELSE partition_name END partition_name,
                                   index_owner,
                                   index_name,
                                   CASE l_part_type WHEN 'subpart' THEN isp.status ELSE ip.status END status
                              FROM all_ind_partitions ip
                              left JOIN all_ind_subpartitions isp
                                   USING (index_owner, index_name, partition_name)
                         ) aip USING( partition_name )
                   RIGHT JOIN all_indexes ai
                         ON ai.index_name = aip.index_name AND ai.owner = aip.index_owner
                   WHERE ai.table_name = UPPER( p_table )
                     AND ai.table_owner = UPPER( p_owner ))
          WHERE REGEXP_LIKE( index_type, '^' || p_index_type, 'i' )
            AND REGEXP_LIKE( partitioned,
                             CASE
                             WHEN REGEXP_LIKE( 'global', p_part_type, 'i' )
                             THEN 'NO'
                             WHEN REGEXP_LIKE( 'local', p_part_type, 'i' )
                             THEN 'YES'
                             ELSE '.'
                             END,
                             'i'
                           )
                -- USE an NVL'd regular expression to determine specific indexes to work on
            AND REGEXP_LIKE( index_name, NVL( p_index_regexp, '.' ), 'i' )
            AND NOT REGEXP_LIKE( index_type, 'iot', 'i' )
            AND include = 'Y'
          ORDER BY idx_ddl_type )
 WHERE status IN( 'VALID', 'USABLE', 'N/A' )
