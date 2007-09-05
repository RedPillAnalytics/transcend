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

EXEC :p_owner := 'whdata';
EXEC :p_table := 'ar_transaction_fact';
EXEC :p_part_type := NULL;
EXEC :p_index_type := NULL;
EXEC :p_index_regexp := '_r$';
EXEC :p_partname := NULL;
EXEC :p_source_object := NULL;

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
                     END
                   ) OVER( partition BY 1 ) num_partitions,
                CASE idx_ddl_type
                WHEN 'I'
                THEN ai_status
                ELSE aip_status
                END status,
		include
           FROM ( SELECT index_type, owner, ai.index_name,
                         partition_name, aip.partition_position,
                         partitioned, aip.status aip_status,
                         ai.status ai_status,
                         CASE
                         WHEN partition_name IS NULL
                      OR partitioned = 'NO'
                         THEN 'I'
                         ELSE 'P'
                         END idx_ddl_type,
			 CASE
			 WHEN ( p_source_object IS NOT NULL
				OR p_partname IS NOT NULL)
		     AND ( partitioned = 'YES')
			 THEN 'N'
			 ELSE 'Y'
			 END include		 
		    FROM partname JOIN all_ind_partitions aip
                         USING( partition_name )
                   right JOIN all_indexes ai
                         ON ai.index_name = aip.index_name
                     AND ai.owner = aip.index_owner
                   WHERE ai.table_name = upper( p_table )
                     AND ai.table_owner = upper( p_owner ))
          WHERE REGEXP_LIKE( index_type, '^' || p_index_type, 'i' )
            AND REGEXP_LIKE( partitioned,
                             CASE
                             WHEN REGEXP_LIKE( 'global',
                                               p_part_type,
                                               'i'
                                             )
                             THEN 'NO'
                             WHEN REGEXP_LIKE( 'local',
                                               p_part_type,
                                               'i'
                                             )
                             THEN 'YES'
                             ELSE '.'
                             END,
                             'i'
                           )
                -- USE an NVL'd regular expression to determine specific indexes to work on
            AND REGEXP_LIKE( index_name, nvl( p_index_regexp, '.' ),
                             'i' )
            AND NOT REGEXP_LIKE( index_type, 'iot', 'i' )
	    AND include = 'Y'
          ORDER BY idx_ddl_type, partition_position )
 WHERE status IN( 'VALID', 'USABLE', 'N/A' )