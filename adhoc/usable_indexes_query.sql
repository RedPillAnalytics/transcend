SET echo off
SET feedback off
SET timing off

var p_table VARCHAR2(30)
var p_owner VARCHAR2(30)
var p_table VARCHAR2(30)
var p_index_type VARCHAR2(30)
var p_index_regexp VARCHAR2(30)
var p_part_type VARCHAR2(30)

EXEC :p_owner := 'eusb009';
EXEC :p_table := 'insurquote_policy';
EXEC :p_index_type := NULL;
EXEC :p_index_regexp :=NULL;

SET feedback on
SET echo on
SET timing on

SELECT  DISTINCT table_name, partition_position,
       'alter table '
       || table_owner
       || '.'
       || table_name
       || ' modify '
       || CASE part_type WHEN 'subpart' THEN 'subpartition ' ELSE 'partition ' end
       || partition_name
       || ' rebuild unusable local indexes' DDL,
       partition_name,
       part_type
  FROM ( SELECT table_name,
                CASE WHEN subpartition_name IS NULL THEN partition_position ELSE subpartition_position END partition_position,
                table_owner,
                CASE WHEN subpartition_name IS NULL THEN partition_name ELSE subpartition_name END partition_name,
                CASE WHEN subpartition_name IS NULL THEN 'part' ELSE 'subpart' END part_type
           FROM  all_tab_partitions ip
           left JOIN all_tab_subpartitions isp
                USING (table_owner, table_name, partition_name )) tabs 
  JOIN ( SELECT table_name,
                table_owner,
                CASE WHEN subpartition_name IS NULL THEN ip.partition_name ELSE isp.subpartition_name END partition_name,
                CASE WHEN subpartition_name IS NULL THEN ip.status ELSE isp.status END status
           FROM all_indexes ix
           JOIN all_ind_partitions ip
                ON ix.owner = ip.index_owner
            AND ix.index_name = ip.index_name
           left JOIN all_ind_subpartitions isp
                ON ip.index_owner = isp.index_owner
            AND ip.index_name = isp.index_name
            AND ip.partition_name = isp.partition_name ) inds
       USING (table_owner, table_name, partition_name)
 WHERE table_name = UPPER( :p_table ) 
   AND table_owner = UPPER( :p_owner )
   AND status = 'UNUSABLE'
 ORDER BY table_name, partition_position
