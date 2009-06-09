SET echo off
SET feedback off
SET timing off

var p_segment VARCHAR2(30)
var p_owner VARCHAR2(30)
var p_segment_type VARCHAR2(30)
var p_partname VARCHAR2(30)

EXEC :p_owner        := 'eusb009';
EXEC :p_segment      := 'insurquote_policy';
EXEC :p_segment_type := 'table';
EXEC :p_partname     := NULL;

SET feedback on
SET echo on
SET timing on

SELECT segment_type,
       part_type,
       count(*) num_segments
  FROM ( SELECT CASE 
                WHEN REGEXP_LIKE(segment_type,'^table','i') THEN 'table' 
                WHEN REGEXP_LIKE(segment_type,'^index','i') THEN 'index' 
                ELSE 'unknown' END segment_type, 
                CASE 
                WHEN REGEXP_LIKE(segment_type,'subpartition$','i') THEN 'subpart' 
                WHEN REGEXP_LIKE(segment_type,'partition$','i') THEN 'part' 
                ELSE 'normal' END part_type
           FROM dba_segments
          WHERE owner = upper( :p_owner )
            AND segment_name = upper( :p_segment )
            AND REGEXP_LIKE( NVL(segment_type,'~'), NVL( :p_segment_type, '.' ), 'i' )
            AND REGEXP_LIKE( NVL(partition_name,'~'), NVL( :p_partname, '.' ), 'i' )
       ) 
 GROUP BY segment_type,
       part_type 