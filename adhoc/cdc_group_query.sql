SET echo off
SET feedback off
SET timing off

var p_group_name VARCHAR2(30)

EXEC :p_group_name := 'salesforce';

SET feedback on
SET echo on
SET timing on

SELECT source_type,
       group_id,
       group_name,
       foundation,
       staging,
       subscription,
       sub_prefix,
       scn_column,
       row_column,
       scnmin_column,
       scnmax_column
  FROM cdc_source
  JOIN cdc_group
       USING (source_id)
  JOIN cdc_subscription
       USING (group_id)
 WHERE lower( group_name ) = lower( :p_group_name )
