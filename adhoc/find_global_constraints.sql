SET termout off
COLUMN disable_ddl format a130
COLUMN enable_ddl format a130

VAR p_table VARCHAR2(30)
VAR p_constraint_regexp VARCHAR2(30)
VAR p_constraint_type VARCHAR2(30)
VAR p_owner VARCHAR2(30)
VAR p_table VARCHAR2(30)
var l_tab_name VARCHAR2(61)
var p_basis VARCHAR2(10)
var p_maint_type VARCHAR2(10)

EXEC :p_owner := 'testdim';
EXEC :p_table := 'customer_dim';

SET termout on

SELECT *
  FROM all_constraints ac
  JOIN all_indexes ai
       ON nvl(ac.index_owner,ac.owner) = ai.owner
   AND ac.index_name = ai.index_name
 WHERE constraint_type IN ('U','P')
   AND ac.table_name = :p_table
   AND ac.owner = :p_owner
   AND partitioned='YES'