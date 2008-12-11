SET termout off
COLUMN disable_ddl format a130
COLUMN enable_ddl format a130

var p_table VARCHAR2(30)
var p_constraint_regexp VARCHAR2(30)
var p_constraint_type VARCHAR2(30)
var p_owner VARCHAR2(30)
var p_table VARCHAR2(30)
var l_tab_name VARCHAR2(61)
var p_basis VARCHAR2(10)
var p_maint_type VARCHAR2(10)

EXEC :p_owner := 'stewart';
EXEC :p_table := 'products2';
EXEC :p_constraint_type := NULL;
EXEC :p_constraint_regexp := NULL;
EXEC :l_tab_name := upper ( :p_owner||'.'|| :p_table );
EXEC :p_basis := 'all';
EXEC :p_maint_type := 'enable';

SET termout on

SELECT 
       CASE maint_type
       WHEN 'validate' THEN validate_ddl
       WHEN 'disable' THEN disable_ddl
       WHEN 'disable validate' THEN disable_ddl
       WHEN 'enable' THEN enable_ddl
       ELSE NULL END ddl,
       CASE maint_type
       WHEN 'validate' THEN validate_msg
       WHEN 'disable' THEN disable_msg
       WHEN 'disable validate' THEN disable_msg
       WHEN 'enable' THEN enable_msg
       ELSE NULL END msg,
       ordering, basis_source, table_owner, table_name, constraint_name,
       disable_ddl, disable_msg, enable_ddl, enable_msg, validate_ddl, validate_msg, 
       basis_include, maint_type
  FROM ( SELECT 
                -- need to specify the kind of constraint maintenance that is to be performed
                CASE
                WHEN lower( p_maint_type ) = 'validate' AND status = 'DISABLED' AND validated = 'NOT VALIDATED'
                THEN 'validate'
                WHEN lower( p_maint_type ) = 'disable' AND status = 'DISABLED' AND validated = 'VALIDATED'
                THEN 'disable validate'
                WHEN lower( p_maint_type ) = 'disable' AND status = 'ENABLED'
                THEN 'disable'         
                WHEN lower( p_maint_type ) = 'enable' AND status = 'DISABLED'
                THEN 'enable'
                ELSE 'none'
                END maint_type,
                ordering, basis_source, table_owner, table_name, constraint_name,
                disable_ddl, disable_msg, enable_ddl, enable_msg, validate_ddl, validate_msg, 
                basis_include
           FROM ( SELECT
                         -- need this to get the order by clause right
                         -- WHEN we are disabling, we need references to go first
                         -- WHEN we are enabling, we need referenced (primary keys) to go first
                         CASE lower( p_maint_type )
                         WHEN 'enable'
                         THEN 1
                         ELSE 2
                         END ordering, 'table' basis_source, owner table_owner, table_name,
                         constraint_name, status, validated,
                         'alter table '
                         || l_tab_name
                         || ' modify constraint '
                         || constraint_name
                         || ' validate' validate_ddl,
                         'Constraint '
                         || constraint_name
                         || ' validated on '
                         || l_tab_name validate_msg,
                         'alter table '
                         || l_tab_name
                         || ' disable constraint '
                         || constraint_name disable_ddl,
                         'Constraint '
                         || constraint_name
                         || ' disabled on '
                         || l_tab_name disable_msg,
                         'alter table '
                         || l_tab_name
                         || ' enable constraint '
                         || constraint_name enable_ddl,
                         'Constraint ' || constraint_name || ' enabled on '
                         || l_tab_name enable_msg,
                         CASE
                         WHEN REGEXP_LIKE( 'table|all', p_basis, 'i' )
                         THEN 'Y'
                         ELSE 'N'
                         END basis_include
                    FROM all_constraints
                   WHERE table_name = upper( p_table )
                     AND owner = upper( p_owner )
                     AND REGEXP_LIKE( constraint_name, nvl( p_constraint_regexp, '.' ), 'i' )
                     AND REGEXP_LIKE( constraint_type, nvl( p_constraint_type, '.' ), 'i' )
                   UNION
                  SELECT
                         -- need this to get the order by clause right
                         -- WHEN we are disabling, we need references to go first
                         -- WHEN we are enabling, we need referenced (primary keys) to go first
                         CASE lower( p_maint_type )
                         WHEN 'enable'
                         THEN 2
                         ELSE 1
                         END ordering, 'reference' basis_source, owner table_owner, table_name,
                         constraint_name, status, validated,
                         'alter table '
                         || owner
                         || '.'
                         || table_name
                         || ' modify constraint '
                         || constraint_name
                         || ' validate' validate_ddl,
                         'Constraint '
                         || constraint_name
                         || ' validated on '
                         || owner
                         || '.'
                         || table_name validate_msg,
                         'alter table '
                         || owner
                         || '.'
                         || table_name
                         || ' disable constraint '
                         || constraint_name disable_ddl,
                         'Constraint '
                         || constraint_name
                         || ' disabled on '
                         || owner
                         || '.'
                         || table_name disable_msg,
                         'alter table '
                         || owner
                         || '.'
                         || table_name
                         || ' enable constraint '
                         || constraint_name enable_ddl,
                         'Constraint '
                         || constraint_name
                         || ' enabled on '
                         || owner
                         || '.'
                         || table_name enable_msg,
                         CASE
                         WHEN REGEXP_LIKE( 'reference|all', p_basis, 'i' )
                         THEN 'Y'
                         ELSE 'N'
                         END basis_include
                    FROM all_constraints
                   WHERE constraint_type = 'R'
                     AND REGEXP_LIKE( constraint_name, nvl( p_constraint_regexp, '.' ), 'i' )
                     AND r_constraint_name IN (
                                                SELECT constraint_name
                                                  FROM all_constraints
                                                 WHERE table_name = upper( p_table )
                                                   AND owner = upper( p_owner )
                                                   AND constraint_type = 'P' )
                     AND r_owner IN (
                                      SELECT owner
                                        FROM all_constraints
                                       WHERE table_name = upper( p_table )
                                         AND owner = upper( p_owner )
                                         AND constraint_type = 'P' )
                )
       )
 WHERE basis_include = 'Y'
   AND maint_type <> 'none'
 ORDER BY ordering