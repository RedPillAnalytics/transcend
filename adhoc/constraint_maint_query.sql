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

EXEC :p_owner := 'edw';
EXEC :p_table := 'customer_dim';
EXEC :p_constraint_type := NULL;
EXEC :p_constraint_regexp := NULL;
EXEC :l_tab_name := upper ( :p_owner||'.'|| :p_table );
EXEC :p_basis := 'reference';
EXEC :p_maint_type := 'disable';

SET termout on

SELECT  *
  FROM ( SELECT
                -- need this to get the order by clause right
                -- when we are disabling, we need references to go first
                -- when we are enabling, we need referenced (primary keys) to go first
                CASE LOWER( :p_maint_type )
                WHEN 'enable'
                THEN 1
                ELSE 2
                END ordering, 'table' basis_source, owner table_owner, table_name,
                constraint_name,
                'alter table '
                || :l_tab_name
                || ' disable constraint '
                || constraint_name disable_ddl,
                'Constraint '
                || constraint_name
                || ' disabled on '
                || :l_tab_name disable_msg,
                'alter table '
                || :l_tab_name
                || ' enable constraint '
                || constraint_name enable_ddl,
                'Constraint ' || constraint_name || ' enabled on '
                || :l_tab_name enable_msg,
                CASE
                WHEN REGEXP_LIKE( 'table|all', :p_basis, 'i' )
                THEN 'Y'
                ELSE 'N'
                END include
           FROM all_constraints
          WHERE table_name = UPPER( :p_table )
            AND owner = UPPER( :p_owner )
            AND status =
                CASE
                WHEN REGEXP_LIKE( 'disable', :p_maint_type, 'i' )
                THEN 'ENABLED'
                WHEN REGEXP_LIKE( 'enable', :p_maint_type, 'i' )
                THEN 'DISABLED'
                END
            AND REGEXP_LIKE( constraint_name, NVL( :p_constraint_regexp, '.' ), 'i' )
            AND REGEXP_LIKE( constraint_type, NVL( :p_constraint_type, '.' ), 'i' )
                UNION
         SELECT
                -- need this to get the order by clause right
                -- when we are disabling, we need references to go first
                -- when we are enabling, we need referenced (primary keys) to go first
                CASE LOWER( :p_maint_type )
                WHEN 'enable'
                THEN 2
                ELSE 1
                END ordering, 'reference' basis_source, owner table_owner, table_name,
                constraint_name,
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
                WHEN REGEXP_LIKE( 'reference|all', :p_basis, 'i' )
                THEN 'Y'
                ELSE 'N'
                END include
           FROM all_constraints
          WHERE constraint_type = 'R'
            AND status =
                CASE
                WHEN REGEXP_LIKE( 'disable', :p_maint_type, 'i' )
                THEN 'ENABLED'
                WHEN REGEXP_LIKE( 'enable', :p_maint_type, 'i' )
                THEN 'DISABLED'
                END
            AND REGEXP_LIKE( constraint_name, NVL( :p_constraint_regexp, '.' ), 'i' )
            AND r_constraint_name IN(
                                      SELECT constraint_name
                                        FROM all_constraints
                                       WHERE table_name = UPPER( :p_table )
                                         AND owner = UPPER( :p_owner )
                                         AND constraint_type = 'P' )
            AND r_owner IN(
                                      SELECT owner
                                        FROM all_constraints
                                       WHERE table_name = UPPER( :p_table )
                                         AND owner = UPPER( :p_owner )
                                         AND constraint_type = 'P' ))
 WHERE include = 'Y'
 ORDER BY ordering