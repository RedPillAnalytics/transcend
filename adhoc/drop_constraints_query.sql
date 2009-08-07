SET termout off

COLUMN constraint_name format a30

VAR p_table VARCHAR2(30)
VAR p_tablespace VARCHAR2(30)
VAR p_source_table VARCHAR2(30)
VAR p_source_owner VARCHAR2(30)
VAR p_constraint_regexp VARCHAR2(30)
VAR p_constraint_type VARCHAR2(30)
VAR p_seg_attributes VARCHAR2 (3)
VAR l_targ_part VARCHAR2(3)
VAR p_owner VARCHAR2(30)
VAR p_table VARCHAR2(30)
VAR p_partname VARCHAR2(30)
VAR p_basis VARCHAR2(10)
VAR l_part_position NUMBER
var default_tablespace VARCHAR2(30)

EXEC :p_tablespace := NULL;
EXEC :p_constraint_regexp := NULL;
EXEC :p_owner := 'mi_scd';
EXEC :p_table := 'pex_location';
EXEC :p_constraint_type := NULL;
EXEC :p_basis := 'all';

EXEC dbms_metadata.set_transform_param( dbms_metadata.session_transform,'SEGMENT_ATTRIBUTES',CASE lower( :p_seg_attributes ) WHEN 'yes' THEN TRUE ELSE FALSE END );

SET termout on
SELECT  *
  FROM ( SELECT    'alter table '
                || owner
                || '.'
                || table_name
                || ' drop constraint '
                || constraint_name constraint_ddl,
                constraint_name, table_name,
                CASE
                WHEN REGEXP_LIKE( 'table|all', :p_basis, 'i' )
                THEN 'Y'
                ELSE 'N'
                END include, 'table' basis_source
           FROM all_constraints
          WHERE table_name = UPPER( :p_table )
            AND owner = UPPER( :p_owner )
            AND REGEXP_LIKE( constraint_name, NVL( :p_constraint_regexp, '.' ), 'i' )
            AND REGEXP_LIKE( constraint_type, NVL( :p_constraint_type, '.' ), 'i' )
          UNION
         SELECT    'alter table '
                || owner
                || '.'
                || table_name
                || ' drop constraint '
                || constraint_name constraint_ddl,
                constraint_name, table_name,
                CASE
                WHEN REGEXP_LIKE( 'reference|all', :p_basis, 'i' )
                THEN 'Y'
                ELSE 'N'
                END include, 'reference' basis_source
           FROM all_constraints
          WHERE constraint_type = 'R'
            AND REGEXP_LIKE( constraint_name, NVL( :p_constraint_regexp, '.' ), 'i' )
            AND r_owner IN (
                             SELECT owner
                               FROM all_constraints
                              WHERE table_name = UPPER( :p_table )
                                AND owner = UPPER( :p_owner )
                                AND constraint_type = 'P' )
            AND r_constraint_name IN(
                                      SELECT constraint_name
                                        FROM all_constraints
                                       WHERE table_name = UPPER( :p_table )
                                         AND owner = UPPER( :p_owner )
                                         AND constraint_type = 'P' ))
 WHERE include = 'Y'
 ORDER BY basis_source