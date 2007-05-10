CREATE OR REPLACE PACKAGE BODY tdinc.dbflex
AS
-- this is not an autonomous transaction because COREUTILS.exec_auto is
   PROCEDURE trunc_tab (p_owner IN VARCHAR2, p_table IN VARCHAR2, p_runmode VARCHAR2 DEFAULT NULL)
   AS
      o_app   applog := applog (p_runmode => p_runmode, p_module => 'trunc_tab');
   BEGIN
      coreutils.exec_auto ('truncate table ' || p_owner || '.' || p_table, p_runmode => o_app.runmode);
      o_app.clear_app_info;
   END trunc_tab;

   -- builds the indexes from one table on another
   -- if both the source and target are partitioned tables, then the index DDL is left alone
   -- if the source is partitioned and the target is not, then all local indexes are created as non-local
   -- if P_TABLESPACE is provided, then that tablespace name is used, regardless of the DDL that is pulled
   PROCEDURE build_indexes (
      p_source_owner   VARCHAR2,
      p_source_table   VARCHAR2,
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_index_regexp   VARCHAR2 DEFAULT NULL,
      p_index_type     VARCHAR2 DEFAULT NULL,
      p_part_type      VARCHAR2 DEFAULT NULL,
      p_tablespace     VARCHAR2 DEFAULT NULL,
      p_runmode        VARCHAR2 DEFAULT NULL)
   IS
      l_ddl            LONG;
      l_e_ddl          LONG;
      l_idx_cnt        NUMBER                        := 0;
      l_tab_name       VARCHAR2 (61)                 := p_owner || '.' || p_table;
      l_src_name       VARCHAR2 (61)                 := p_source_owner || '.' || p_source_table;
      l_part_type      VARCHAR2 (6);
      l_targ_part      dba_tables.partitioned%TYPE;
      l_rows           BOOLEAN                       := FALSE;
      e_dup_idx_name   EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_dup_idx_name, -955);
      e_dup_col_list   EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_dup_col_list, -1408);
      o_app            applog
                             := applog (p_module       => 'build_indexes',
                                        p_runmode      => p_runmode);
   BEGIN
      -- execute immediate doesn't like ";" on the end
      DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'SQLTERMINATOR', FALSE);
      -- we need the segment attributes so things go where we want them to
      DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform,
                                         'SEGMENT_ATTRIBUTES',
                                         TRUE);
      -- don't want all the other storage aspects though
      DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'STORAGE', FALSE);
      o_app.set_action ('Build indexes');

      -- find out if the target table is partitioned so we know how to formulate the index ddl
      SELECT partitioned
        INTO l_targ_part
        FROM dba_tables
       WHERE table_name = UPPER (p_table) AND owner = UPPER (p_owner);

      o_app.log_msg ('Building indexes on ' || l_tab_name);

      -- create a cursor containing the DDL from the target indexes
      FOR c_indexes IN
         (SELECT
                    -- if idx_rename already exists (constructed below), then we will try to rename the index to something generic
                    -- this name will only be used when an exception is raised
                    -- this index is shown in debug mode
                    p_table
                 || '_'
                 || idx_e_ext
                 -- rank function gives us the index number by specific index extension (formulated below)
                 || RANK () OVER (PARTITION BY idx_e_ext ORDER BY index_name) idx_e_rename,
                 index_ddl,
                 table_owner,
                 table_name,
                 owner,
                 index_name,
                 idx_rename,
                 partitioned,
                 uniqueness,
                 idx_e_ext,
                 index_type
            FROM (SELECT    REGEXP_REPLACE
                               
                               -- dbms_metadata pulls the metadata for the source object out of the dictionary
                            (   DBMS_METADATA.get_ddl ('INDEX', index_name, owner),
                                CASE
                                   -- target is not partitioned and no tablespace provided
                                WHEN l_targ_part = 'NO' AND p_tablespace IS NULL
                                      -- remove all partitioning and the local keyword
                                THEN '(\(\s*partition.+\))|local'
                                   -- target is not partitioned but tablespace is provided
                                WHEN l_targ_part = 'NO' AND p_tablespace IS NOT NULL
                                      -- strip out partitioned info and local keyword and tablespace clause
                                THEN '(\(\s*partition.+\))|local|(tablespace)\s*[^ ]+'
                                   -- target is partitioned and tablespace is provided
                                WHEN l_targ_part = 'YES' AND p_tablespace IS NOT NULL
                                      -- strip out partitioned info keeping local keyword and remove tablespace clause
                                THEN '(\(\s*partition.+\))|(tablespace)\s*[^ ]+'
                                   ELSE NULL
                                END,
                                NULL,
                                1,
                                0,
                                'in')
                         || CASE
                               -- if tablespace is provided, tack it on the end
                            WHEN p_tablespace IS NOT NULL
                                  THEN ' TABLESPACE ' || p_tablespace
                               ELSE NULL
                            END index_ddl,
                         table_owner,
                         table_name,
                         owner,
                         index_name,
                         -- this is the index name that will be used in the first attempt
                         -- this index name is shown in debug mode
                         REGEXP_REPLACE (index_name, p_source_table, p_table, 1, 0, 'i') idx_rename,
                         CASE
                            -- devise generic index extensions for the different types
                         WHEN index_type = 'BITMAP'
                               THEN 'BMI'
                            WHEN REGEXP_LIKE (index_type, '^function', 'i')
                               THEN 'FNC'
                            WHEN uniqueness = 'UNIQUE'
                               THEN 'UK'
                            ELSE 'IK'
                         END idx_e_ext,
                         partitioned,
                         uniqueness,
                         index_type
                    FROM all_indexes
                   -- use a CASE'd regular expression to determine whether to include global indexes
                  WHERE  REGEXP_LIKE (partitioned,
                                      CASE
                                         WHEN REGEXP_LIKE ('global', p_part_type, 'i')
                                            THEN 'NO'
                                         WHEN REGEXP_LIKE ('local', p_part_type, 'i')
                                            THEN 'YES'
                                         ELSE '.'
                                      END,
                                      'i')
                     AND table_name = UPPER (p_source_table)
                     AND table_owner = UPPER (p_source_owner)
                     -- use an NVL'd regular expression to determine specific indexes to work on
                     AND REGEXP_LIKE (index_name, NVL (p_index_regexp, '.'), 'i')
                     -- use an NVL'd regular expression to determine the index types to worked on
                     AND REGEXP_LIKE (index_type, '^' || NVL (p_index_type, '.'), 'i')))
      LOOP
         o_app.log_msg ('Source index: ' || c_indexes.index_name, 4);
         o_app.log_msg ('Renamed index: ' || c_indexes.idx_rename, 4);
         o_app.log_msg ('Exception renamed index: ' || c_indexes.idx_e_rename, 4);
         l_rows := TRUE;
         o_app.set_action ('Format index DDL');
         -- replace the source table name with the target table name
         -- if a " is found, then use it... otherwise don't
         l_ddl :=
            REGEXP_REPLACE (c_indexes.index_ddl,
                            '(\."?)(' || p_source_table || ')(")?',
                            '\1' || p_table || '\3');
              -- replace the index owner with the target owner
         -- if a " is found, then use it... otherwise don't
         l_ddl :=
            REGEXP_REPLACE (l_ddl,
                            '(")?(' || c_indexes.owner || ')("?\.)',
                            '\1' || p_owner || '\3',
                            1,
                            0,
                            'i');
         -- though it's not necessary for EXECUTE IMMEDIATE, remove blank lines for looks
         l_ddl := REGEXP_REPLACE (l_ddl, CHR (10) || '[[:space:]]+' || CHR (10), NULL);
         -- and for looks, remove the last carriage return
         l_ddl := REGEXP_REPLACE (l_ddl, '[[:space:]]*$', NULL);
         o_app.set_action ('Execute index DDL');
         l_e_ddl :=
            REGEXP_REPLACE (l_ddl,
                            '(\."?)(\w)+(")?( on)',
                            '\1' || c_indexes.idx_e_rename || '\3 \4',
                            1,
                            0,
                            'i');
         o_app.log_msg ('Renamed DDL for exceptions: ' || l_e_ddl, 4);

         BEGIN
            coreutils.exec_auto (l_ddl, p_runmode => o_app.runmode);
            o_app.log_msg ('Index ' || c_indexes.idx_rename || ' built');
            l_idx_cnt := l_idx_cnt + 1;
         EXCEPTION
            -- if this index_name already exists, try to rename it to something else
            WHEN e_dup_idx_name
            THEN
	    o_app.log_msg('New index name being used because index name already exists',3);
               BEGIN
                  coreutils.exec_auto (l_e_ddl);
                  o_app.log_msg ('Index ' || c_indexes.idx_e_rename || ' built');
                  l_idx_cnt := l_idx_cnt + 1;
               EXCEPTION
                  -- now the name is different, but check to see if the columns are already indexed
                  WHEN e_dup_col_list
                  THEN
                     o_app.log_msg (   'Index comparable to '
                                    || c_indexes.index_name
                                    || ' already exists');
               END;
         END;
      END LOOP;

      IF NOT l_rows
      THEN
         o_app.log_msg ('No matching indexes found on ' || l_src_name);
      ELSE
         o_app.log_msg (   l_idx_cnt
                        || ' index'
                        || CASE
                              WHEN l_idx_cnt = 1
                                 THEN NULL
                              ELSE 'es'
                           END
                        || ' built on '
                        || l_tab_name);
      END IF;

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END build_indexes;

   -- builds the constraints from one table on another
   PROCEDURE build_constraints (
      p_source_owner        VARCHAR2,
      p_source_table        VARCHAR2,
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_seg_attributes      VARCHAR2 DEFAULT 'no',
      p_tablespace          VARCHAR2 DEFAULT NULL,
      p_runmode             VARCHAR2 DEFAULT NULL)
   IS
      l_targ_part      dba_tables.partitioned%TYPE;
      l_ddl            LONG;
      l_con_cnt        NUMBER                        := 0;
      l_tab_name       VARCHAR2 (61)                 := p_owner || '.' || p_table;
      l_src_name       VARCHAR2 (61)                 := p_source_owner || '.' || p_source_table;
      l_rows           BOOLEAN                       := FALSE;
      e_dup_con_name   EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_dup_con_name, -2264);
      e_dup_not_null   EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_dup_not_null, -1442);
      e_dup_pk         EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_dup_pk, -2260);
      o_app            applog
                         := applog (p_module       => 'build_constraints',
                                    p_runmode      => p_runmode);
   BEGIN
      -- execute immediate doesn't like ";" on the end
      DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'SQLTERMINATOR', FALSE);
      -- determine whether information about segments is included
      -- for unique and primary key constraints, these are linked to an index
      -- with segment_attributes true, the USING INDEX and other information will be included
      DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform,
                                         'SEGMENT_ATTRIBUTES',
                                         CASE LOWER (p_seg_attributes)
                                            WHEN 'yes'
                                               THEN TRUE
                                            ELSE FALSE
                                         END);

      -- need this to determine how to build constraints associated with indexes on target table
      SELECT partitioned
        INTO l_targ_part
        FROM dba_tables
       WHERE table_name = UPPER (p_table) AND owner = UPPER (p_owner);

      o_app.set_action ('Build constraints');
      o_app.log_msg ('Adding constraints on ' || l_tab_name);

      FOR c_constraints IN
         (SELECT
                    -- this is the constraint name used if CON_RENAME (formulated below) already exists
                    -- this is only used in the case of an exception
                    -- this can be seen in debug mode
                    p_table
                 || '_'
                 || con_e_ext
                 -- the rank function gives us a unique number to use for each index with a specific extension
                 -- gives us something like UK1 or UK2
                 || RANK () OVER (PARTITION BY con_e_ext ORDER BY constraint_name) con_e_rename,
                 constraint_ddl,
                 owner,
                 constraint_name,
                 con_rename,
                 constraint_type,
                 index_owner,
                 index_name
            FROM (SELECT REGEXP_REPLACE
                            (REGEXP_REPLACE
                                
                                -- different DBMS_METADATA function is used for referential integrity constraints
                             (   DBMS_METADATA.get_ddl (CASE constraint_type
                                                           WHEN 'R'
                                                              THEN 'REF_CONSTRAINT'
                                                           ELSE 'CONSTRAINT'
                                                        END,
                                                        constraint_name,
                                                        owner),
                                 CASE
                                    -- target is not partitioned and no tablespace provided
                                 WHEN l_targ_part = 'NO' AND p_tablespace IS NULL
                                       -- remove all partitioning and the local keyword
                                 THEN '(\(\s*partition.+\))|local'
                                    -- target is not partitioned but tablespace is provided
                                 WHEN l_targ_part = 'NO' AND p_tablespace IS NOT NULL
                                       -- strip out partitioned, local keyword and tablespace clause
                                 THEN '(\(\s*partition.+\))|local|(tablespace)\s*[^ ]+'
                                    -- target is partitioned and tablespace is provided
                                 WHEN l_targ_part = 'YES' AND p_tablespace IS NOT NULL
                                       -- strip out partitioning, keep local keyword and remove tablespace clause
                                 THEN '(\(\s*partition.+\))|(tablespace)\s*[^ ]+'
                                    ELSE NULL
                                 END,
                                 NULL,
                                 1,
                                 0,
                                 'in'),
                             
                             -- TABLESPACE clause cannot come after the ENABLE|DISABLE keyword, so I need to place it before
                             '(\s+)(enable|disable)(\s*)$',
                             CASE
                                -- if tablespace is provided, tack it on the end
                             WHEN coreutils.get_yn_ind (p_seg_attributes) = 'yes'
                             AND p_tablespace IS NOT NULL
                             AND constraint_type IN ('P', 'U')
                                   THEN '\1TABLESPACE ' || p_tablespace || '\1\2'
                                ELSE '\1\2\3'
                             END,
                             1,
                             0,
                             'i') constraint_ddl,
                         owner,
                         constraint_name,
                         constraint_type,
                         index_owner,
                         index_name,
                         -- this is the constraint name used with the first attempt
                         -- this can be seen in debug mode
                         REGEXP_REPLACE (constraint_name, p_source_table, p_table, 1, 0, 'i')
                                                                                         con_rename,
                         CASE constraint_type
                            -- devise a specific constraint extention based on information about it
                         WHEN 'R'
                               THEN 'F'
                            ELSE constraint_type || 'K'
                         END con_e_ext
                    FROM dba_constraints
                   WHERE table_name = UPPER (p_source_table)
                     AND owner = UPPER (p_source_owner)
                     AND REGEXP_LIKE (constraint_name, NVL (p_constraint_regexp, '.'), 'i')
                     AND REGEXP_LIKE (constraint_type, NVL (p_constraint_type, '.'), 'i')))
      LOOP
         -- catch empty cursor sets
         l_rows := TRUE;
         o_app.log_msg ('Renamed constraint: ' || c_constraints.con_rename, 4);
         o_app.log_msg ('Exception renamed constraint: ' || c_constraints.con_e_rename, 4);
         o_app.set_action ('Format constraint DDL');
         l_ddl :=
            REGEXP_REPLACE (c_constraints.constraint_ddl,
                            '\.' || p_source_table,
                            '.' || p_table,
                            1,
                            0,
                            'i');
              -- replace the source table name with the target table name
              -- if a " is found, then use it... otherwise don't
         -- replace table name inside the constraint name as well
         l_ddl :=
            REGEXP_REPLACE (c_constraints.constraint_ddl,
                            '(\."?|constraint "?)(' || p_source_table || ')(")?',
                            '\1' || p_table || '\3',
                            1,
                            0,
                            'i');
         -- replace the source owner with the target owner
         -- if a " is found, then use it... otherwise don't
         l_ddl :=
            REGEXP_REPLACE (l_ddl,
                            '(")?(' || p_source_owner || ')("?\.)',
                            '\1' || p_owner || '\3',
                            1,
                            0,
                            'i');
         -- though it's not necessary for EXECUTE IMMEDIATE, remove blank lines for looks
         l_ddl := REGEXP_REPLACE (l_ddl, CHR (10) || '[[:space:]]+' || CHR (10), NULL);
         -- and for looks, remove the last carriage return
         l_ddl := REGEXP_REPLACE (l_ddl, '[[:space:]]*$', NULL);
         o_app.set_action ('Execute constraint DDL');

         BEGIN
            coreutils.exec_auto (l_ddl, p_runmode => o_app.runmode);
            o_app.log_msg ('Constraint ' || c_constraints.con_rename || ' built');
            l_con_cnt := l_con_cnt + 1;
         EXCEPTION
            WHEN e_dup_pk
            THEN
               o_app.log_msg ('Primary key constraint already exists on table ' || l_tab_name);
            WHEN e_dup_not_null
            THEN
               o_app.log_msg ('Referenced not null constraint already exists on table '
                              || l_tab_name);
            WHEN e_dup_con_name
            THEN
               coreutils.exec_auto (REGEXP_REPLACE (l_ddl,
                                                    '(constraint "?)(\w+)("?)',
                                                    '\1' || c_constraints.con_e_rename || '\3 \4',
                                                    1,
                                                    0,
                                                    'i'));
               o_app.log_msg ('Constraint ' || c_constraints.con_e_rename || ' built');
               l_con_cnt := l_con_cnt + 1;
         END;
      END LOOP;

      IF NOT l_rows
      THEN
         o_app.log_msg ('No matching constraints found on ' || l_src_name);
      ELSE
         o_app.log_msg (   l_con_cnt
                        || ' constraint'
                        || CASE
                              WHEN l_con_cnt = 1
                                 THEN NULL
                              ELSE 's'
                           END
                        || ' built on '
                        || l_tab_name);
      END IF;

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END build_constraints;

   -- drop particular indexes from a table
   PROCEDURE drop_indexes (
      p_owner          VARCHAR2,
      p_table          VARCHAR2,
      p_index_type     VARCHAR2 DEFAULT NULL,
      p_index_regexp   VARCHAR2 DEFAULT NULL,
      p_runmode        VARCHAR2 DEFAULT NULL)
   IS
      l_rows       BOOLEAN       := FALSE;
      l_tab_name   VARCHAR2 (61) := p_owner || '.' || p_table;
      l_idx_cnt    NUMBER        := 0;
      o_app        applog     := applog (p_module       => 'drop_indexes',
                                         p_runmode      => p_runmode);
   BEGIN
      o_app.log_msg ('Dropping indexes on ' || l_tab_name);

      FOR c_indexes IN (SELECT 'drop index ' || owner || '.' || index_name index_ddl,
                               index_name,
                               table_name,
                               owner,
                               owner || '.' || index_name full_index_name
                          FROM dba_indexes
                         WHERE table_name = UPPER (p_table)
                           AND table_owner = UPPER (p_owner)
                           AND REGEXP_LIKE (index_name, NVL (p_index_regexp, '.'), 'i')
                           AND REGEXP_LIKE (index_type, '^' || NVL (p_index_type, '.'), 'i'))
      LOOP
         l_rows := TRUE;
         coreutils.exec_auto (c_indexes.index_ddl, o_app.runmode);
         l_idx_cnt := l_idx_cnt + 1;
         o_app.log_msg ('Index ' || c_indexes.index_name || ' dropped');
      END LOOP;

      IF NOT l_rows
      THEN
         o_app.log_msg ('No matching indexes found on ' || l_tab_name);
      ELSE
         o_app.log_msg (   l_idx_cnt
                        || ' index'
                        || CASE
                              WHEN l_idx_cnt = 1
                                 THEN NULL
                              ELSE 'es'
                           END
                        || ' dropped on '
                        || l_tab_name);
      END IF;

      o_app.clear_app_info;
   END drop_indexes;

   -- drop particular constraints from a table
   PROCEDURE drop_constraints (
      p_owner               VARCHAR2,
      p_table               VARCHAR2,
      p_constraint_type     VARCHAR2 DEFAULT NULL,
      p_constraint_regexp   VARCHAR2 DEFAULT NULL,
      p_runmode             VARCHAR2 DEFAULT NULL)
   IS
      l_con_cnt    NUMBER        := 0;
      l_tab_name   VARCHAR2 (61) := p_owner || '.' || p_table;
      l_rows       BOOLEAN       := FALSE;
      o_app        applog := applog (p_module       => 'drop_constraints',
                                     p_runmode      => p_runmode);
   BEGIN
      -- drop constraints
      FOR c_constraints IN (SELECT    'alter table '
                                   || owner
                                   || '.'
                                   || table_name
                                   || ' drop constraint '
                                   || constraint_name constraint_ddl,
                                   constraint_name,
                                   table_name
                              FROM dba_constraints
                             WHERE table_name = UPPER (p_table)
                               AND owner = UPPER (p_owner)
                               AND REGEXP_LIKE (constraint_name, NVL (p_constraint_regexp, '.'),
                                                'i')
                               AND REGEXP_LIKE (constraint_type, NVL (p_constraint_type, '.'), 'i'))
      LOOP
         -- catch empty cursor sets
         l_rows := TRUE;
         coreutils.exec_auto (c_constraints.constraint_ddl, o_app.runmode);
         l_con_cnt := l_con_cnt + 1;
         o_app.log_msg ('Constraint ' || c_constraints.constraint_name || ' dropped');
      END LOOP;

      IF NOT l_rows
      THEN
         o_app.log_msg ('No matching constraints found on ' || l_tab_name);
      ELSE
         o_app.log_msg (   l_con_cnt
                        || ' constraint'
                        || CASE
                              WHEN l_con_cnt = 1
                                 THEN NULL
                              ELSE 's'
                           END
                        || ' dropped on '
                        || l_tab_name);
      END IF;

      o_app.clear_app_info;
   END drop_constraints;

   -- structures an insert or insert append statement from the source to the target provided
   PROCEDURE insert_table (
      p_source_owner    VARCHAR2,
      p_source_object   VARCHAR2,
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_trunc           VARCHAR2 DEFAULT 'no',
      p_direct          VARCHAR2 DEFAULT 'yes',
      p_log_table       VARCHAR2 DEFAULT NULL,
      p_reject_limit    VARCHAR2 DEFAULT 'unlimited',
      p_runmode         VARCHAR2 DEFAULT NULL)
   IS
      l_src_name   VARCHAR2 (61) := p_source_owner || '.' || p_source_object;
      l_trg_name   VARCHAR2 (61) := p_owner || '.' || p_table;
      o_app        applog
         := applog (p_module       => 'insert_table',
                    p_runmode      => p_runmode,
                    p_action       => 'Check existence of objects');
   BEGIN
      CASE
         WHEN NOT coreutils.object_exists (p_source_owner, p_source_object)
         THEN
            raise_application_error (coreutils.get_err_cd ('no_object'),
                                     coreutils.get_err_msg ('no_object') || ' : ' || l_src_name);
         WHEN NOT coreutils.table_exists (p_owner, p_table)
         THEN
            raise_application_error (coreutils.get_err_cd ('no_object'),
                                     coreutils.get_err_msg ('no_object') || ' : ' || l_trg_name);
         ELSE
            NULL;
      END CASE;

      -- warning concerning using LOG ERRORS clause and the APPEND hint
      IF coreutils.is_true (p_direct) AND p_log_table IS NOT NULL
      THEN
         o_app.log_msg
            ('Unique constraints can still be violated when using P_LOG_TABLE in conjunction with P_DIRECT mode',
             4);
      END IF;

      IF coreutils.is_true (p_trunc)
      THEN
         -- truncate the target table
         trunc_tab (p_owner, p_table, o_app.runmode);
      END IF;

      -- enable|disable parallel dml depending on the parameter for P_DIRECT
      coreutils.exec_sql (   'ALTER SESSION '
                          || CASE
                                WHEN REGEXP_LIKE ('yes', p_direct, 'i')
                                   THEN 'ENABLE'
                                ELSE 'DISABLE'
                             END
                          || ' PARALLEL DML',
                          p_runmode      => o_app.runmode);
      o_app.log_msg ('Inserting records from ' || l_src_name || ' into ' || l_trg_name, 3);
      coreutils.exec_sql (   REGEXP_REPLACE (   'insert /*+ APPEND */ into '
                                             || l_trg_name
                                             || ' select * from '
                                             || l_src_name,
                                             CASE
                                                -- just use a regular expression to remove the APPEND hint if P_DIRECT is disabled
                                             WHEN REGEXP_LIKE ('no', p_direct, 'i')
                                                   THEN '/\*\+ APPEND \*/ '
                                                ELSE NULL
                                             END)
                          -- if a logging table is specified, then just append it on the end
                          || CASE NVL (p_log_table, 'N/A')
                                WHEN 'N/A'
                                   THEN NULL
                                ELSE    ' log errors into '
                                     || p_log_table
                                     || ' reject limit '
                                     || p_reject_limit
                             END,
                          o_app.runmode);
      -- record the number of rows affected
      o_app.log_cnt_msg (SQL%ROWCOUNT);
      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END insert_table;

   -- structures a merge statement between two tables that have the same table
   PROCEDURE merge_table (
      p_source_owner    VARCHAR2,
      p_source_object   VARCHAR2,
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_columns         VARCHAR2 DEFAULT NULL,
      p_direct          VARCHAR2 DEFAULT 'yes',
      p_log_table       VARCHAR2 DEFAULT NULL,
      p_reject_limit    VARCHAR2 DEFAULT 'unlimited',
      p_runmode         VARCHAR2 DEFAULT 'no')
   IS
      l_onclause        VARCHAR2 (32000);
      l_update          VARCHAR2 (32000);
      l_insert          VARCHAR2 (32000);
      l_values          VARCHAR2 (32000);
      l_src_name        VARCHAR2 (61)    := p_source_owner || '.' || p_source_object;
      l_trg_name        VARCHAR2 (61)    := p_owner || '.' || p_table;
      e_no_on_columns   EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_no_on_columns, -936);
      o_app             applog
         := applog (p_module       => 'merge_table',
                    p_runmode      => p_runmode,
                    p_action       => 'Check existence of objects');
   BEGIN
      CASE
         WHEN NOT coreutils.object_exists (p_source_owner, p_source_object)
         THEN
            raise_application_error (coreutils.get_err_cd ('no_object'),
                                     coreutils.get_err_msg ('no_object') || ' : ' || l_src_name);
         WHEN NOT coreutils.table_exists (p_owner, p_table)
         THEN
            raise_application_error (coreutils.get_err_cd ('no_object'),
                                     coreutils.get_err_msg ('no_object') || ' : ' || l_trg_name);
         ELSE
            NULL;
      END CASE;

      -- warning concerning using LOG ERRORS clause and the APPEND hint
      IF REGEXP_LIKE ('yes', p_direct, 'i') AND p_log_table IS NOT NULL
      THEN
         o_app.log_msg
            ('Unique constraints can still be violated when using P_LOG_TABLE in conjunction with P_DIRECT mode',
             4);
      END IF;

      o_app.set_action ('Construct MERGE ON clause');

      -- use the columns provided in P_COLUMNS.
      -- if that is left null, then choose the columns in the primary key of the target table
      -- if there is no primary key, then choose a unique key (any unique key)
      IF p_columns IS NOT NULL
      THEN
         WITH DATA AS
              
              -- this allows us to create a variable IN LIST based on multiple column names provided
              (SELECT     TRIM (SUBSTR (COLUMNS,
                                        INSTR (COLUMNS, ',', 1, LEVEL) + 1,
                                          INSTR (COLUMNS, ',', 1, LEVEL + 1)
                                        - INSTR (COLUMNS, ',', 1, LEVEL)
                                        - 1)) AS token
                     FROM (SELECT ',' || p_columns || ',' COLUMNS
                             FROM DUAL)
               CONNECT BY LEVEL <= LENGTH (p_columns) - LENGTH (REPLACE (p_columns, ',', '')) + 1)
         SELECT REGEXP_REPLACE (   '('
                                || stragg ('target.' || column_name || ' = source.' || column_name)
                                || ')',
                                ',',
                                ' AND' || CHR (10)) LIST
           INTO l_onclause
           FROM dba_tab_columns
          WHERE table_name = UPPER (p_table)
            AND owner = UPPER (p_owner)
            -- select from the variable IN LIST
            AND column_name IN (SELECT *
                                  FROM DATA);
      ELSE
         -- otherwise, we need to get a constraint name
         -- we first choose a PK if it exists
         -- otherwise get a UK at random
         SELECT LIST
           INTO l_onclause
           FROM (SELECT REGEXP_REPLACE (   '('
                                        || stragg (   'target.'
                                                   || column_name
                                                   || ' = source.'
                                                   || column_name)
                                        || ')',
                                        ',',
                                        ' AND' || CHR (10)) LIST,
                        -- the MIN function will ensure that primary keys are selected first
                        -- otherwise, it will randonmly choose a remaining constraint to use
                        MIN (dc.constraint_type) con_type
                   FROM all_cons_columns dcc JOIN all_constraints dc USING (constraint_name,
                                                                            table_name)
                  WHERE table_name = UPPER (p_table)
                    AND dcc.owner = UPPER (p_owner)
                    AND dc.constraint_type IN ('P', 'U'));
      END IF;

      o_app.set_action ('Construct MERGE update clause');

      IF p_columns IS NOT NULL
      THEN
         SELECT REGEXP_REPLACE (stragg ('target.' || column_name || ' = source.' || column_name),
                                ',',
                                ',' || CHR (10))
           INTO l_update
           -- if P_COLUMNS is provided, we use the same logic from the ON clause
           -- to make sure those same columns are not inlcuded in the update clause
           -- MINUS gives us that
         FROM   (WITH DATA AS
                      (SELECT     TRIM (SUBSTR (COLUMNS,
                                                INSTR (COLUMNS, ',', 1, LEVEL) + 1,
                                                  INSTR (COLUMNS, ',', 1, LEVEL + 1)
                                                - INSTR (COLUMNS, ',', 1, LEVEL)
                                                - 1)) AS token
                             FROM (SELECT ',' || p_columns || ',' COLUMNS
                                     FROM DUAL)
                       CONNECT BY LEVEL <=
                                        LENGTH (p_columns) - LENGTH (REPLACE (p_columns, ',', ''))
                                        + 1)
                 SELECT column_name
                   FROM all_tab_columns
                  WHERE table_name = UPPER (p_table) AND owner = UPPER (p_owner)
                 MINUS
                 SELECT column_name
                   FROM dba_tab_columns
                  WHERE table_name = UPPER (p_table)
                    AND owner = UPPER (p_owner)
                    AND column_name IN (SELECT *
                                          FROM DATA));
      ELSE
         -- otherwise, we once again MIN a constraint type to ensure it's the same constraint
         -- then, we just minus the column names so they aren't included
         SELECT REGEXP_REPLACE (stragg ('target.' || column_name || ' = source.' || column_name),
                                ',',
                                ',' || CHR (10))
           INTO l_update
           FROM (SELECT column_name
                   FROM all_tab_columns
                  WHERE table_name = UPPER (p_table) AND owner = UPPER (p_owner)
                 MINUS
                 SELECT column_name
                   FROM (SELECT   column_name,
                                  MIN (dc.constraint_type) con_type
                             FROM all_cons_columns dcc JOIN all_constraints dc
                                  USING (constraint_name, table_name)
                            WHERE table_name = UPPER (p_table)
                              AND dcc.owner = UPPER (p_owner)
                              AND dc.constraint_type IN ('P', 'U')
                         GROUP BY column_name));
      END IF;

      o_app.set_action ('Construnct MERGE insert clause');

      SELECT   REGEXP_REPLACE ('(' || stragg ('target.' || column_name) || ') ', ',',
                               ',' || CHR (10)) LIST
          INTO l_insert
          FROM all_tab_columns
         WHERE table_name = UPPER (p_table) AND owner = UPPER (p_owner)
      ORDER BY column_name;

      o_app.set_action ('Construct MERGE values clause');
      l_values := REGEXP_REPLACE (l_insert, 'target.', 'source.');
      o_app.log_msg (   'Merging records from '
                     || p_source_owner
                     || '.'
                     || p_source_object
                     || ' into '
                     || p_owner
                     || '.'
                     || p_table);

      BEGIN
         o_app.set_action ('Issue MERGE statement');
         -- ENABLE|DISABLE parallel dml depending on the value of P_DIRECT
         coreutils.exec_sql (   'ALTER SESSION '
                             || CASE
                                   WHEN REGEXP_LIKE ('yes', p_direct, 'i')
                                      THEN 'ENABLE'
                                   ELSE 'DISABLE'
                                END
                             || ' PARALLEL DML',
                             p_runmode      => o_app.runmode);
         o_app.log_msg ('Merging records from ' || l_src_name || ' into ' || l_trg_name, 3);
         -- we put the merge statement together using all the different clauses constructed above
         coreutils.exec_sql (   REGEXP_REPLACE (   'MERGE INTO '
                                                || p_owner
                                                || '.'
                                                || p_table
                                                || ' target using '
                                                || CHR (10)
                                                || '(select * from '
                                                || p_source_owner
                                                || '.'
                                                || p_source_object
                                                || ') source on '
                                                || CHR (10)
                                                || l_onclause
                                                || CHR (10)
                                                || ' WHEN MATCHED THEN UPDATE SET '
                                                || CHR (10)
                                                || l_update
                                                || CHR (10)
                                                || ' WHEN NOT MATCHED THEN INSERT /*+ APPEND */ '
                                                || CHR (10)
                                                || l_insert
                                                || CHR (10)
                                                || ' VALUES '
                                                || CHR (10)
                                                || l_values,
                                                -- just strip the APPEND hint out if P_DIRECT is 'no'
                                                CASE
                                                   WHEN REGEXP_LIKE ('no', p_direct, 'i')
                                                      THEN '/\*\+ APPEND \*/ '
                                                   ELSE NULL
                                                END)
                             -- if we specify a logging table, append that on the end
                             || CASE p_log_table
                                   WHEN NULL
                                      THEN NULL
                                   ELSE    ' log errors into '
                                        || p_log_table
                                        || ' reject limit '
                                        -- if no reject limit is specified, then use unlimited
                                        || p_reject_limit
                                END,
                             o_app.runmode);
      EXCEPTION
         -- ON columns not specified correctly
         WHEN e_no_on_columns
         THEN
            raise_application_error (coreutils.get_err_cd ('on_clause_missing'),
                                     coreutils.get_err_msg ('on_clause_missing'));
      END;

      -- show the records merged
      o_app.log_cnt_msg (SQL%ROWCOUNT);
      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END merge_table;

   -- queries the dictionary based on regular expressions and loads tables using either the load_tab method or the merge_tab method
   PROCEDURE load_tables (
      p_source_owner    VARCHAR2,
      p_source_regexp   VARCHAR2,
      p_owner           VARCHAR2 DEFAULT NULL,
      p_suffix          VARCHAR2 DEFAULT NULL,
      p_merge           VARCHAR2 DEFAULT 'no',
      p_part_tabs       VARCHAR2 DEFAULT 'yes',
      p_trunc           VARCHAR2 DEFAULT 'no',
      p_direct          VARCHAR2 DEFAULT 'yes',
      p_commit          VARCHAR2 DEFAULT 'yes',
      p_runmode         VARCHAR2 DEFAULT NULL)
   IS
      l_rows   BOOLEAN := FALSE;
      o_app    applog  := applog (p_module => 'load_tables', p_runmode => p_runmode);
   BEGIN
      o_app.log_msg ('Loading matching objects from the ' || p_source_owner || ' schema.');

      -- dynamic cursor contains source and target objects
      FOR c_objects IN (SELECT o.owner src_owner,
                               object_name src,
                               t.owner targ_owner,
                               table_name targ
                          FROM all_objects o JOIN all_tables t
                               ON (REGEXP_REPLACE (object_name, '([^_]+)(_)([^_]+)$', '\1') =
                                      REGEXP_REPLACE (table_name,
                                                      CASE
                                                         WHEN p_suffix IS NULL
                                                            THEN '?'
                                                         ELSE '_' || p_suffix || '$'
                                                      END,
                                                      NULL))
                         WHERE REGEXP_LIKE (object_name, p_source_regexp, 'i')
                           AND REGEXP_LIKE (table_name,
                                            CASE
                                               WHEN p_suffix IS NULL
                                                  THEN '?'
                                               ELSE '_' || p_suffix || '$'
                                            END,
                                            'i')
                           AND o.owner = UPPER (p_source_owner)
                           AND t.owner = UPPER (NVL (p_owner, p_source_owner))
                           AND o.object_type IN ('TABLE', 'VIEW', 'SYNONYM')
                           AND object_name <> CASE
                                                WHEN o.owner = t.owner
                                                   THEN table_name
                                                ELSE NULL
                                             END
                           AND partitioned <>
                                  CASE
                                     WHEN REGEXP_LIKE ('no', p_part_tabs, 'i')
                                        THEN NULL
                                     WHEN REGEXP_LIKE ('yes', p_part_tabs, 'i')
                                        THEN 'YES'
                                  END)
      LOOP
         l_rows := TRUE;
         o_app.log_msg (   c_objects.src_owner
                        || '.'
                        || c_objects.src
                        || ' loading into '
                        || c_objects.targ_owner
                        || '.'
                        || c_objects.targ,
                        3);

         -- use the load_tab or merge_tab procedure depending on P_MERGE
         CASE
            WHEN coreutils.is_true (p_trunc)
            THEN
               merge_table (p_source_owner       => c_objects.src_owner,
                            p_source_object      => c_objects.src,
                            p_owner              => c_objects.targ_owner,
                            p_table              => c_objects.targ,
                            p_direct             => p_direct,
                            p_runmode            => o_app.runmode);
            WHEN NOT coreutils.is_true (p_trunc)
            THEN
               insert_table (p_source_owner       => c_objects.src_owner,
                             p_source_object      => c_objects.src,
                             p_owner              => c_objects.targ_owner,
                             p_table              => c_objects.targ,
                             p_direct             => p_direct,
                             p_trunc              => p_trunc,
                             p_runmode            => o_app.runmode);
         END CASE;

         -- whether or not to commit after each table
         IF REGEXP_LIKE ('yes', p_commit, 'i')
         THEN
            COMMIT;
         END IF;
      END LOOP;

      IF NOT l_rows
      THEN
         raise_application_error (coreutils.get_err_cd ('incorrect_parameters'),
                                  coreutils.get_err_msg ('incorrect_parameters'));
      END IF;

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END load_tables;

   -- procedure to exchange a partitioned table with a non-partitioned table
   PROCEDURE exchange_partition (
      p_source_owner     VARCHAR2,
      p_source_table     VARCHAR2,
      p_owner            VARCHAR2,
      p_table            VARCHAR2,
      p_partname         VARCHAR2 DEFAULT NULL,
      p_idx_tablespace   VARCHAR2 DEFAULT NULL,
      p_index_drop       VARCHAR2 DEFAULT 'yes',
      p_gather_stats     VARCHAR2 DEFAULT 'yes',
      p_statpercent      NUMBER DEFAULT DBMS_STATS.auto_sample_size,
      p_statdegree       NUMBER DEFAULT DBMS_STATS.auto_degree,
      p_statmethod       VARCHAR2 DEFAULT DBMS_STATS.get_param ('method_opt'),
      p_runmode          VARCHAR2 DEFAULT NULL)
   IS
      l_src_name       VARCHAR2 (61)                     := p_source_owner || '.' || p_source_table;
      l_tab_name       VARCHAR2 (61)                            := p_owner || '.' || p_table;
      l_target_owner   all_tab_partitions.table_name%TYPE       DEFAULT p_source_owner;
      l_rows           BOOLEAN                                  := FALSE;
      l_partname       dba_tab_partitions.partition_name%TYPE;
      l_ddl            LONG;
      l_numrows        NUMBER;
      l_numblks        NUMBER;
      l_avgrlen        NUMBER;
      l_cachedblk      NUMBER;
      l_cachehit       NUMBER;
      e_no_stats       EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_no_stats, -20000);
      e_compress       EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_compress, -14646);
      o_app            applog
                        := applog (p_module       => 'exchange_partition',
                                   p_runmode      => p_runmode);
   BEGIN
      o_app.set_action ('Determine partition to use');

      -- error if the target table is not partitioned
      IF NOT coreutils.is_part_table (p_owner, p_table)
      THEN
         raise_application_error (coreutils.get_err_cd ('not_partitioned'),
                                  coreutils.get_err_msg ('not_partitioned' || ': ' || p_table));
      END IF;

      -- use either the value for P_PARTNAME or the max partition
      SELECT NVL (UPPER (p_partname), partition_name)
        INTO l_partname
        FROM all_tab_partitions
       WHERE table_name = UPPER (p_table)
         AND table_owner = UPPER (p_owner)
         AND partition_position IN (
                                 SELECT MAX (partition_position)
                                   FROM all_tab_partitions
                                  WHERE table_name = UPPER (p_table)
                                    AND table_owner = UPPER (p_owner));

      -- either gather stats on the table prior to exchanging
      -- or get the stats from the current partition and import it in for the table
      CASE
         WHEN coreutils.is_true (p_gather_stats)
         THEN
            o_app.set_action ('Gather stats on new partition');

            IF NOT o_app.is_debugmode
            THEN
               DBMS_STATS.gather_table_stats (UPPER (p_source_owner),
                                              UPPER (p_source_table),
                                              estimate_percent      => p_statpercent,
                                              DEGREE                => p_statdegree,
                                              method_opt            => p_statmethod);
            END IF;

            o_app.log_msg ('Statistics gathered on table ' || l_src_name, 3);
         WHEN NOT coreutils.is_true (p_gather_stats)
         THEN
            IF NOT o_app.is_debugmode
            THEN
               BEGIN
                  -- if partition stats are not going to be gathered on the new schema,
                  -- keep the current stats of the partition
                  o_app.set_action ('Transfer stats');
                  DBMS_STATS.get_table_stats (UPPER (p_owner),
                                              UPPER (p_table),
                                              l_partname,
                                              numrows        => l_numrows,
                                              numblks        => l_numblks,
                                              avgrlen        => l_avgrlen,
                                              cachedblk      => l_cachedblk,
                                              cachehit       => l_cachehit);
                  DBMS_STATS.set_table_stats (UPPER (p_source_owner),
                                              UPPER (p_source_table),
                                              numrows        => l_numrows,
                                              numblks        => l_numblks,
                                              avgrlen        => l_avgrlen,
                                              cachedblk      => l_cachedblk,
                                              cachehit       => l_cachehit);
               EXCEPTION
                  WHEN e_no_stats
                  THEN
                     -- no stats existed on the target table
                     -- just leave them blank
                     o_app.log_msg ('No stats existed for partition ' || p_partname, 3);
               END;
            END IF;

            o_app.log_msg (   'Statistics transferred from partition '
                           || l_partname
                           || ' of table '
                           || l_tab_name
                           || ' to table '
                           || l_src_name,
                           3);
      END CASE;

      -- build the indexes on the stage table just like the target table
      build_indexes (p_owner             => p_source_owner,
                     p_table             => p_source_table,
                     p_source_owner      => p_owner,
                     p_source_table      => p_table,
                     p_part_type         => 'local',
                     p_tablespace        => p_idx_tablespace,
                     p_runmode           => o_app.runmode);
      o_app.set_action ('Exchange table');

      BEGIN
         coreutils.exec_auto (   'alter table '
                              || l_tab_name
                              || ' exchange partition '
                              || l_partname
                              || ' with table '
                              || l_src_name
                              || ' including indexes update global indexes',
                              o_app.runmode);
         o_app.log_msg (   l_src_name
                        || ' exchanged for partition '
                        || l_partname
                        || ' of table '
                        || l_tab_name);
      EXCEPTION
         WHEN e_compress
         THEN
            -- need to compress the staging table
            coreutils.exec_auto ('alter table ' || l_src_name || ' move compress', o_app.runmode);
            -- now, rerun the exchange
            coreutils.exec_auto (   'alter table '
                                 || l_tab_name
                                 || ' exchange partition '
                                 || l_partname
                                 || ' with table '
                                 || l_src_name
                                 || ' including indexes update global indexes',
                                 o_app.runmode);
            o_app.log_msg (   l_src_name
                           || ' exchanged for partition '
                           || l_partname
                           || ' of table '
                           || l_tab_name);
      END;

      -- drop the indexes on the stage table
      IF coreutils.is_true (p_index_drop)
      THEN
         drop_indexes (p_owner        => p_source_owner, p_table => p_source_table,
                       p_runmode      => o_app.runmode);
      END IF;

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END exchange_partition;

   PROCEDURE pop_partname (
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_partname        VARCHAR2 DEFAULT NULL,
      p_source_owner    VARCHAR2 DEFAULT NULL,
      p_source_object   VARCHAR2 DEFAULT NULL,
      p_source_column   VARCHAR2 DEFAULT NULL,
      p_d_num           NUMBER DEFAULT 0,
      p_p_num           NUMBER DEFAULT 65535)
   AS
      l_dsql            LONG;
      -- to catch empty cursors
      l_source_column   all_part_key_columns.column_name%TYPE;

      TYPE partname_type IS TABLE OF partname.partition_name%TYPE
         INDEX BY BINARY_INTEGER;

      t_partname        partname_type;
   BEGIN
      IF p_partname IS NOT NULL
      THEN
         INSERT INTO tdinc.partname
              VALUES (p_partname);
      ELSE
         IF p_source_column IS NULL
         THEN
            SELECT column_name
              INTO l_source_column
              FROM all_part_key_columns
             WHERE NAME = UPPER (p_table) AND owner = UPPER (p_owner);
         ELSE
            l_source_column := p_source_column;
         END IF;

         coreutils.exec_sql (   'insert into tdinc.partname '
                             || ' SELECT partition_name'
                             || '  FROM all_tab_partitions'
                             || ' WHERE table_owner = '''
                             || UPPER (p_owner)
                             || ''' AND table_name = '''
                             || UPPER (p_table)
                             || ''' AND partition_position IN '
                             || ' (SELECT DISTINCT tbl$or$idx$part$num("'
                             || UPPER (p_owner)
                             || '"."'
                             || UPPER (p_table)
                             || '", 0, '
                             || p_d_num
                             || ', '
                             || p_p_num
                             || ', "'
                             || UPPER (l_source_column)
                             || '")	 FROM '
                             || UPPER (p_source_owner)
                             || '.'
                             || UPPER (p_source_object)
                             || ') '
                             || 'ORDER By partition_position');
      END IF;
   END pop_partname;

   -- Provides functionality for setting local and non-local indexes to unusable based on parameters
   -- Can also base which index partitions to mark as unuable based on the contents of another table
   -- This procedure uses an undocumented database function called tbl$or$idx$part$num.
   -- There are two "magic" numbers that are required to make it work correctly.
   -- The defaults will quite often work.
   -- The simpliest way to find which magic numbers make this function work is to
   -- do a partition exchange on the target table and trace that statement.
   -- sets particular indexes on a table as unusable depending on provided parameters
   PROCEDURE unusable_indexes (
      p_owner           VARCHAR2,                       -- owner of table for the indexes to work on
      p_table           VARCHAR2,                                 -- table to operate on indexes for
      p_partname        VARCHAR2 DEFAULT NULL,        -- partition to mark indexes on (if specified)
      p_source_owner    VARCHAR2 DEFAULT NULL,
      p_source_object   VARCHAR2 DEFAULT NULL,
      p_source_column   VARCHAR2 DEFAULT NULL,
      p_d_num           NUMBER DEFAULT 0,                     -- first magic number from unpublished
      p_p_num           NUMBER DEFAULT 65535,
      p_index_type      VARCHAR2 DEFAULT NULL,    -- possible options: specify different index types
      p_part_type       VARCHAR2 DEFAULT NULL,
      -- when P_PART_NAME is specified, then whether to mark all global's as unusable
      p_runmode         VARCHAR2 DEFAULT NULL)
   IS
      l_msg        VARCHAR2 (2000);
      l_ddl        VARCHAR2 (2000);
      l_pidx_cnt   NUMBER;
      l_idx_cnt    NUMBER;
      l_rows       BOOLEAN         DEFAULT FALSE;
      o_app        applog := applog (p_module       => 'unusable_indexes',
                                     p_runmode      => p_runmode);
   BEGIN
      CASE
         WHEN     p_partname IS NOT NULL
              AND (p_source_owner IS NOT NULL OR p_source_object IS NOT NULL)
         THEN
            raise_application_error (coreutils.get_err_cd ('parms_not_compatible'),
                                        coreutils.get_err_msg ('parms_not_compatible')
                                     || ': P_PARTNAME with either P_SOURCE_OWNER or P_SOURCE_OBJECT');
         WHEN p_source_owner IS NOT NULL AND p_source_object IS NULL
         THEN
            raise_application_error (coreutils.get_err_cd ('parms_not_compatible'),
                                        coreutils.get_err_msg ('parms_not_compatible')
                                     || ': P_SOURCE_OWNER without P_SOURCE_OBJECT');
         WHEN p_source_owner IS NULL AND p_source_object IS NOT NULL
         THEN
            raise_application_error (coreutils.get_err_cd ('parms_not_compatible'),
                                        coreutils.get_err_msg ('parms_not_compatible')
                                     || ': P_SOURCE_OBJECT without P_SOURCE_OWNER');
         ELSE
            NULL;
      END CASE;

      o_app.set_action ('Populate PARTNAME table');
      -- populate a global temporary table with the indexes to work on
      -- this is a requirement because the dynamic SQL needed to use the tbl$or$idx$part$num function
      pop_partname (p_owner              => p_owner,
                    p_table              => p_table,
                    p_partname           => p_partname,
                    p_source_owner       => p_source_owner,
                    p_source_object      => p_source_object,
                    p_source_column      => p_source_column,
                    p_d_num              => p_d_num,
                    p_p_num              => p_p_num);

      -- this cursor will contain all the ALTER INDEX statements necessary to mark indexes unusable
      -- the contents of the cursor depends very much on the parameters specified
      -- also depends on the contents of the PARTNAME global temporary table
      FOR c_idx IN (SELECT DISTINCT    'alter index '
                                    || owner
                                    || '.'
                                    || index_name
                                    || CASE idx_ddl_type
                                          WHEN 'I'
                                             THEN NULL
                                          ELSE ' modify partition ' || partition_name
                                       END
                                    || ' unusable' DDL,
                                    idx_ddl_type,
                                    partition_name,
                                    partition_position,
                                    SUM (CASE idx_ddl_type
                                            WHEN 'I'
                                               THEN 1
                                            ELSE 0
                                         END) OVER (PARTITION BY 1) num_indexes,
                                    SUM (CASE idx_ddl_type
                                            WHEN 'P'
                                               THEN 1
                                            ELSE 0
                                         END) OVER (PARTITION BY 1) num_partitions
                               FROM (SELECT index_type,
                                            owner,
                                            ai.index_name,
                                            partition_name,
                                            partition_position,
                                            partitioned,
                                            CASE
                                               WHEN partition_name IS NULL OR partitioned = 'NO'
                                                  THEN 'I'
                                               ELSE 'P'
                                            END idx_ddl_type
                                       FROM tdinc.partname JOIN all_ind_partitions aip
                                            USING (partition_name)
                                            RIGHT JOIN all_indexes ai
                                            ON ai.index_name = aip.index_name
                                          AND ai.owner = aip.index_owner
                                      WHERE table_name = UPPER (p_table)
                                        AND table_owner = UPPER (p_owner)
                                        AND (ai.status = 'VALID' OR aip.status = 'USABLE'))
                              WHERE REGEXP_LIKE (index_type, '^' || p_index_type, 'i')
                                AND REGEXP_LIKE (partitioned,
                                                 CASE
                                                    WHEN REGEXP_LIKE ('global', p_part_type, 'i')
                                                       THEN 'NO'
                                                    WHEN REGEXP_LIKE ('local', p_part_type, 'i')
                                                       THEN 'YES'
                                                    ELSE '.'
                                                 END,
                                                 'i')
                                AND NOT REGEXP_LIKE (index_type, 'iot', 'i')
                           ORDER BY idx_ddl_type,
                                    partition_position)
      LOOP
         l_rows := TRUE;
         coreutils.exec_auto (c_idx.DDL, p_runmode => o_app.runmode);
         l_pidx_cnt := c_idx.num_partitions;
         l_idx_cnt := c_idx.num_indexes;
      END LOOP;

      IF l_rows
      THEN
         IF l_idx_cnt > 0
         THEN
            o_app.log_msg (   l_idx_cnt
                           || CASE
                                 WHEN coreutils.is_part_table (p_owner, p_table)
                                    THEN ' global'
                                 ELSE NULL
                              END
                           || ' index'
                           || CASE l_idx_cnt
                                 WHEN 1
                                    THEN NULL
                                 ELSE 'es'
                              END
                           || ' affected');
         END IF;

         IF l_pidx_cnt > 0
         THEN
            o_app.log_msg (   l_pidx_cnt
                           || ' local index partition'
                           || CASE l_idx_cnt
                                 WHEN 1
                                    THEN NULL
                                 ELSE 's'
                              END
                           || ' affected');
         END IF;
      ELSE
         o_app.log_msg ('No matching usable indexes found');
      END IF;

      -- commit needed to clear the contents of the global temporary table
      COMMIT;
      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END unusable_indexes;

   -- rebuilds all unusable index segments on a particular table
   PROCEDURE usable_indexes (
      p_owner     VARCHAR2,                             -- owner of table for the indexes to work on
      p_table     VARCHAR2,                                       -- table to operate on indexes for
      p_runmode   VARCHAR2 DEFAULT NULL)
   IS
      l_ddl    VARCHAR2 (2000);
      l_rows   BOOLEAN         := FALSE;                                  -- to catch empty cursors
      l_cnt    NUMBER          := 0;
      o_app    applog
         := applog (p_module       => 'usable_indexes',
                    p_action       => 'Rebuild indexes',
                    p_runmode      => p_runmode);
   BEGIN
      IF NOT o_app.is_debugmode
      THEN
         o_app.log_msg ('Making unusable indexes on ' || p_owner || '.' || p_table || ' usable');
      END IF;

      IF coreutils.is_part_table (p_owner, p_table)
      THEN
         -- rebuild local indexes first
         FOR c_idx IN (SELECT   table_name,
                                partition_position,
                                   'alter table '
                                || table_owner
                                || '.'
                                || table_name
                                || ' modify partition '
                                || partition_name
                                || ' rebuild unusable local indexes' DDL,
                                partition_name
                           FROM all_tab_partitions
                          WHERE table_name = UPPER (p_table) AND table_owner = UPPER (p_owner)
                       ORDER BY table_name,
                                partition_position)
         LOOP
            coreutils.exec_auto (c_idx.DDL, p_runmode => o_app.runmode);
            l_cnt := l_cnt + 1;
         END LOOP;

         o_app.log_msg (   'Any unusable indexes on '
                        || l_cnt
                        || ' table partition'
                        || CASE
                              WHEN l_cnt = 1
                                 THEN NULL
                              ELSE 's'
                           END
                        || ' rebuilt');
      END IF;

      -- reset variables
      l_cnt := 0;
      l_rows := FALSE;

      -- now see if any global are still unusable
      FOR c_gidx IN (SELECT   table_name,
                              'alter index ' || owner || '.' || index_name || ' rebuild' DDL
                         FROM all_indexes
                        WHERE table_name = UPPER (p_table)
                          AND table_owner = UPPER (p_owner)
                          AND status = 'UNUSABLE'
                          AND partitioned = 'NO'
                     ORDER BY table_name)
      LOOP
         l_rows := TRUE;
         coreutils.exec_auto (c_gidx.DDL, o_app.runmode);
         l_cnt := l_cnt + 1;
      END LOOP;

      IF l_rows
      THEN
         o_app.log_msg (   l_cnt
                        || CASE
                              WHEN coreutils.is_part_table (p_owner, p_table)
                                 THEN ' global'
                              ELSE NULL
                           END
                        || ' index'
                        || CASE l_cnt
                              WHEN 1
                                 THEN NULL
                              ELSE 'es'
                           END
                        || ' rebuilt');
      ELSE
         o_app.log_msg (   'No matching unusable '
                        || CASE
                              WHEN coreutils.is_part_table (p_owner, p_table)
                                 THEN 'global '
                              ELSE NULL
                           END
                        || 'indexes found');
      END IF;

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END usable_indexes;
   
   -- structures a merge statement between two tables that have the same table
   PROCEDURE load_scd (
      p_source_owner    VARCHAR2,
      p_source_object   VARCHAR2,
      p_owner           VARCHAR2,
      p_table           VARCHAR2,
      p_type1_columns   VARCHAR2 DEFAULT NULL,
      p_type2_columns   VARCHAR2 DEFAULT NULL
      p_runmode         VARCHAR2 DEFAULT 'no')
   IS
      l_src_name        VARCHAR2 (61)    := p_source_owner || '.' || p_source_object;
      l_trg_name        VARCHAR2 (61)    := p_owner || '.' || p_table;
      o_app             applog
         := applog (p_module       => 'load_scd',
                    p_runmode      => p_runmode,
                    p_action       => 'Check existence of objects');
   BEGIN
      o_app.set_action ('Construct MERGE ON clause');

      -- use the columns provided in P_COLUMNS.
      -- if that is left null, then choose the columns in the primary key of the target table
      -- if there is no primary key, then choose a unique key (any unique key)
      IF p_columns IS NOT NULL
      THEN
         WITH DATA AS
              
              -- this allows us to create a variable IN LIST based on multiple column names provided
              (SELECT     TRIM (SUBSTR (COLUMNS,
                                        INSTR (COLUMNS, ',', 1, LEVEL) + 1,
                                          INSTR (COLUMNS, ',', 1, LEVEL + 1)
                                        - INSTR (COLUMNS, ',', 1, LEVEL)
                                        - 1)) AS token
                     FROM (SELECT ',' || p_columns || ',' COLUMNS
                             FROM DUAL)
               CONNECT BY LEVEL <= LENGTH (p_columns) - LENGTH (REPLACE (p_columns, ',', '')) + 1)
         SELECT REGEXP_REPLACE (   '('
                                || stragg ('target.' || column_name || ' = source.' || column_name)
                                || ')',
                                ',',
                                ' AND' || CHR (10)) LIST
           INTO l_onclause
           FROM dba_tab_columns
          WHERE table_name = UPPER (p_table)
            AND owner = UPPER (p_owner)
            -- select from the variable IN LIST
            AND column_name IN (SELECT *
                                  FROM DATA);
      ELSE
         -- otherwise, we need to get a constraint name
         -- we first choose a PK if it exists
         -- otherwise get a UK at random
         SELECT LIST
           INTO l_onclause
           FROM (SELECT REGEXP_REPLACE (   '('
                                        || stragg (   'target.'
                                                   || column_name
                                                   || ' = source.'
                                                   || column_name)
                                        || ')',
                                        ',',
                                        ' AND' || CHR (10)) LIST,
                        -- the MIN function will ensure that primary keys are selected first
                        -- otherwise, it will randonmly choose a remaining constraint to use
                        MIN (dc.constraint_type) con_type
                   FROM all_cons_columns dcc JOIN all_constraints dc USING (constraint_name,
                                                                            table_name)
                  WHERE table_name = UPPER (p_table)
                    AND dcc.owner = UPPER (p_owner)
                    AND dc.constraint_type IN ('P', 'U'));
      END IF;

      o_app.set_action ('Construct MERGE update clause');

      IF p_columns IS NOT NULL
      THEN
         SELECT REGEXP_REPLACE (stragg ('target.' || column_name || ' = source.' || column_name),
                                ',',
                                ',' || CHR (10))
           INTO l_update
           -- if P_COLUMNS is provided, we use the same logic from the ON clause
           -- to make sure those same columns are not inlcuded in the update clause
           -- MINUS gives us that
         FROM   (WITH DATA AS
                      (SELECT     TRIM (SUBSTR (COLUMNS,
                                                INSTR (COLUMNS, ',', 1, LEVEL) + 1,
                                                  INSTR (COLUMNS, ',', 1, LEVEL + 1)
                                                - INSTR (COLUMNS, ',', 1, LEVEL)
                                                - 1)) AS token
                             FROM (SELECT ',' || p_columns || ',' COLUMNS
                                     FROM DUAL)
                       CONNECT BY LEVEL <=
                                        LENGTH (p_columns) - LENGTH (REPLACE (p_columns, ',', ''))
                                        + 1)
                 SELECT column_name
                   FROM all_tab_columns
                  WHERE table_name = UPPER (p_table) AND owner = UPPER (p_owner)
                 MINUS
                 SELECT column_name
                   FROM dba_tab_columns
                  WHERE table_name = UPPER (p_table)
                    AND owner = UPPER (p_owner)
                    AND column_name IN (SELECT *
                                          FROM DATA));
      ELSE
         -- otherwise, we once again MIN a constraint type to ensure it's the same constraint
         -- then, we just minus the column names so they aren't included
         SELECT REGEXP_REPLACE (stragg ('target.' || column_name || ' = source.' || column_name),
                                ',',
                                ',' || CHR (10))
           INTO l_update
           FROM (SELECT column_name
                   FROM all_tab_columns
                  WHERE table_name = UPPER (p_table) AND owner = UPPER (p_owner)
                 MINUS
                 SELECT column_name
                   FROM (SELECT   column_name,
                                  MIN (dc.constraint_type) con_type
                             FROM all_cons_columns dcc JOIN all_constraints dc
                                  USING (constraint_name, table_name)
                            WHERE table_name = UPPER (p_table)
                              AND dcc.owner = UPPER (p_owner)
                              AND dc.constraint_type IN ('P', 'U')
                         GROUP BY column_name));
      END IF;

      o_app.set_action ('Construnct MERGE insert clause');

      SELECT   REGEXP_REPLACE ('(' || stragg ('target.' || column_name) || ') ', ',',
                               ',' || CHR (10)) LIST
          INTO l_insert
          FROM all_tab_columns
         WHERE table_name = UPPER (p_table) AND owner = UPPER (p_owner)
      ORDER BY column_name;

      o_app.set_action ('Construct MERGE values clause');
      l_values := REGEXP_REPLACE (l_insert, 'target.', 'source.');
      o_app.log_msg (   'Merging records from '
                     || p_source_owner
                     || '.'
                     || p_source_object
                     || ' into '
                     || p_owner
                     || '.'
                     || p_table);

      BEGIN
         o_app.set_action ('Issue MERGE statement');
         -- ENABLE|DISABLE parallel dml depending on the value of P_DIRECT
         coreutils.exec_sql (   'ALTER SESSION '
                             || CASE
                                   WHEN REGEXP_LIKE ('yes', p_direct, 'i')
                                      THEN 'ENABLE'
                                   ELSE 'DISABLE'
                                END
                             || ' PARALLEL DML',
                             p_runmode      => o_app.runmode);
         o_app.log_msg ('Merging records from ' || l_src_name || ' into ' || l_trg_name, 3);
         -- we put the merge statement together using all the different clauses constructed above
         coreutils.exec_sql (   REGEXP_REPLACE (   'MERGE INTO '
                                                || p_owner
                                                || '.'
                                                || p_table
                                                || ' target using '
                                                || CHR (10)
                                                || '(select * from '
                                                || p_source_owner
                                                || '.'
                                                || p_source_object
                                                || ') source on '
                                                || CHR (10)
                                                || l_onclause
                                                || CHR (10)
                                                || ' WHEN MATCHED THEN UPDATE SET '
                                                || CHR (10)
                                                || l_update
                                                || CHR (10)
                                                || ' WHEN NOT MATCHED THEN INSERT /*+ APPEND */ '
                                                || CHR (10)
                                                || l_insert
                                                || CHR (10)
                                                || ' VALUES '
                                                || CHR (10)
                                                || l_values,
                                                -- just strip the APPEND hint out if P_DIRECT is 'no'
                                                CASE
                                                   WHEN REGEXP_LIKE ('no', p_direct, 'i')
                                                      THEN '/\*\+ APPEND \*/ '
                                                   ELSE NULL
                                                END)
                             -- if we specify a logging table, append that on the end
                             || CASE p_log_table
                                   WHEN NULL
                                      THEN NULL
                                   ELSE    ' log errors into '
                                        || p_log_table
                                        || ' reject limit '
                                        -- if no reject limit is specified, then use unlimited
                                        || p_reject_limit
                                END,
                             o_app.runmode);
      EXCEPTION
         -- ON columns not specified correctly
         WHEN e_no_on_columns
         THEN
            raise_application_error (coreutils.get_err_cd ('on_clause_missing'),
                                     coreutils.get_err_msg ('on_clause_missing'));
      END;

      -- show the records merged
      o_app.log_cnt_msg (SQL%ROWCOUNT);
      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END load_scd;
END dbflex;
/