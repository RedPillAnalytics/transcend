CREATE OR REPLACE PACKAGE BODY tdinc.dbflex
AS
-- truncates a table based upon the input table name and owner
-- operates in an autonomous transaction
   PROCEDURE trunc_tab (p_owner IN VARCHAR2, p_table IN VARCHAR2)
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_app   app_info := app_info (p_module => 'DBFLEX.TRUNC_TAB');
   BEGIN
      EXECUTE IMMEDIATE 'truncate table ' || p_owner || '.' || p_table;

      o_app.clear_app_info;
   END trunc_tab;

   -- creates all the local indexes and constraints on one table to be exactly like those of another table
   PROCEDURE clone_indexes (
      p_source_owner   VARCHAR2,
      p_source_table   VARCHAR2,
      p_target_owner   VARCHAR2,
      p_target_table   VARCHAR2,
      p_debug          BOOLEAN DEFAULT FALSE)
   IS
      l_ddl         VARCHAR2 (2000);
      l_ind_count   NUMBER                     := 1;
      l_username    user_users.username%TYPE;
      l_rows        BOOLEAN                    := FALSE;                  -- to catch empty cursors
      l_app         app_info   := app_info (p_module      => 'DBFLEX.CLONE_INDEXES',
                                            p_debug       => p_debug);
   BEGIN
      IF p_debug
      THEN
         SELECT username
           INTO l_username
           FROM user_users;

         o_app.log_msg ('Executing user: ' || l_username);
      ELSE
         o_app.log_msg ('Building indexes on ' || p_target_table);
         -- this is required for dbms_metadata to work correctly.
         DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'SQLTERMINATOR', TRUE);
      END IF;

      -- create a cursor containing the DDL from the target indexes
      FOR c_indexes IN (SELECT DBMS_METADATA.get_ddl ('INDEX', index_name, owner) index_ddl
                          FROM (SELECT DISTINCT ai.index_name,
                                                ai.owner
                                           FROM all_indexes ai JOIN all_ind_partitions ap
                                                ON (    ai.owner = ap.index_owner
                                                    AND ai.index_name = ap.index_name)
                                          WHERE table_name = p_source_table
                                            AND owner = p_source_owner
                                MINUS
                                SELECT constraint_name index_name,
                                       owner
                                  FROM all_constraints
                                 WHERE table_name = p_source_table
                                   AND owner = p_source_owner
                                   AND constraint_type IN ('P', 'U')))
      LOOP
         -- remove double quotes, as it's easier to work without them
         -- remove the local index information, as this is for exchanges
         -- replace the source owner with the target owner
         -- replace the source table with the target table
         o_app.set_action ('Format index DDL');
         l_ddl :=
            REGEXP_REPLACE (REGEXP_REPLACE (REGEXP_REPLACE (c_indexes.index_ddl, '"', NULL),
                                            '(local.+)?(;)',
                                            NULL,
                                            1,
                                            0,
                                            'in'),
                            '\.' || p_source_table,
                            '.' || p_target_table);

         -- if the table_name is in a different schema, then everything is fine
         -- if it's in the same schema, the indexes need to be renamed
         IF p_source_owner = p_target_owner
         THEN
            l_ddl :=
               REGEXP_REPLACE (l_ddl,
                               '\\..+ ON',
                               '.' || p_target_table || '_ik' || l_ind_count || ' ON');
            l_ind_count := l_ind_count + 1;
         ELSE
            l_ddl := REGEXP_REPLACE (l_ddl, p_source_owner || '\.', p_target_owner || '.');
         END IF;

         o_app.set_action ('Execute index DDL');

         IF p_debug
         THEN
            o_app.log_msg ('Index DDL: ' || l_ddl);
         ELSE
            EXECUTE IMMEDIATE l_ddl;
         END IF;
      END LOOP;

      -- now clone constraints
      o_app.set_action ('Open cursor for all constraints');

      IF NOT p_debug
      THEN
         o_app.log_msg ('Building constraints on ' || p_target_table);
      END IF;

      FOR c_constraints IN (SELECT DBMS_METADATA.get_ddl ('CONSTRAINT', constraint_name, owner)
                                                                                     constraint_ddl,
                                   owner,
                                   constraint_name
                              FROM all_constraints
                             WHERE table_name = p_source_table
                               AND owner = p_source_owner
                               AND constraint_type IN ('P', 'U')
                            UNION ALL
                            SELECT DBMS_METADATA.get_ddl ('REF_CONSTRAINT', constraint_name, owner)
                                                                                     constraint_ddl,
                                   owner,
                                   constraint_name
                              FROM all_constraints
                             WHERE table_name = p_source_table
                               AND owner = p_source_owner
                               AND constraint_type = 'R')
      LOOP
         -- catch empty cursor sets
         l_rows := TRUE;
         -- remove double quotes, as it's easier to work without them
         -- replace the source owner with the target owner
         -- replace the source table with the target table
         o_app.set_action ('Format constraint DDL');
         l_ddl :=
            REGEXP_REPLACE (REGEXP_REPLACE (REGEXP_REPLACE (c_constraints.constraint_ddl, '"', NULL),
                                            '(local.+)?(;)',
                                            NULL,
                                            1,
                                            0,
                                            'in'),
                            '\.' || p_source_table,
                            '.' || p_target_table);

         -- if the table_name is in a different schema, then everything is fine
         -- if it's in the same schema, the indexes need to be renamed
         IF p_source_owner = p_target_owner
         THEN
            l_ddl :=
               REGEXP_REPLACE (l_ddl,
                               '\..+ ON',
                               '\.' || p_target_table || '_ik' || l_ind_count || ' ON');
            l_ind_count := l_ind_count + 1;
         ELSE
            l_ddl := REGEXP_REPLACE (l_ddl, p_source_owner || '\.', p_target_owner || '.');
         END IF;

         -- debug ddl after length checks
         COMMIT;

         IF p_debug
         THEN
            o_app.log_msg ('Index DDL: ' || l_ddl);
         ELSE
            o_app.set_action ('Execute constraint DDL');

            EXECUTE IMMEDIATE l_ddl;
         END IF;
      END LOOP;

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END clone_indexes;

   -- drop indexes and constraints from a table
   PROCEDURE drop_indexes (p_owner VARCHAR2, p_table VARCHAR2, p_debug BOOLEAN DEFAULT FALSE)
   IS
      l_ind_ddl   VARCHAR2 (2000);
      l_rows      BOOLEAN         := FALSE;
      l_sql       VARCHAR2 (2000);
      l_app       app_info      := app_info (p_module      => 'DBFLEX.DROP_INDEXES',
                                             p_debug       => p_debug);
   BEGIN
      -- drop constraints
      FOR l_constraints IN (SELECT constraint_name,
                                   table_name
                              FROM all_constraints
                             WHERE table_name = p_table AND owner = p_owner
                                   AND constraint_type <> 'C')
      LOOP
         -- catch empty cursor sets
         l_rows := TRUE;
         l_sql :=
               'alter table '
            || p_owner
            || '.'
            || l_constraints.table_name
            || ' drop constraint '
            || l_constraints.constraint_name;

         IF p_debug
         THEN
            o_app.log_msg ('SQL: ' || l_sql);
         ELSE
            o_app.set_action ('Execute constraint DDL');

            EXECUTE IMMEDIATE l_sql;
         END IF;
      END LOOP;

      o_app.set_action ('Open cursor for indexes');
      o_app.log_msg ('Dropping indexes on ' || p_table);

      -- drop indexes
      FOR l_indexes IN (SELECT index_name,
                               table_name
                          FROM all_indexes
                         WHERE table_name = p_table AND owner = p_owner)
      LOOP
         -- try to catch empty cursor sets
         l_rows := TRUE;
         l_sql := 'drop index ' || p_owner || '.' || l_indexes.index_name;

         IF p_debug
         THEN
            o_app.log_msg ('SQL: ' || l_sql);
         ELSE
            o_app.set_action ('Execute index DDL');

            EXECUTE IMMEDIATE l_sql;
         END IF;
      END LOOP;

      o_app.clear_app_info;
   END drop_indexes;

   -- structures an insert or insert append statement from the source to the target provided
   PROCEDURE load_tab (
      p_source_owner    VARCHAR2,
      p_source_object   VARCHAR2,
      p_target_owner    VARCHAR2,
      p_target_table    VARCHAR2,
      p_trunc           BOOLEAN DEFAULT FALSE,
      p_direct          BOOLEAN DEFAULT TRUE)
   IS
      l_sqlstmt   VARCHAR2 (2000);
      l_object    all_objects.object_name%TYPE;
      l_table     all_tables.table_name%TYPE;
      l_app       app_info                       := app_info (p_module => 'DBFLEX.LOAD_TAB');
   BEGIN
      IF p_trunc
      THEN
         -- truncate the target table
         trunc_tab (p_target_owner, p_target_table);
      END IF;

      -- change the action back after calling trunc_tab
      o_app.set_action ('Construnct insert statement');
      o_app.log_msg (   'Loading records from '
                     || p_source_owner
                     || '.'
                     || p_source_object
                     || ' into '
                     || p_target_owner
                     || '.'
                     || p_target_table);
      l_sqlstmt :=
            'insert /*+ APPEND */ into '
         || p_target_owner
         || '.'
         || p_target_table
         || ' select * from '
         || p_source_owner
         || '.'
         || p_source_object;

      -- if this is not direct-path, then modify the statement
      IF NOT p_direct
      THEN
         l_sqlstmt := REGEXP_REPLACE (l_sqlstmt, '/\*\+ APPEND \*/ ', NULL);
      END IF;

      -- debug statement
      --o_app.log_msg ('Insert statement: ' || l_sqlstmt);
      EXECUTE IMMEDIATE l_sqlstmt;

      o_app.log_msg (SQL%ROWCOUNT || ' records loaded' || CHR (10));
      -- load the bad data into the ext_bad_log
      o_app.clear_app_info;
   END load_tab;

   -- structures a merge statement between two tables that have the same table
   PROCEDURE merge_tab (
      p_source_owner   VARCHAR2,
      p_source_table   VARCHAR2,
      p_target_owner   VARCHAR2,
      p_target_table   VARCHAR2,
      p_direct         BOOLEAN DEFAULT TRUE)
   IS
      l_sqlstmt      VARCHAR2 (32000);
      l_onclause     VARCHAR2 (32000);
      l_update       VARCHAR2 (32000);
      l_insert       VARCHAR2 (32000);
      l_values       VARCHAR2 (32000);
      e_unique_key   EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_unique_key, -936);
      l_app          app_info
                     := app_info (p_module      => 'DBFLEX.MERGE_TAB',
                                  p_action      => 'Generate ON clause');
   BEGIN
      -- construct the "ON" clause for the MERGE statement
      SELECT LIST
        INTO l_onclause
        FROM (SELECT REGEXP_REPLACE (   '('
                                     || stragg ('target.' || column_name || ' = source.'
                                                || column_name)
                                     || ')',
                                     ',',
                                     ' AND' || CHR (10)) LIST,
                     MIN (dc.constraint_type) con_type
                FROM all_cons_columns dcc JOIN all_constraints dc USING (constraint_name,
                                                                         table_name)
               WHERE table_name = UPPER (p_target_table)
                 AND dcc.owner = UPPER (p_target_owner)
                 AND dc.constraint_type IN ('P', 'U'));

      o_app.set_action ('Generate UPDATE clause');

      -- construct the "UPDATE" clause for the MERGE statement
      SELECT REGEXP_REPLACE (stragg ('target.' || column_name || ' = source.' || column_name),
                             ',',
                             ',' || CHR (10))
        INTO l_update
        FROM (SELECT column_name
                FROM all_tab_columns
               WHERE table_name = UPPER (p_target_table) AND owner = UPPER (p_target_owner)
              MINUS
              SELECT column_name
                FROM (SELECT   column_name,
                               MIN (dc.constraint_type) con_type
                          FROM all_cons_columns dcc JOIN all_constraints dc
                               USING (constraint_name, table_name)
                         WHERE table_name = UPPER (p_target_table)
                           AND dcc.owner = UPPER (p_target_owner)
                           AND dc.constraint_type IN ('P', 'U')
                      GROUP BY column_name));

      o_app.set_action ('Generate INSERT clause');

      -- construct the "INSERT" clause for the MERGE statement
      SELECT   REGEXP_REPLACE ('(' || stragg ('target.' || column_name) || ') ', ',',
                               ',' || CHR (10)) LIST
          INTO l_insert
          FROM all_tab_columns
         WHERE table_name = UPPER (p_target_table) AND owner = UPPER (p_target_owner)
      ORDER BY column_name;

      o_app.set_action ('Generate VALUES clause');
      -- construct the "VALUES" clause for the MERGE statement
      l_values := REGEXP_REPLACE (l_insert, 'target.', 'source.');
      -- put the entire statement together
      l_sqlstmt :=
            'MERGE INTO '
         || p_target_owner
         || '.'
         || p_target_table
         || ' target using '
         || CHR (10)
         || '(select * from '
         || p_source_owner
         || '.'
         || p_source_table
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
         || l_values;
      o_app.log_msg (   'Merging records from '
                     || p_source_owner
                     || '.'
                     || p_source_table
                     || ' into '
                     || p_target_owner
                     || '.'
                     || p_target_table);

      -- if the insert won't be direct, then modify the ddl statement
      IF NOT p_direct
      THEN
         l_sqlstmt := REGEXP_REPLACE (l_sqlstmt, '/\*\+ APPEND \*/ ', NULL);
      END IF;

      BEGIN
         o_app.set_action ('Issue MERGE statement');

         EXECUTE IMMEDIATE l_sqlstmt;
      EXCEPTION
         -- catch "non-unique" errors
         WHEN e_unique_key
         THEN
            raise_application_error (-20001, 'Missing a unique constraint for MERGE operation.');
      END;

      -- show the records merged
      o_app.log_msg (SQL%ROWCOUNT || ' records merged' || CHR (10));
      o_app.clear_app_info;
   END merge_tab;

   -- queries the dictionary based on regular expressions and loads tables using either the load_tab method or the merge_tab method
   PROCEDURE load_regexp (
      p_source_owner   VARCHAR2,
      p_regexp         VARCHAR2,
      p_target_owner   VARCHAR2 DEFAULT NULL,
      p_suf_re_rep     VARCHAR2 DEFAULT '?',
      p_merge          BOOLEAN DEFAULT FALSE,
      p_part_tabs      BOOLEAN DEFAULT TRUE,
      p_trunc          BOOLEAN DEFAULT FALSE,
      p_direct         BOOLEAN DEFAULT TRUE,
      p_commit         BOOLEAN DEFAULT TRUE,
      p_debug          BOOLEAN DEFAULT FALSE)
   IS
      l_target_owner     all_tables.owner%TYPE   := p_source_owner;
      l_rows             BOOLEAN                 := FALSE;
      e_data_cartridge   EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_data_cartridge, -29913);
      l_app              app_info
                                 := app_info (p_module      => 'DBFLEX.LOAD_REGEXP',
                                              p_debug       => p_debug);
   BEGIN
      IF NOT p_debug
      THEN
         -- enable session parameters depending on p_direct
         IF p_direct
         THEN
            EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
         ELSE
            EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML';
         END IF;
      END IF;

      o_app.log_msg (   'Loading tables matching regular expression for the '
                     || p_source_owner
                     || ' schema.'
                     || CHR (10));

      IF p_target_owner IS NOT NULL
      THEN
         l_target_owner := p_target_owner;
      END IF;

       -- determine whether partitioned tables will be allowed
      -- for performance reasons, I wrote two different cursors
      -- instead of just checking to see if each table is a partitioned table in the loop
      IF p_part_tabs
      THEN
         o_app.set_action ('Load all objects');

         -- dynamic cursor contains source and target objects
         FOR c_objects IN (SELECT o.owner src_owner,
                                  object_name src,
                                  t.owner targ_owner,
                                  table_name targ
                             FROM all_objects o JOIN all_tables t
                                  ON (REGEXP_REPLACE (object_name, '_[[:alnum:]]+$', NULL) =
                                                     REGEXP_REPLACE (table_name, p_suf_re_rep, NULL))
                            WHERE REGEXP_LIKE (object_name, p_regexp, 'i')
                              AND REGEXP_LIKE (table_name, p_suf_re_rep, 'i')
                              AND o.owner = p_source_owner
                              AND t.owner = l_target_owner
                              AND o.object_type IN ('TABLE', 'VIEW', 'SYNONYM'))
         LOOP
            l_rows := TRUE;

            IF p_debug
            THEN
               o_app.log_msg (   c_objects.src_owner
                              || '.'
                              || c_objects.src
                              || ' will be loaded into '
                              || c_objects.targ_owner
                              || '.'
                              || c_objects.targ);
            ELSE
               -- use the load_tab or merge_tab procedure depending on P_MERGE
               IF p_merge
               THEN
                  merge_tab (c_objects.src_owner,
                             c_objects.src,
                             c_objects.targ_owner,
                             c_objects.targ,
                             p_direct);
               ELSE
                  load_tab (c_objects.src_owner,
                            c_objects.src,
                            c_objects.targ_owner,
                            c_objects.targ,
                            p_trunc,
                            p_direct);
               END IF;

               -- whether or not to commit after each table
               IF p_commit
               THEN
                  COMMIT;
               END IF;
            END IF;
         END LOOP;
      ELSE
         o_app.set_action ('Load non-partitioned tables and other objects');

         -- dynamic cursor contains source and target objects of only non-partitioned tables
         FOR c_objects IN (SELECT o.owner src_owner,
                                  t.owner targ_owner,
                                  object_name src,
                                  table_name targ
                             FROM all_objects o
                                  JOIN
                                  (SELECT owner,
                                          table_name
                                     FROM all_tables
                                   MINUS
                                   SELECT table_owner,
                                          table_name
                                     FROM all_tab_partitions) t
                                  ON (REGEXP_REPLACE (object_name, '_[[:alnum:]]+$', NULL) =
                                                     REGEXP_REPLACE (table_name, p_suf_re_rep, NULL))
                            WHERE REGEXP_LIKE (object_name, p_regexp, 'i')
                              AND REGEXP_LIKE (table_name, p_suf_re_rep, 'i')
                              AND o.owner = p_source_owner
                              AND t.owner = l_target_owner
                              AND o.object_type IN ('TABLE', 'VIEW', 'SYNONYM'))
         LOOP
            l_rows := TRUE;

            IF p_debug
            THEN
               o_app.log_msg ('Running in DEBUG Mode');
               o_app.log_msg (   c_objects.src_owner
                              || '.'
                              || c_objects.src
                              || ' will be loaded into '
                              || c_objects.targ_owner
                              || '.'
                              || c_objects.targ);
            ELSE
               -- use the load_tab or merge_tab procedure depending on P_MERGE
               IF p_merge
               THEN
                  merge_tab (c_objects.src_owner,
                             c_objects.src,
                             c_objects.targ_owner,
                             c_objects.targ,
                             p_direct);
               ELSE
                  load_tab (c_objects.src_owner,
                            c_objects.src,
                            c_objects.targ_owner,
                            c_objects.targ,
                            p_trunc,
                            p_direct);
               END IF;

               -- whether or not to commit after each table
               IF p_commit
               THEN
                  COMMIT;
               END IF;
            END IF;
         END LOOP;
      END IF;

      IF NOT l_rows
      THEN
         raise_application_error (-20001,
                                  'Combination of parameters renders zero target tables to load');
      END IF;

      o_app.clear_app_info;
   EXCEPTION
      WHEN e_data_cartridge
      THEN
         -- use a regular expression to pull the KUP error out of SQLERRM
         CASE REGEXP_SUBSTR (SQLERRM, '^KUP-[[:digit:]]{5}', 1, 1, 'im')
             -- the case statement allows multiple WHEN/THEN clauses
            -- to handle any number of errors
         WHEN 'KUP-04040'
            THEN
               o_app.log_msg ('The file does not exist in the directory');
               -- do whatever else needs to be done if the file doesn't exist
            -- you could even retry the FTP (using UTL_FTP)
            -- or send the logfile using UTL_MAIL, which allows attachments
         ELSE
               o_app.log_msg ('Unknown data cartridge error');
         -- do whatever is needed if error is unknown
         END CASE;

         RAISE;
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END load_regexp;

   -- procedure to exchange a partitioned table with a non-partitioned table
   PROCEDURE exchange_table (
      p_source_owner   VARCHAR2,
      p_source_table   VARCHAR2,
      p_target_owner   VARCHAR2,
      p_target_table   VARCHAR2,
      p_partname       VARCHAR2 DEFAULT NULL,
      p_index_drop     BOOLEAN DEFAULT TRUE,
      p_stats          VARCHAR2 DEFAULT 'KEEP',
      p_statpercent    NUMBER DEFAULT DBMS_STATS.auto_sample_size,
      p_statdegree     NUMBER DEFAULT DBMS_STATS.default_degree,
      p_statmo         VARCHAR2 DEFAULT 'FOR ALL COLUMNS SIZE AUTO',
      p_debug          BOOLEAN DEFAULT FALSE)
   IS
      l_target_owner   all_tab_partitions.table_name%TYPE       DEFAULT p_source_owner;
      l_rows           BOOLEAN                                  := FALSE;
      l_partname       all_tab_partitions.partition_name%TYPE;
      l_sql            VARCHAR2 (2000);
      l_numrows        NUMBER;
      l_numblks        NUMBER;
      l_avgrlen        NUMBER;
      l_cachedblk      NUMBER;
      l_cachehit       NUMBER;
      e_no_stats       EXCEPTION;
      PRAGMA EXCEPTION_INIT (e_no_stats, -20000);
      l_app            app_info
                              := app_info (p_module      => 'DBFLEX.EXCHANGE_TABLE',
                                           p_debug       => p_debug);
   BEGIN
      IF NOT p_debug
      THEN
         o_app.log_msg (   'Exchanging table '
                        || p_source_owner
                        || '.'
                        || p_source_table
                        || ' for '
                        || p_target_owner
                        || '.'
                        || p_target_table
                        || CHR (10));
      END IF;

      -- determine whether to use P_PARTNAME or use the max partition
      IF p_partname IS NOT NULL
      THEN
         l_partname := p_partname;
      ELSE
         BEGIN
            o_app.set_action ('Determine partition name');

            SELECT partition_name
              INTO l_partname
              FROM all_tab_partitions
             WHERE table_name = p_target_table
               AND table_owner = p_target_owner
               AND partition_position IN (
                                  SELECT MAX (partition_position)
                                    FROM all_tab_partitions
                                   WHERE table_name = p_target_table
                                         AND table_owner = p_target_owner);
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               raise_application_error (-20001, 'Partition name cannot be returned.');
         END;
      END IF;

      IF p_debug
      THEN
         NULL;
      ELSE
         -- do something with stats
         BEGIN
            CASE
               WHEN REGEXP_LIKE (p_stats, 'keep', 'i')
               THEN
                  -- just use the table level statistics for the new partition now
                  -- the automatic stats collection will gather new stats later
                  o_app.set_action ('Transfer table stats to new partition');
                  DBMS_STATS.get_table_stats (p_target_owner,
                                              p_target_table,
                                              l_partname,
                                              numrows        => l_numrows,
                                              numblks        => l_numblks,
                                              avgrlen        => l_avgrlen,
                                              cachedblk      => l_cachedblk,
                                              cachehit       => l_cachehit);
                  DBMS_STATS.set_table_stats (p_source_owner,
                                              p_source_table,
                                              numrows        => l_numrows,
                                              numblks        => l_numblks,
                                              avgrlen        => l_avgrlen,
                                              cachedblk      => l_cachedblk,
                                              cachehit       => l_cachehit);
               -- gather new stats on the partition
            WHEN REGEXP_LIKE (p_stats, 'new', 'i')
               THEN
                  o_app.set_action ('Gather stats on new partition');
                  DBMS_STATS.gather_table_stats (p_source_owner,
                                                 p_source_table,
                                                 estimate_percent      => p_statpercent,
                                                 DEGREE                => p_statdegree,
                                                 method_opt            => p_statmo);
               ELSE
                  NULL;
            END CASE;
         EXCEPTION
            WHEN e_no_stats
            THEN
               -- if no stats existed on the target table, then generate new stats
               o_app.log_msg (   'No stats existed on '
                              || p_target_owner
                              || '.'
                              || p_target_table
                              || '... gathered new stats');
               DBMS_STATS.gather_table_stats (p_source_owner,
                                              p_source_table,
                                              estimate_percent      => p_statpercent,
                                              DEGREE                => p_statdegree,
                                              method_opt            => p_statmo);
         END;
      END IF;

      -- build the indexes on the stage table just like the target table
      clone_indexes (p_target_owner, p_target_table, p_source_owner, p_source_table, p_debug);
      o_app.set_action ('Exchange table');
      l_sql :=
            'alter table '
         || p_target_owner
         || '.'
         || p_target_table
         || ' exchange partition '
         || l_partname
         || ' with table '
         || p_source_owner
         || '.'
         || p_source_table
         || ' including indexes update global indexes';

      BEGIN
         IF p_debug
         THEN
            o_app.log_msg ('SQL: ' || l_sql);
         ELSE
            -- exchange in the partitions
            EXECUTE IMMEDIATE l_sql;

            o_app.log_msg (p_source_table || ' exchanged for ' || p_target_table);
         END IF;
      -- need to drop indexes on the temp partitions if an exception occurs in this block
      -- this is for restartability
      EXCEPTION
         WHEN OTHERS
         THEN
            o_app.log_err;
            o_app.log_msg ('Dropping indexes because of exception');
            drop_indexes (p_source_owner, p_source_table, p_debug);
            -- re-raise the originial exception
            RAISE;
      END;

      -- drop the indexes on the stage table
      IF p_index_drop
      THEN
         drop_indexes (p_source_owner, p_source_table, p_debug);
      END IF;

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END exchange_table;

   -- queries the dictionary using a regexp looking for single-partitioned tables
   -- those tables matching will be exchanged with non-partitioned tables matching a similar regexp
   PROCEDURE exchange_regexp (
      p_source_owner   VARCHAR2,
      p_regexp         VARCHAR2,
      p_target_owner   VARCHAR2 DEFAULT NULL,
      p_suf_re_rep     VARCHAR2 DEFAULT '?',
      p_partname       VARCHAR2 DEFAULT NULL,
      p_index_drop     BOOLEAN DEFAULT TRUE,
      p_stats          VARCHAR2 DEFAULT 'KEEP',
      p_statpercent    NUMBER DEFAULT DBMS_STATS.auto_sample_size,
      p_statdegree     NUMBER DEFAULT DBMS_STATS.default_degree,
      p_statmo         VARCHAR2 DEFAULT 'FOR ALL COLUMNS SIZE AUTO',
      p_debug          BOOLEAN DEFAULT FALSE)
   IS
      l_target_owner   all_tab_partitions.table_name%TYPE       DEFAULT p_source_owner;
      l_rows           BOOLEAN                                  := FALSE;
      l_partname       all_tab_partitions.partition_name%TYPE;
      l_sql            VARCHAR2 (2000);
      l_app            app_info
                             := app_info (p_module      => 'DBFLEX.EXCHANGE_REGEXP',
                                          p_debug       => p_debug);
   BEGIN
      IF NOT p_debug
      THEN
         o_app.log_msg (   'Exchanging tables matching the regular expression for the '
                        || p_source_owner
                        || ' schema'
                        || CHR (10));
      END IF;

      IF p_target_owner IS NOT NULL
      THEN
         l_target_owner := p_target_owner;
      END IF;

      o_app.set_action ('Open cursor of tables');

      -- dynamic cursor containing all single-partitioned tables
      FOR c_objects IN (SELECT tt.owner src_owner,
                               tt.table_name src,
                               tp.table_owner targ_owner,
                               tp.table_name targ,
                               tp.num_partitions
                          FROM all_tables tt
                               JOIN
                               (SELECT   table_owner,
                                         table_name,
                                         COUNT (partition_name) num_partitions
                                    FROM all_tab_partitions
                                GROUP BY table_owner,
                                         table_name) tp
                               ON (REGEXP_REPLACE (tt.table_name, '_[[:alnum:]]+$', NULL) =
                                                  REGEXP_REPLACE (tp.table_name, p_suf_re_rep, NULL))
                         WHERE REGEXP_LIKE (tt.table_name, p_regexp, 'i')
                           AND REGEXP_LIKE (tp.table_name, p_suf_re_rep, 'i')
                           AND tt.owner = p_source_owner
                           AND tp.table_owner = l_target_owner)
      LOOP
         -- as I'm using dynamic cursors, I need to catch empty cursor sets
         l_rows := TRUE;
         o_app.set_action ('Exchange tables');
         exchange_table (c_objects.src_owner,
                         c_objects.src,
                         c_objects.targ_owner,
                         c_objects.targ,
                         p_partname,
                         p_index_drop,
                         p_stats,
                         p_statpercent,
                         p_statdegree,
                         p_statmo,
                         p_debug);
      END LOOP;

      o_app.set_action ('Check exceptions');

      -- if l_rows is still false, then no results from the cursor
      IF NOT l_rows
      THEN
         raise_application_error (-20001, 'No target tables exist for the schema specified');
      END IF;

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END exchange_regexp;

   -- sets particular indexes on a table as unusable depending on provided parameters
   PROCEDURE unusable_indexes (
      p_owner        VARCHAR2,                          -- owner of table for the indexes to work on
      p_table        VARCHAR2,                                    -- table to operate on indexes for
      p_part_name    VARCHAR2 DEFAULT NULL,           -- partition to mark indexes on (if specified)
      p_index_type   VARCHAR2 DEFAULT NULL,       -- possible options: specify different index types
      p_global       BOOLEAN DEFAULT FALSE,
      -- when P_PART_NAME is specified, then whether to mark all global's as unusable
      p_debug        BOOLEAN DEFAULT FALSE)
   IS
      l_msg           VARCHAR2 (2000);
      l_ddl           VARCHAR2 (2000);
      l_cnt           NUMBER                         := 0;
      l_partitioned   all_indexes.partitioned%TYPE;
      l_app           app_info
                            := app_info (p_module      => 'DBFLEX.UNUSABLE_INDEXES',
                                         p_debug       => p_debug);
   BEGIN
      o_app.set_action ('Test to see if the table is partitioned');

      -- find out whether the table specified is partitioned
      SELECT partitioned
        INTO l_partitioned
        FROM all_tables
       WHERE table_name = UPPER (p_table) AND owner = UPPER (p_owner);

      o_app.set_action ('Evaluate global indexes to work on');

      CASE
         -- error if P_PART_NAME is specified but the table isn't partitioned
      WHEN l_partitioned = 'NO' AND p_part_name IS NOT NULL
         THEN
            raise_application_error (-20001,
                                        p_owner
                                     || '.'
                                     || p_table
                                     || ' is not partitioned; cannot specify P_PART_NAME');
         -- P_GLOBAL is only approprate when this is a partitioned table
         -- and P_PART_NAME is specified
      WHEN p_global AND (p_part_name IS NULL OR l_partitioned = 'NO')
         THEN
            raise_application_error
               (-20001,
                'Specifying TRUE for P_GLOBAL is only appropriate when P_PART_NAME is specified and the table is partitioned.');
         WHEN p_global AND l_partitioned = 'YES' AND p_part_name IS NOT NULL
         THEN
            -- this is a partitioned table
            -- and we specified TRUE for P_GLOBAL
            -- and we specified P_PART_NAME
            -- first mark all global indexes as unusable
            FOR c_gidx IN (SELECT 'alter index ' || owner || '.' || index_name || ' unusable' DDL
                             FROM all_indexes
                            WHERE table_name = UPPER (p_table)
                              AND table_owner = UPPER (p_owner)
                              AND NOT REGEXP_LIKE (index_type, 'iot', 'i')
                              AND REGEXP_LIKE (index_type, '^' || p_index_type, 'i')
                              AND partitioned = 'NO'
                              AND status = 'VALID')
            LOOP
               IF p_debug
               THEN
                  o_app.log_msg ('Index DDL: ' || c_gidx.DDL);
               ELSE
                  EXECUTE IMMEDIATE c_gidx.DDL;
               END IF;

               l_cnt := l_cnt + 1;
            END LOOP;

            -- only give this message if there were indexes returned in the cursor
            IF l_cnt > 0
            THEN
               o_app.log_msg (l_cnt || ' global index(es) affected');
            END IF;
         ELSE
            NULL;
      END CASE;

      l_cnt := 0;
      o_app.set_action ('Evaluate indexes to work on');

      -- this cursor will contain all the ALTER INDEX statements necessary to mark indexes unusable
      -- the contents of the cursor depends very much on the parameters specified
      FOR c_idx IN (SELECT DISTINCT DDL
                               FROM (SELECT    'alter index '
                                            || owner
                                            || '.'
                                            || ai.index_name
                                            || DECODE (p_part_name,
                                                       NULL, NULL,
                                                       ' modify partition ' || p_part_name)
                                            || ' unusable' DDL,
                                            index_type,
                                            partition_name part_name
                                       FROM all_indexes ai LEFT OUTER JOIN all_ind_partitions aip
                                            ON ai.index_name = aip.index_name
                                          AND ai.owner = aip.index_owner
                                      WHERE table_name = UPPER (p_table)
                                        AND table_owner = UPPER (p_owner)
                                        AND (ai.status = 'VALID' OR aip.status = 'USABLE')) INDEXES
                              WHERE REGEXP_LIKE (index_type, '^' || p_index_type, 'i')
                                AND NOT REGEXP_LIKE (index_type, 'iot', 'i')
                                AND REGEXP_LIKE (NVL (part_name, '^'), '^' || p_part_name, 'i'))
      LOOP
         IF p_debug
         THEN
            o_app.log_msg ('Index DDL: ' || c_idx.DDL);
         ELSE
            EXECUTE IMMEDIATE c_idx.DDL;
         END IF;

         l_cnt := l_cnt + 1;
      END LOOP;

      o_app.log_msg (   l_cnt
                     || ' index(es) affected'
                     || CASE
                           WHEN p_part_name IS NULL
                              THEN NULL
                           ELSE ' for partition ' || p_part_name
                        END);
      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END unusable_indexes;

   -- Provides functionality for setting indexes to unusable based on the contents of another object
   -- This procedure uses an undocumented database function called tbl$or$idx$part$num.
   -- There are two "magic" numbers that are required to make it work correctly.
   -- The defaults will quite often work.
   -- The simpliest way to find which magic numbers make this function work is to
   -- do a partition exchange on the target table and trace that statement.
   PROCEDURE unusable_idx_src (
      p_owner        VARCHAR2,                          -- owner of table for the indexes to work on
      p_table        VARCHAR2,                                    -- table to operate on indexes for
      p_src_owner    VARCHAR2 DEFAULT NULL,
      -- owner of the object to use to evaluate indexes to make unusable
      p_src_obj      VARCHAR2 DEFAULT NULL,
      -- object to use to look for partitions to make unusable
      p_src_col      VARCHAR2 DEFAULT NULL,
      -- column to use to base comparision... defaults to partitioned column
      p_index_type   VARCHAR2 DEFAULT NULL,
      p_global       BOOLEAN DEFAULT FALSE,
      p_d_num        NUMBER DEFAULT 0,          -- first "magic" number in the undocumented function
      p_p_num        NUMBER DEFAULT 65535,     -- second "magic" number in the undocumented function
      p_debug        BOOLEAN DEFAULT FALSE)
   IS
      l_ddl         VARCHAR2 (2000);
      l_dyn_ddl     VARCHAR2 (2000);
      l_src_owner   all_tables.owner%TYPE
                                       := NVL (p_src_owner, REGEXP_REPLACE (p_owner, 'DW$', 'STG'));
      l_src_obj     all_objects.object_name%TYPE            := NVL (p_src_obj, p_table || '_STG');
      -- to catch empty cursors
      l_rows        BOOLEAN                                 := FALSE;
      l_src_col     all_part_key_columns.column_name%TYPE;
      l_cnt         NUMBER                                  := 0;

      -- type for doing a dynamic bulk collection
      TYPE parts_ttyp IS TABLE OF all_tab_partitions.partition_name%TYPE
         INDEX BY BINARY_INTEGER;

      tt_parts      parts_ttyp;
      l_app         app_info
                            := app_info (p_module      => 'DBFLEX.UNUSABLE_IDX_SRC',
                                         p_debug       => p_debug);
   BEGIN
      IF p_src_col IS NULL
      THEN
         SELECT column_name
           INTO l_src_col
           FROM all_part_key_columns
          WHERE NAME = UPPER (p_table) AND owner = UPPER (p_owner);
      ELSE
         l_src_col := p_src_col;
      END IF;

      o_app.log_msg (   'Evaluating '
                     || l_src_owner
                     || '.'
                     || l_src_obj
                     || ' to determine partitions on '
                     || p_owner
                     || '.'
                     || p_table
                     || ' to operate on');
      l_dyn_ddl :=
            'SELECT partition_name '
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
         || UPPER (l_src_col)
         || '")	 FROM '
         || UPPER (l_src_owner)
         || '.'
         || UPPER (l_src_obj)
         || ') '
         || 'ORDER By partition_position';

      IF p_debug
      THEN
         o_app.log_msg ('Dynamic Cursor: ' || l_dyn_ddl);
      END IF;

      EXECUTE IMMEDIATE l_dyn_ddl
      BULK COLLECT INTO tt_parts;

      FOR i IN 1 .. tt_parts.COUNT
      LOOP
         unusable_indexes (p_owner           => p_owner,
                           p_table           => p_table,
                           p_part_name       => tt_parts (i),
                           p_index_type      => p_index_type,
                           p_global          => p_global,
                           p_debug           => p_debug);
      END LOOP;

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END unusable_idx_src;

   -- rebuilds all unusable index segments on a particular table
   PROCEDURE usable_indexes (
      p_owner   VARCHAR2,                               -- owner of table for the indexes to work on
      p_table   VARCHAR2,                                         -- table to operate on indexes for
      p_debug   BOOLEAN DEFAULT FALSE)
   IS
      l_ddl    VARCHAR2 (2000);
      l_rows   BOOLEAN         := FALSE;                                  -- to catch empty cursors
      l_cnt    NUMBER          := 0;
      l_app    app_info
         := app_info (p_module      => 'DBFLEX.USABLE_INDEXES',
                      p_action      => 'Rebuild indexes',
                      p_debug       => p_debug);
   BEGIN
      IF NOT p_debug
      THEN
         o_app.log_msg ('Making unusable indexes on ' || p_owner || '.' || p_table || ' usable');
      END IF;

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
         l_rows := TRUE;

         IF p_debug
         THEN
            o_app.log_msg ('Partition DDL: ' || c_idx.DDL);
         ELSE
            EXECUTE IMMEDIATE c_idx.DDL;
         END IF;

         l_cnt := l_cnt + 1;
      END LOOP;

      IF l_cnt > 0
      THEN
         o_app.log_msg ('Any unusable local indexes on ' || l_cnt || ' table partitions rebuilt');
      END IF;

      l_cnt := 0;

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

         IF p_debug
         THEN
            o_app.log_msg ('Partition DDL: ' || c_gidx.DDL);
         ELSE
            EXECUTE IMMEDIATE c_gidx.DDL;
         END IF;

         l_cnt := l_cnt + 1;
      END LOOP;

      IF l_cnt > 0
      THEN
         o_app.log_msg (l_cnt || ' non-local index(es) affected');
      END IF;

      o_app.clear_app_info;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END usable_indexes;
END dbflex;
/