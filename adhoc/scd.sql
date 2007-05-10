DROP TABLE test_dim
/

CREATE TABLE TEST_DIM
   (    TEST_KEY NUMBER,
        NAT_KEY NUMBER,
        BIRTHDATE DATE,
        NAME VARCHAR2(50),
        EFFECTIVE_START_DT DATE,
        EFFECTIVE_END_DT DATE,
        CURRENT_IND VARCHAR2(1)
   )
/

DROP TABLE test_stg
/

CREATE TABLE TEST_STG
   (    NAT_KEY NUMBER,
        BIRTHDATE DATE,
        NAME VARCHAR2(50),
        EFFECTIVE_START_DT DATE
   )
/

DROP SEQUENCE test_dim_seq
/
CREATE SEQUENCE TEST_DIM_SEQ START WITH 21
/

DROP SEQUENCE test_natkey_seq
/
CREATE SEQUENCE TEST_NATKEY_SEQ START WITH 1033477
/

INSERT INTO test_dim VALUES (test_dim_seq.nextval, test_natkey_seq.nextval, '02/03/1972','Stewart A. Bryson',to_date('01/01/2006','mm/dd/yyyy'),to_date('01/31/2006','mm/dd/yyyy'),'N')
/

INSERT INTO test_dim VALUES (test_dim_seq.nextval, test_natkey_seq.currval, '02/03/1972','Stewart B. Bryson',to_date('02/01/2006','mm/dd/yyyy'),to_date('02/28/2006','mm/dd/yyyy'),'N')
/

INSERT INTO test_dim VALUES (test_dim_seq.nextval, test_natkey_seq.currval, '02/03/1972','Stewart C. Bryson',to_date('03/01/2006','mm/dd/yyyy'),to_date('03/30/2006','mm/dd/yyyy'),'Y')
/

INSERT INTO test_stg VALUES (test_natkey_seq.currval, '02/04/1972','Stewart W. Bryson',sysdate)
/
SELECT * FROM TEST_DIM
/
-- SELECT CASE test_key WHEN -1 THEN test_dim_seq.nextval ELSE test_key END test_key,
--        nat_key,
--        birthdate,
--        name,
--        new_current_ind current_ind
--   FROM ( SELECT *
--            FROM (SELECT nat_key,
--                         'S' source,
--                         'N' current_ind,
--                         'Y' new_current_ind,
--                         -1 test_key,
--                         birthdate,
--                         name,
--                         'Y' include
--                    FROM test_stg
--                         UNION
--                  SELECT nat_key,
--                         'D' source,
--                         current_ind,
--                         current_ind new_current_ind,
--                         test_key,
--                         birthdate,
--                         name,
--                         'Y' include
--                    FROM test_dim)
--           MODEL partition BY ( nat_key)
--                 dimension BY ( source,
--                                current_ind)
--                 measures ( test_key,
--                            birthdate,
--                            name,
--                            include,
--                            new_current_ind)
--                 UNIQUE single reference
--                 rules ( -- SET the include flag to 'Y' or 'N' to determine whether to include the row from STG table
--                         include['S','N'] = CASE WHEN name['S','N'] = name['D','Y'] THEN 'N' ELSE 'Y' END,
--                         -- SET the CURRENT_IND flag of the current record to 'Y' or 'N'
--                         new_current_ind['D','Y'] = CASE WHEN name['S','N'] = name['D','Y'] THEN 'Y' ELSE 'N' END,
--                         -- SET the TYPE 1 attribute to the new value for all records
--                         birthdate['D',ANY] = birthdate['S','N']
--                       )
--                 ORDER BY source,name)
--  WHERE include='Y'
-- /
