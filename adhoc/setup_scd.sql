DROP TABLE test_dim
/

CREATE TABLE TEST_DIM
   (    TEST_KEY NUMBER,
        NAT_KEY NUMBER,
        BIRTHDATE DATE,
        NAME VARCHAR2(50),
        EFFECT_START_DT DATE,
        EFFECT_END_DT DATE,
        CURRENT_IND VARCHAR2(1)
   )
/

DROP TABLE test_stg
/

CREATE TABLE TEST_STG
   (    NAT_KEY NUMBER,
        BIRTHDATE DATE,
        NAME VARCHAR2(50),
        EFFECT_START_DT DATE
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

INSERT INTO test_dim VALUES (test_dim_seq.nextval, test_natkey_seq.currval, '02/03/1972','Stewart B. Bryson',to_date('01/31/2006','mm/dd/yyyy'),to_date('02/28/2006','mm/dd/yyyy'),'N')
/

INSERT INTO test_dim VALUES (test_dim_seq.nextval, test_natkey_seq.currval, '02/03/1972','Stewart C. Bryson',to_date('02/28/2006','mm/dd/yyyy'),to_date('12/31/9999','mm/dd/yyyy'),'Y')
/

INSERT INTO test_stg VALUES (test_natkey_seq.currval, '02/04/1972','Stewart W. Bryson',sysdate)
/
SELECT * FROM TEST_DIM
/
