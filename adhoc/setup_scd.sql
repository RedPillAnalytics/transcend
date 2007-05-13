DROP TABLE test_dim
/

CREATE TABLE test_dim
       ( test_key NUMBER,
         nat_key NUMBER,
         birthdate DATE,
	 name VARCHAR2(50),
	 zip NUMBER,
         effect_start_dt DATE,
         effect_end_dt DATE,
         current_ind VARCHAR2(1)
       )
/

DROP TABLE test_stg
/

CREATE TABLE test_stg
       ( nat_key NUMBER,
         birthdate DATE,
	 name VARCHAR2(50),
	 zip NUMBER,
         effect_start_dt DATE
       )
/

DROP SEQUENCE test_dim_seq
/
CREATE SEQUENCE test_dim_seq START WITH 21
/

DROP SEQUENCE test_natkey_seq
/
CREATE SEQUENCE test_natkey_seq START WITH 1033477
/

INSERT INTO test_dim VALUES (test_dim_seq.nextval, test_natkey_seq.nextval, '02/03/1972','Stewart A. Bryson',30328,to_date('01/01/2006','mm/dd/yyyy'),to_date('01/31/2006','mm/dd/yyyy'),'N')
/

INSERT INTO test_dim VALUES (test_dim_seq.nextval, test_natkey_seq.currval, '02/03/1972','Stewart B. Bryson',30075,to_date('01/31/2006','mm/dd/yyyy'),to_date('02/28/2006','mm/dd/yyyy'),'N')
/

INSERT INTO test_dim VALUES (test_dim_seq.nextval, test_natkey_seq.currval, '02/03/1972','Stewart C. Bryson',30075,to_date('02/28/2006','mm/dd/yyyy'),to_date('12/31/9999','mm/dd/yyyy'),'Y')
/

INSERT INTO test_stg VALUES (test_natkey_seq.currval, '02/04/1972', 'Stewart D. Bryson', 30066, to_date('03/04/2006','mm/dd/yyyy'))
/

INSERT INTO test_stg VALUES (test_natkey_seq.currval, '02/04/1972', 'Stewart W. Bryson', 30066, to_date('03/09/2006','mm/dd/yyyy'))
/

INSERT INTO test_dim VALUES (test_dim_seq.nextval, test_natkey_seq.nextval, '07/26/1975','Pamela S. Bryson',30328,to_date('01/01/2006','mm/dd/yyyy'),to_date('12/31/9999','mm/dd/yyyy'),'Y')
/

INSERT INTO test_stg VALUES (test_natkey_seq.currval, '06/26/1975','Pamela E. Bryson',30075,to_date('03/04/2006','mm/dd/yyyy'))
/

INSERT INTO test_stg VALUES (test_natkey_seq.currval, '06/26/1975','Pamela E. Bryson',30066,SYSDATE)
/

SELECT * FROM test_dim
/

SELECT * FROM test_stg
/