
COL name format a20
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

INSERT INTO test_dim VALUES 
       (test_dim_seq.nextval, test_natkey_seq.nextval, '01/03/1975','John Smith',30328,
	 to_date('01/01/2006','mm/dd/yyyy'),to_date('01/31/2006','mm/dd/yyyy'),'N')
/

INSERT INTO test_dim VALUES 
       (test_dim_seq.nextval, test_natkey_seq.currval, '01/03/1975','John A. Smith',30075,
	 to_date('01/31/2006','mm/dd/yyyy'),to_date('02/28/2006','mm/dd/yyyy'),'N')
/

INSERT INTO test_dim VALUES 
       (test_dim_seq.nextval, test_natkey_seq.currval, '01/03/1975','John D. Smith',30075,
	 to_date('02/28/2006','mm/dd/yyyy'),to_date('12/31/9999','mm/dd/yyyy'),'Y')
/

INSERT INTO test_stg VALUES 
       (test_natkey_seq.currval, '01/03/1975', 'John H. Smith', 30066, 
	 to_date('03/04/2006','mm/dd/yyyy'))
/

INSERT INTO test_stg VALUES 
       (test_natkey_seq.currval, '01/04/1975', 'John H. Smith', 30066, 
	 to_date('03/09/2006','mm/dd/yyyy'))
/

INSERT INTO test_dim VALUES 
       (test_dim_seq.nextval, test_natkey_seq.nextval, '07/26/1975','Jane S. Smith',30328,
	 to_date('01/01/2006','mm/dd/yyyy'),to_date('12/31/9999','mm/dd/yyyy'),'Y')
/

INSERT INTO test_stg VALUES 
       (test_natkey_seq.currval, '09/26/1980','Jane B. Smtih',30075,
	 to_date('03/04/2006','mm/dd/yyyy'))
/

INSERT INTO test_stg VALUES 
(test_natkey_seq.currval, '08/26/1980','Jane W. Smith',30066,SYSDATE)
/

SELECT * FROM test_dim
/

SELECT * FROM test_stg
/

DROP TABLE test_fact
/

CREATE TABLE test_fact
       ( test_key NUMBER,
	 quantity NUMBER,
	 amount NUMBER)
/

INSERT INTO test_fact VALUES (21, 4, 37.49);
INSERT INTO test_fact VALUES (21, 5, 50.99);
INSERT INTO test_fact VALUES (22, 3, 22.14);
INSERT INTO test_fact VALUES (23, 12, 117.55);

SELECT * FROM test_fact;