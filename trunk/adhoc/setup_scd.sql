DROP TABLE test_dim purge
/

CREATE TABLE test_dim
       ( test_key NUMBER,
         nat_key NUMBER,
         birthdate DATE,
	 name VARCHAR2(50),
	 zip NUMBER,
	 zip_plus4 NUMBER,
         effect_start_dt DATE,
         effect_end_dt DATE,
         current_ind VARCHAR2(1)
       )
/

DROP TABLE test_stg purge
/

CREATE TABLE test_stg
       ( nat_key NUMBER,
         birthdate DATE,
	 name VARCHAR2(50),
	 zip NUMBER,
	 zip_plus4 NUMBER,
         effect_start_dt DATE
       )
/

DROP SEQUENCE test_dim_seq
/
CREATE SEQUENCE test_dim_seq START WITH 21
/

INSERT INTO test_dim VALUES 
       (test_dim_seq.nextval, 1033477, '01/03/1975','John Smith',30328,NULL,
	 to_date('01/01/2006','mm/dd/yyyy'),to_date('01/31/2006','mm/dd/yyyy'),'N')
/

INSERT INTO test_dim VALUES 
       (test_dim_seq.nextval, 1033477, '01/03/1975','John A. Smith',30075,NULL,
	 to_date('01/31/2006','mm/dd/yyyy'),to_date('02/28/2006','mm/dd/yyyy'),'N')
/

INSERT INTO test_dim VALUES 
       (test_dim_seq.nextval, 1033477, '01/03/1975','John D. Smith',30075,NULL,
	 to_date('02/28/2006','mm/dd/yyyy'),to_date('12/31/9999','mm/dd/yyyy'),'Y')
/

INSERT INTO test_stg VALUES 
       (1033477, '01/03/1975', 'John H. Smith', 30066,NULL,
	 to_date('03/04/2006','mm/dd/yyyy'))
/

INSERT INTO test_stg VALUES 
       (1033477, '01/04/1975', 'John H. Smith', 30066,3333,
	 to_date('03/10/2006','mm/dd/yyyy'))
/


INSERT INTO test_dim VALUES 
       (test_dim_seq.nextval, 1039242, '07/26/1975','Jane S. Smith',30328,NULL,
	 to_date('01/01/2006','mm/dd/yyyy'),to_date('12/31/9999','mm/dd/yyyy'),'Y')
/

INSERT INTO test_stg VALUES 
       (1039242, '09/26/1980','Jane B. Smith',NULL,null,
	 to_date('03/04/2006','mm/dd/yyyy'))
/

INSERT INTO test_stg VALUES 
       (1039242, '09/26/1980','Jane B. Smith',NULL,NULL,SYSDATE)
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

COMMIT;