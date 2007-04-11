DROP TABLE tdinc.log_table
/

CREATE TABLE tdinc.log_table
       ( entry_ts TIMESTAMP (6) DEFAULT systimestamp NOT NULL,
	 msg VARCHAR2(2000) NOT NULL,
	 client_info VARCHAR2(64) NOT NULL,
	 module VARCHAR2(48) NOT NULL,
	 action VARCHAR2(32) NOT NULL,
	 runmode VARCHAR2(10) NOT NULL,
	 session_id NUMBER NOT NULL,
	 current_scn NUMBER NOT NULL,
	 instance_name VARCHAR2(30) NOT NULL,
	 machine VARCHAR2(100) NOT NULL,
	 dbuser VARCHAR2(30) NOT NULL,
	 osuser VARCHAR2(30) NOT NULL,
	 code NUMBER NOT NULL,
	 call_stack VARCHAR2(1024),
	 back_trace VARCHAR2(1024)
       ) TABLESPACE tdinc
/

GRANT SELECT ON tdinc.log_table TO tdinc_applog
/