DROP TABLE efw.log_table
/

CREATE TABLE efw.log_table
       ( entry_ts TIMESTAMP (6) NOT NULL,
	 session_id NUMBER NOT NULL,
	 current_scn NUMBER NOT NULL,
	 instance_name VARCHAR2(30) NOT NULL,
	 msg VARCHAR2(2000) NOT NULL,
	 code NUMBER NOT NULL,
	 client_info VARCHAR2(64) NOT NULL,
	 module VARCHAR2(48) NOT NULL,
	 action VARCHAR2(32) NOT NULL,
	 call_stack VARCHAR2(1024),
	 back_trace VARCHAR2(1024)
       ) TABLESPACE efw
/
ALTER TABLE efw.log_table ADD CONSTRAINT log_msg_pk PRIMARY KEY (instance_name,session_id, entry_ts)
      USING INDEX 
      TABLESPACE efw  ENABLE
/

GRANT SELECT ON efw.log_table TO efw_job
/