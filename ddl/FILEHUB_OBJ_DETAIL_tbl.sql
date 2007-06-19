DROP TABLE filehub_obj_detail purge
/

CREATE TABLE filehub_obj_detail
       ( filehub_obj_id NUMBER NOT NULL,
	 filehub_id NUMBER NOT NULL,
	 filehub_type VARCHAR2(7) NOT NULL,
	 filehub_name VARCHAR2(30) NOT NULL,
	 filehub_group VARCHAR2(50) NOT NULL,
	 object_owner VARCHAR2(30) NOT NULL,
	 object_name  VARCHAR2(30) NOT NULL,
	 processed_ts TIMESTAMP DEFAULT systimestamp NOT NULL,
	 num_rows NUMBER,
	 num_lines NUMBER,
	 percent_diff NUMBER,
	 session_id NUMBER DEFAULT sys_context('USERENV','SESSIONID') NOT NULL)
/

ALTER TABLE filehub_obj_detail ADD (
  CONSTRAINT filehub_obj_detail_pk
 PRIMARY KEY
 (filehub_obj_id)
    USING INDEX)
/


COMMENT ON TABLE filehub_obj_detail IS 'Detail information about external tables after File Mover runs.'
/

COMMENT ON COLUMN filehub_obj_detail.filehub_obj_id IS 'sequence generated pk';
COMMENT ON COLUMN filehub_obj_detail.filehub_id IS 'parent key from the filehub_conf table';
COMMENT ON COLUMN filehub_obj_detail.filehub_type IS '"feed" or "extract"';
COMMENT ON COLUMN filehub_obj_detail.object_name IS 'Name of the object.';
COMMENT ON COLUMN filehub_obj_detail.object_owner IS 'Owner of the object.';
COMMENT ON COLUMN filehub_obj_detail.filehub_group IS 'Name of the filehub_group from the FILEHUB_CONF table';
COMMENT ON COLUMN filehub_obj_detail.processed_ts IS 'Time the file was processed by FileHub.';
COMMENT ON COLUMN filehub_obj_detail.num_rows IS 'Result of a "SELECT COUNT(*)..." on the object.';
COMMENT ON COLUMN filehub_obj_detail.num_lines IS 'The number of lines at the OS level in the file or files defined for the filehub job.';
COMMENT ON COLUMN filehub_obj_detail.percent_diff IS 'Percentage difference between the two.';
COMMENT ON COLUMN filehub_obj_detail.session_id IS 'SESSION_ID of the processing session.';

GRANT SELECT ON filehub_obj_detail TO filehub_sel;
/

DROP SEQUENCE filehub_obj_detail_seq
/

CREATE SEQUENCE filehub_obj_detail_seq
/