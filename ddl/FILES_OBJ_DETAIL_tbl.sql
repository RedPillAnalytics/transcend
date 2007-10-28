DROP TABLE files_obj_detail purge
/

CREATE TABLE files_obj_detail
       ( file_obj_detail_id NUMBER NOT NULL,
	 file_label VARCHAR2(30) NOT NULL,
	 file_group VARCHAR2(50) NOT NULL,
	 file_type VARCHAR2(7) NOT NULL,
	 object_owner VARCHAR2(30) NOT NULL,
	 object_name  VARCHAR2(30) NOT NULL,
	 processed_ts TIMESTAMP DEFAULT systimestamp NOT NULL,
	 num_rows NUMBER,
	 num_lines NUMBER,
	 percent_diff NUMBER,
	 session_id NUMBER DEFAULT sys_context('USERENV','SESSIONID') NOT NULL)
/

ALTER TABLE files_obj_detail ADD (
  CONSTRAINT files_obj_detail_pk
 PRIMARY KEY
 (file_obj_detail_id)
    USING INDEX)
/


COMMENT ON TABLE files_obj_detail IS 'Detail information about external tables after File Mover runs.'
/

COMMENT ON COLUMN files_obj_detail.file_obj_detail_id IS 'sequence generated pk';
COMMENT ON COLUMN files_obj_detail.file_label IS 'the unique label of the file process';
COMMENT ON COLUMN files_obj_detail.file_type IS '"feed" or "extract"';
COMMENT ON COLUMN files_obj_detail.object_name IS 'Name of the object.';
COMMENT ON COLUMN files_obj_detail.object_owner IS 'Owner of the object.';
COMMENT ON COLUMN files_obj_detail.file_group IS 'Name of the file_group from the FILES_CONF table';
COMMENT ON COLUMN files_obj_detail.processed_ts IS 'Time the file was processed.';
COMMENT ON COLUMN files_obj_detail.num_rows IS 'Result of a "SELECT COUNT(*)..." on the object.';
COMMENT ON COLUMN files_obj_detail.num_lines IS 'The number of lines at the OS level in the file or files defined for this file_label.';
COMMENT ON COLUMN files_obj_detail.percent_diff IS 'Percentage difference between the two.';
COMMENT ON COLUMN files_obj_detail.session_id IS 'SESSION_ID of the processing session.';

DROP SEQUENCE files_obj_detail_seq
/

CREATE SEQUENCE files_obj_detail_seq
/
