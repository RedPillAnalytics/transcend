CREATE OR REPLACE PACKAGE efw.file_adm
IS
PROCEDURE audit_file 
      ( p_file_proc_id file_process_dtl.file_process_id%type,
	p_src_filename file_process_dtl.src_filename%type,
	p_arch_filename file_process_dtl.arch_filename%type,
	p_num_bytes file_process_dtl.num_bytes%type,
	p_file_dt file_process_dtl.file_dt%type,
	p_process_type file_process_dtl.file_process_type%type,
	p_debug BOOLEAN DEFAULT FALSE );
END file_adm;
/