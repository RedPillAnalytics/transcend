CREATE OR REPLACE VIEW feed_ov
OF feed_ot
WITH object identifier (file_label,file_group)
as
SELECT file_label,
       file_group,
       file_type,
       object_owner,
       object_name,
       directory,
       dirpath,
       filename,
       dirpath || '/' || filename filepath,
       arch_directory,
       arch_dirpath,
       NULL arch_filename,
       null arch_filepath,
       file_datestamp,
       min_bytes,
       max_bytes,
       baseurl,
       NULL file_url,
       passphrase,
       source_directory,
       source_dirpath,
       source_regexp,
       regexp_options,
       source_policy,
       required,
       delete_source,
       reject_limit
  FROM (SELECT file_label,
               file_group,
               file_type,
               object_owner,
               object_name,
               directory,
	       td_utils.get_dir_path (directory) dirpath,
               filename,
	       arch_directory,
	       td_utils.get_dir_path (arch_directory) arch_dirpath,
	       file_datestamp,
               min_bytes,
               max_bytes,
               baseurl,
	       passphrase,
	       source_directory,
	       td_utils.get_dir_path (source_directory) source_dirpath,
	       source_regexp,
	       regexp_options,
	       source_policy,
	       required,
	       delete_source,
	       reject_limit
          FROM files_conf
	 WHERE REGEXP_LIKE (file_type, '^feed$', 'i'));