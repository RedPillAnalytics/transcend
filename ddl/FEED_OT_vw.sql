CREATE OR REPLACE VIEW tdinc.feed_ot 
OF tdinc.feed
WITH object identifier (filehub_id)
as
SELECT notify_type,
       notify_type_id, 
       message,
       subject,
       cast('N' AS VARCHAR2(1)) debug,
       filehub_id,
       filehub_name,
       filehub_group,
       filehub_type,
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
       min_bytes,
       max_bytes,
       CASE baseurl
       WHEN 'NA'
       THEN 'NA'
       ELSE 
       baseurl||'/'||filename 
       END file_url,
       passphrase,
       source_directory,
       source_dirpath,
       source_regexp,
       regexp_options,
       source_policy,
       required
  FROM (SELECT notify_type,
	       notify_type_id,
	       filehub_id,
               filehub_name filehub_name,
               filehub_group filehub_group,
               filehub_type,
               object_owner,
               object_name,
               directory,
	       coreutils.get_dir_path (directory) dirpath,
               filename,
	       arch_directory,
	       coreutils.get_dir_path (arch_directory) arch_dirpath,
               min_bytes,
               max_bytes,
               baseurl,
	       passphrase,
	       message,
	       subject,
	       source_directory,
	       coreutils.get_dir_path (source_directory) source_dirpath,
	       source_regexp,
	       regexp_options,
	       source_policy,
	       required
          FROM tdinc.filehub_conf left JOIN tdinc.notify_conf
	       USING (notify_id)
	 WHERE REGEXP_LIKE (filehub_type, '^feed$', 'i'));