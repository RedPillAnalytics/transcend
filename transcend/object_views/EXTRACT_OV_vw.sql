CREATE OR REPLACE VIEW extract_ov
OF extract_ot
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
       td_utils.get_dir_path (directory) || '/' || filename filepath,
       arch_directory,
       arch_dirpath,
       arch_filename,
       td_utils.get_dir_path (arch_directory) || '/' || arch_filename arch_filepath,
       file_datestamp,
       min_bytes,
       max_bytes,
       baseurl,
       CASE baseurl
       WHEN null
       THEN null
       ELSE 
       baseurl||'/'||filename 
       END file_url,
       passphrase,
       'alter session set nls_date_format=''' || dateformat || '''' dateformat_ddl,
       'alter session set nls_timestamp_format=''' || timestampformat || '''' tsformat_ddl,
       delimiter,
       quotechar,
       headers
  FROM (SELECT file_label,
               file_group,
               file_type,
               object_owner,
               object_name,
               directory,
	       td_utils.get_dir_path (directory) dirpath,
               CASE nvl(file_datestamp,'NA')
               WHEN 'NA'
               THEN filename
               ELSE regexp_replace (filename,
                                     '\.',
                                     '_'
                                     || to_char (SYSDATE,
                                                  file_datestamp)
                                     || '.')
               END filename,
	       CASE nvl(file_datestamp,'NA')
	       WHEN 'NA'
               THEN  filename
               || '.'
               || to_char (SYSDATE, 'yyyymmddhhmiss')
               ELSE regexp_replace (filename,
                                     '\.',
                                     '_'
                                     || to_char (SYSDATE,
                                                  file_datestamp)
                                     || '.')
               END arch_filename,
               arch_directory,
	       td_utils.get_dir_path (arch_directory) arch_dirpath,
	       file_datestamp,
               min_bytes,
               max_bytes,
               baseurl,
	       passphrase,
               dateformat,
               timestampformat,
               delimiter,
               quotechar,
               headers
          FROM files_conf
	 WHERE REGEXP_LIKE (file_type, '^extract$', 'i'));