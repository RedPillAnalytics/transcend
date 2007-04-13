CREATE OR REPLACE VIEW tdinc.extract_ot 
OF tdinc.extract
WITH object identifier (filehub_id)
as
SELECT cast('runtime' AS VARCHAR2(10)) runmode,
       filehub_id,
       filehub_name,
       filehub_group,
       filehub_type,
       object_owner,
       object_name,
       directory,
       dirpath,
       filename,
       coreutils.get_dir_path (directory) || '/' || filename filepath,
       arch_directory,
       arch_dirpath,
       arch_filename,
       coreutils.get_dir_path (arch_directory) || '/' || arch_filename arch_filepath,
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
  FROM (SELECT filehub_id,
               filehub_name filehub_name,
               filehub_group filehub_group,
               filehub_type,
               object_owner,
               object_name,
               directory,
	       coreutils.get_dir_path (directory) dirpath,
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
	       coreutils.get_dir_path (arch_directory) arch_dirpath,
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
          FROM tdinc.filehub_conf
	 WHERE REGEXP_LIKE (filehub_type, '^extract$', 'i'));