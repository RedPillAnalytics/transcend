CREATE OR REPLACE VIEW tdinc.extract_ot 
OF tdinc.extract
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
       filename,
       coreutils.get_dir_path (directory) || '/' || filename filepath,
       arch_directory,
       arch_filename,
       coreutils.get_dir_path (arch_directory) || '/' || arch_filename arch_filepath,
       min_bytes,
       max_bytes,
       CASE baseurl
       WHEN 'NA'
       THEN 'NA'
       ELSE 
       baseurl||'/'||filename 
       END file_url,
       'alter session set nls_date_format=''' || dateformat || '''' dateformat_ddl,
       'alter session set nls_timestamp_format=''' || timestampformat || '''' tsformat_ddl,
       delimiter,
       quotechar,
       headers
  FROM (SELECT notify_type,
	       notify_type_id,
	       filehub_id,
               filehub_name filehub_name,
               filehub_group filehub_group,
               filehub_type,
               object_owner,
               object_name,
               directory,
               CASE file_datestamp
               WHEN 'NA'
               THEN filename
               ELSE regexp_replace (filename,
                                     '\.',
                                     '_'
                                     || to_char (SYSDATE,
                                                  file_datestamp)
                                     || '.')
               END filename,
               CASE file_datestamp
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
               min_bytes,
               max_bytes,
               baseurl,
	       message,
	       subject,
               dateformat,
               timestampformat,
               delimiter,
               CASE
               WHEN quotechar = 'NA'
               THEN NULL
               WHEN quotechar IS NOT NULL
               THEN quotechar
               END quotechar,
               headers
          FROM tdinc.filehub_conf left JOIN tdinc.notify_conf
	       USING (notify_id)
	 WHERE REGEXP_LIKE (filehub_type, '^extract$', 'i'));