CREATE OR REPLACE VIEW tdinc.email_ot 
OF tdinc.email
WITH object identifier (notify_id)
as
SELECT cast('runtime' AS VARCHAR2(10)) runmode,
       notify_id,
       notify_enabled,
       action,
       module,
       module_id,
       message,
       subject,
       sender,
       recipients
  FROM tdinc.notify_conf;