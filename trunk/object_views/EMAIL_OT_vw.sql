CREATE OR REPLACE VIEW email_ot 
OF emailtype
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
  FROM notify_conf;