CREATE OR REPLACE VIEW tdinc.notify_ot 
OF tdinc.notify
WITH object identifier (notify_id)
as
SELECT cast('runtime' AS VARCHAR2(10)) runmode,
       notify_id,
       notify_method,
       notify_enabled,
       action,
       module,
       module_id,
       message,
       subject
  FROM tdinc.notify_conf;