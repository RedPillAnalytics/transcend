CREATE OR REPLACE VIEW email_ov
OF emailtype
WITH object identifier (notify_id)
as
SELECT notify_id,
       notify_enabled,
       action,
       module,
       module_id,
       message,
       subject,
       sender,
       recipients
  FROM notify_conf;