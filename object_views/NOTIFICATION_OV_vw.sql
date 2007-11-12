CREATE OR REPLACE VIEW notification_ov
OF notification_ot
WITH object identifier (notification_label, module, action)
as
SELECT notification_label,
       module,
       action,
       notification_method,
       notification_enabled,
       notification_required,
       subject,
       message,
       sender,
       recipients
  FROM notification_conf
  JOIN notification_events
       USING (module,action)
/