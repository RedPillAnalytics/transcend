CREATE OR REPLACE VIEW notification_ov
OF notification_ot
WITH object identifier ( label, module, action )
as
SELECT label,
       module,
       action,
       method,
       enabled,
       required,
       subject,
       message,
       sender,
       recipients
  FROM notification_conf
  JOIN notification_events
       USING ( module,action )
/