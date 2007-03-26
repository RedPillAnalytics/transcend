CREATE OR REPLACE TYPE BODY tdinc.notify
AS
   -- GET method for DEBUG mode
   MEMBER FUNCTION DEBUG_MODE
      RETURN BOOLEAN
   AS
   BEGIN
      RETURN CASE DEBUG
         WHEN 'Y'
            THEN TRUE
         ELSE FALSE
      END;
   END DEBUG_MODE;
   -- SET method for DEBUG mode
   MEMBER PROCEDURE DEBUG_MODE (p_debug BOOLEAN DEFAULT FALSE)
   AS
   BEGIN
      DEBUG := CASE p_debug
                 WHEN TRUE
                    THEN 'Y'
                 ELSE 'F'
              END;
   END DEBUG_MODE;
   MEMBER PROCEDURE send
   AS
      l_recipients   VARCHAR2 (2000);
      l_sender       VARCHAR2 (50);
      o_app          applog      := applog (p_module      => 'notify.email',
                                            p_debug       => SELF.DEBUG_MODE);
   BEGIN
      CASE NVL (notify_type, 'NA')
         WHEN 'email'
         THEN
            SELECT recipients,
                   sender
              INTO l_recipients,
                   l_sender
              FROM email_notify_conf
             WHERE email_notify_id = notify_type_id;

            IF SELF.DEBUG_MODE
            THEN
               o_app.log_msg (   'Email Information:'
                              || CHR (10)
                              || 'Sender: '
                              || l_sender
                              || CHR (10)
                              || 'Recipient: '
                              || l_recipients
                              || CHR (10)
                              || 'Subject: '
                              || subject
                              || CHR (10)
                              || 'Message: '
                              || MESSAGE);
            ELSE
               UTL_MAIL.send (sender          => l_sender,
                              recipients      => l_recipients,
                              subject         => subject,
                              MESSAGE         => MESSAGE,
                              mime_type       => 'text/html');
               o_app.log_msg ('Email sent to: ' || l_recipients);
            END IF;
         WHEN 'NA'
         THEN
            o_app.log_msg ('Notification not configured');
         ELSE
            raise_application_error (o_app.get_err_cd ('notify_method_invalid'),
                                     o_app.get_err_msg ('notify_method_invalid'));
      END CASE;
   EXCEPTION
      WHEN OTHERS
      THEN
         o_app.log_err;
         RAISE;
   END send;
END;
/