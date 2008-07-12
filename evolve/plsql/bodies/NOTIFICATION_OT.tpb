CREATE OR REPLACE TYPE BODY notification_ot
AS
   CONSTRUCTOR FUNCTION notification_ot( p_label VARCHAR2 )
      RETURN SELF AS RESULT
   AS
   BEGIN
      evolve.log_msg( 'Notification module: ' || module, 5 );
      evolve.log_msg( 'Notification action: ' || action, 5 );
      evolve.print_query(    'SELECT label,'
                              || 'method,'
                              || 'enabled,'
                              || 'required,'
                              || 'subject,'
                              || 'message,'
                              || 'sender,'
                              || 'recipients '
                              || 'FROM notification_conf '
                              || 'JOIN notification_events '
                              || 'USING ( module,action ) '
                              || 'WHERE lower(module) = lower('''
                              || td_inst.module
                              || ''') AND lower(action) = lower('''
                              || td_inst.action
                              || ''') AND lower(label) = lower('''
                              || p_label
                              || ''') '
                            );

      BEGIN
         SELECT label, method, enabled, required, subject, MESSAGE, sender, recipients
           INTO label, method, enabled, required, subject, MESSAGE, sender, recipients
           FROM notification_conf JOIN notification_events USING( module, action )
          WHERE LOWER( module ) = LOWER( td_inst.module )
            AND LOWER( action ) = LOWER( td_inst.action )
            AND LOWER( label ) = LOWER( p_label );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            evolve.log_msg(    'No notification configured for label "'
                                || p_label
                                || '" with module "'
                                || td_inst.module
                                || '" and action "'
                                || td_inst.action
                                || '"',
                                4
                              );
         WHEN TOO_MANY_ROWS
         THEN
            evolve.raise_err( 'notify_err',
                                     'label "'
                                  || p_label
                                  || '" with module "'
                                  || td_inst.module
                                  || '" and action "'
                                  || td_inst.action
                                  || '"'
                                );
      END;

      RETURN;
   END;
   MEMBER PROCEDURE send( p_message VARCHAR2 DEFAULT NULL )
   AS
      e_smtp_error1   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_smtp_error1, -29278 );
      e_smtp_error2   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_smtp_error2, -29269 );
      e_smtp_error3   EXCEPTION;
      PRAGMA EXCEPTION_INIT( e_smtp_error3, -29261 );
   BEGIN
      evolve.log_msg( 'Value for ENABLED: ' || enabled, 5 );

      IF td_core.is_true( enabled, TRUE )
      THEN
         IF NOT evolve.is_debugmode
         THEN
            CASE method
               WHEN 'email'
               THEN
                  BEGIN
                     UTL_MAIL.send( sender          => sender,
                                    recipients      => recipients,
                                    subject         => subject,
                                    MESSAGE         => NVL( p_message, MESSAGE ),
                                    mime_type       => 'text/html'
                                  );
                     evolve.log_msg( 'Email sent to: ' || recipients );
                     evolve.log_msg(    'Email Information:'
                                         || CHR( 10 )
                                         || 'Sender: '
                                         || sender
                                         || CHR( 10 )
                                         || 'Recipient: '
                                         || recipients
                                         || CHR( 10 )
                                         || 'Subject: '
                                         || subject
                                         || CHR( 10 )
                                         || 'Message: '
                                         || p_message,
                                         5
                                       );
                  EXCEPTION
                     WHEN e_smtp_error1 OR e_smtp_error2 OR e_smtp_error3
                     THEN
                        IF td_core.is_true( required )
                        THEN
                           evolve.raise_err( 'utl_mail_err', SQLERRM );
                        ELSE
                           evolve.log_msg( 'The following SMTP error occured: ' || SQLERRM );
                        END IF;
                  END;
               ELSE
                  evolve.raise_err( 'notify_method_invalid' );
            END CASE;
         END IF;
      END IF;
   END send;
END;
/

SHOW errors