CREATE OR REPLACE TYPE BODY notification_ot
AS
   CONSTRUCTOR FUNCTION notification_ot( p_label VARCHAR2 )
      RETURN SELF AS RESULT
   AS
   BEGIN
      evolve_log.log_msg( 'Notification module: ' || module, 5 );
      evolve_log.log_msg( 'Notification action: ' || action, 5 );
      td_utils.print_query(    'SELECT label,'
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
            evolve_log.log_msg(    'No notification configured for label "'
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
            evolve_log.raise_err( 'notify_err',
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
      IF td_core.is_true( enabled )
      THEN
         IF NOT evolve_log.is_debugmode
         THEN
            CASE method
               WHEN 'email'
               THEN
                  evolve_log.log_msg(    'Email Information:'
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

                  BEGIN
                     -- instead of sitting and waiting for the email server to respond, we will submit them in the background
                     evolve_app.submit_mail( p_sender          => sender,
                                             p_recipients      => recipients,
                                             p_subject         => subject,
                                             p_message         => NVL( p_message, MESSAGE ),
                                             p_mime_type       => 'text/html'
                                           );
                     evolve_log.log_msg( 'Email sent to: ' || recipients, 3 );

                     IF td_core.is_true( required )
                     THEN
                        evolve_app.coordinate_sql;
                     END IF;
                  END;
               ELSE
                  evolve_log.raise_err( 'notify_method_invalid' );
            END CASE;
         END IF;
      END IF;
   END send;
END;
/

SHOW errors