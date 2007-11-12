DROP TABLE notification_events CASCADE CONSTRAINTS purge
/

CREATE TABLE notification_events
       ( module              VARCHAR2(48) NOT NULL,
	 action    	     VARCHAR2(32) NOT NULL,
	 subject             VARCHAR2(100) NOT NULL,
	 message             VARCHAR2(2000) NOT NULL,
	 created_user        VARCHAR2(30) DEFAULT SYS_CONTEXT('USERENV','SESSION_USER') NOT NULL,
	 created_dt   	     DATE DEFAULT SYSDATE NOT NULL,
	 modified_user       VARCHAR2(30),
	 modified_dt         DATE
       )
/
ALTER TABLE notification_events ADD
      (
	CONSTRAINT notification_events_pk
	PRIMARY KEY
	( action, module )
	USING INDEX
      )
/