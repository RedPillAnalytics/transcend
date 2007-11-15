DROP TABLE notification_conf CASCADE CONSTRAINTS purge
/

CREATE TABLE notification_conf
       ( label		   VARCHAR2(40) NOT NULL,
	 module        	   VARCHAR2(48) NOT NULL,
	 action        	   VARCHAR2(32) NOT NULL,
	 method      	   VARCHAR2(20) NOT NULL,
	 enabled     	   VARCHAR2(3) DEFAULT 'yes',
	 required	   VARCHAR2(3) DEFAULT 'no',
	 sender            VARCHAR2(1024),
	 recipients        VARCHAR2(2000) NOT NULL,
	 created_user      VARCHAR2(30) DEFAULT SYS_CONTEXT('USERENV','SESSION_USER') NOT NULL,
	 created_dt   	   DATE DEFAULT SYSDATE NOT NULL,
	 modified_user     VARCHAR2(30),
	 modified_dt       DATE
       )
/

ALTER TABLE notification_conf ADD
      (
	CONSTRAINT notification_conf_pk
	PRIMARY KEY
	( label,module,action )
	USING INDEX
      )
/
ALTER TABLE notification_conf
      ADD (
       CONSTRAINT notification_conf_fk1
       FOREIGN KEY ( module, action )
       REFERENCES notification_events
       ( module, action )
     )
/

ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck1 CHECK (module=lower(module));
ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck2 CHECK (action=lower(action));
ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck3 CHECK (method=lower(method));
ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck4 CHECK (enabled=lower(enabled));
ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck5 CHECK (required=lower(required));
ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck6 CHECK (sender=lower(sender));
ALTER TABLE notification_conf ADD CONSTRAINT notification_conf_ck7 CHECK (recipients=lower(recipients));

