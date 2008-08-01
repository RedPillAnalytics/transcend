-- ticket:92
UPDATE runmode_conf SET module=evolve_adm.all_modules WHERE module='default';
UPDATE registration_conf SET module=evolve_adm.all_modules WHERE module='default';
UPDATE logging_conf SET module=evolve_adm.all_modules WHERE module='default';