#!/bin/bash

# create the evolve_unwrapped.zip file
zip -r ./evolve_unwrapped.zip ./plsql/specs/*.*s ./plsql/bodies/*.*b  ./install/install_evolve.sql ./evolve/java/* ./evolve/plsql/specs/*.*s ./evolve/plsql/bodies/*.*b ./evolve/plsql/functions/*.fnc

# create the evolve.zip file
zip -r ./evolve.zip ./plsql/specs/*.*s ./plsql/wrapped_bodies/*plb  ./install/install_evolve.sql ./evolve/java/* ./evolve/plsql/specs/*.*s ./evolve/plsql/wrapped_bodies/*plb

# create the transcend_unwrapped.zip file
zip -r ./transcend_unwrapped.zip ./plsql/specs/*.*s ./plsql/bodies/*.*b ./evolve/java/* ./evolve/plsql/specs/*.*s ./evolve/plsql/bodies/*.*b ./evolve/plsql/functions/*.fnc
zip -r ./transcend_unwrapped.zip ./install/install_transcend.sql ./install/upgrade_transcend.sql ./transcend/plsql/specs/*.*s ./transcend/plsql/bodies/*.*b

# create the transcend.zip file
zip -r ./transcend.zip ./plsql/specs/*.*s ./plsql/wrapped_bodies/*plb ./evolve/java/* ./evolve/plsql/specs/*.*s ./evolve/plsql/wrapped_bodies/*plb
zip -r ./transcend.zip ./install/install_transcend.sql ./install/upgrade_transcend.sql ./transcend/plsql/specs/*.*s ./transcend/plsql/wrapped_bodies/*plb
