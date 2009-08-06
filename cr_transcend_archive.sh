#!/bin/bash

zip -r ./transcend.zip ./plsql/specs/*.*s ./plsql/bodies/*.*b ./evolve/java/* ./evolve/plsql/specs/*.*s ./evolve/plsql/bodies/*.*b ./evolve/plsql/functions/*.fnc
zip -r ./transcend.zip ./install/install_transcend.sql ./transcend/plsql/specs/*.*s ./transcend/plsql/bodies/*.*b
