#!/bin/bash

zip -r ./transcend_wrapped.zip ./plsql/specs/*.*s ./plsql/wrapped_bodies/*plb ./evolve/java/* ./evolve/plsql/specs/*.*s ./evolve/plsql/wrapped_bodies/*plb
zip -r ./transcend_wrapped.zip ./install/install_transcend.sql ./transcend/plsql/specs/*.*s ./transcend/plsql/wrapped_bodies/*plb
