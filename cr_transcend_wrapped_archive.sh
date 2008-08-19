#!/bin/bash

zip -r ./transcend_wrapped.zip ./plsql/specs/*.*s ./plsql/wrapped_bodies/*plb  ./install/*evolve*.sql ./install/*tdsys*.sql ./evolve/java/* ./evolve/plsql/specs/*.*s ./evolve/plsql/wrapped_bodies/*plb
zip -r ./transcend_wrapped.zip ./install/*transcend*.sql ./transcend/plsql/specs/*.*s ./transcend/plsql/wrapped_bodies/*plb
