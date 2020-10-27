#!/bin/bash

./query1 -Dcity=BUE "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/
./query1 -Dcity=VAN "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/

./query2 -Dcity=BUE "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/ -Dmin=300
./query2 -Dcity=VAN "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/ -Dmin=300

./query3 -Dcity=BUE "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/ -Dn=3
./query3 -Dcity=VAN "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/ -Dn=3

./query4 -Dcity=BUE "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/ -Dmin=11000 -Dname='Fraxinus pennsylvanica'
./query4 -Dcity=VAN "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/ -Dmin=11000 -Dname='COMMON HORSECHESTNUT'

./query5 -Dcity=BUE "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/
./query5 -Dcity=VAN "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/
