#!/bin/bash

./query1.sh -Dcity=BUE "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/
./query1.sh -Dcity=VAN "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/

./query2.sh -Dcity=BUE "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/ -Dmin=300
./query2.sh -Dcity=VAN "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/ -Dmin=300

./query3.sh -Dcity=BUE "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/ -Dn=3
./query3.sh -Dcity=VAN "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/ -Dn=3

./query4.sh -Dcity=BUE "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/ -Dmin=11000 -Dname='Fraxinus pennsylvanica'
./query4.sh -Dcity=VAN "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/ -Dmin=11000 -Dname='COMMON HORSECHESTNUT'

./query5.sh -Dcity=BUE "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/
./query5.sh -Dcity=VAN "$@" -DinPath=./../../../examples/ -DoutPath=./../../../examples/
