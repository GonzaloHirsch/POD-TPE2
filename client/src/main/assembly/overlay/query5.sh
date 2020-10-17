#!/bin/bash

java -Dquery=5 $* -cp 'lib/jars/*' "ar.edu.itba.pod.client.Client"