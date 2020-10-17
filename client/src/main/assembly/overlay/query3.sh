#!/bin/bash

java -Dquery=3 $* -cp 'lib/jars/*' "ar.edu.itba.pod.client.Client"