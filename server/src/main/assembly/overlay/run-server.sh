#!/bin/bash

java -Dinterface_ip="$1" -cp 'lib/jars/*' "ar.edu.itba.pod.server.Server"

