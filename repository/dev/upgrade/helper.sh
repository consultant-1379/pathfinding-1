#!/bin/bash

/dc/java/bin/java -classpath lib/testhelper_5-0-0b14.jar:lib/dbunit-2.4.7.jar:lib/engine_5-0-0b235.jar:lib/jconn*.jar:lib/repository_5-0-0b85.jar com.distocraft.dc5000.etl.TestHelper $1 $2 $3
