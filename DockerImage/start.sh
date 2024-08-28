#!/bin/sh
java -jar /eniq/installer/InstallerApplication-0.0.1-SNAPSHOT.jar &
sleep 10
curl -X POST -H "Content-Type: application/json" -d @/eniq/installer/inputList.json http://10.45.194.84:30002/install
exit 0

