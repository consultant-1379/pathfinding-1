#!/bin/bash
# ********************************************************************
# Ericsson Radio Systems AB                                     SCRIPT
# ********************************************************************
#
# (c) Ericsson Radio Systems AB 2016 - All rights reserved.
#
# The copyright to the computer program(s) herein is the property
# of Ericsson Radio Systems AB, Sweden. The programs may be used
# and/or copied only with the written permission from Ericsson Radio
# Systems AB or in accordance with the terms and conditions stipulated
# in the agreement/contract under which the program(s) have been
# supplied .
#
# ********************************************************************
# Name    : loglevelcheck.sh
# Purpose : Main script to change the log level to DEFAULT.
#
# Usage   : bash loglevelcheck.sh
#
# ********************************************************************
#
#   Command Section
#
# ********************************************************************
. /eniq/home/dcuser/.profile

if [ ! -r "${BIN_DIR}/common_variables.lib" ] ; then
  echo "ERROR: Source file is not readable at ${BIN_DIR}/common_variables.lib"
  exit 3
fi

. ${BIN_DIR}/common_variables.lib

CONF_DIR=/eniq/sw/conf
export CONF_DIR
LOG_DIR=/eniq/log/sw_log
LOG_FILE=$LOG_DIR/engine/logmanagement-`$DATE +%Y_%m_%d`.log
ENGINELOGFILE=$CONF_DIR/engineLogging.properties
ENGINELOGFILEBKUP=$CONF_DIR/engineLoggingproperties_bkup
SCHEDULERLOGFILE=$CONF_DIR/schedulerLogging.properties
SCHEDULERLOGFILEBKUP=$CONF_DIR/schedulerLoggingproperties_bkup
LICENSINGLOGFILE=$CONF_DIR/licensingLogging.properties
LICENSINGLOGFILEBKUP=$CONF_DIR/licensingLoggingproperties_bkup
FILE=$CONF_DIR/static.properties

declare -A engine_default=( [".level"]="INFO" ["sun.rmi.level"]="WARNING" ["etlengine.level"]="INFO" ["etlengine.priorityqueue.level"]="INFO" ["file.level"]="INFO" ["lwphelper.level"]="INFO" ["handlers"]="com.ericsson.eniq.common.EngineLogger")

declare -A scheduler_default=( [".level"]="CONFIG" ["sun.rmi.level"]="WARNING" ["handlers"]="com.ericsson.eniq.common.SchedulerLogger")

declare -A licensing_default=( [".level"]="INFO" ["sun.rmi.level"]="WARNING" ["licensing.level"]="FINE" ["file.level"]="INFO" ["handlers"]="com.ericsson.eniq.common.LicensingLogger")

_echo()
{
	$ECHO "`$DATE '+%Y_%m_%d_%H:%M:%S'` : INFO : $1" >> $LOG_FILE
}
abort_script()
{    
if [ "$1" ]; then
	$ECHO "`$DATE '+%Y_%m_%d_%H:%M:%S'` : ERROR : $_err_msg_" >> $LOG_FILE
else
    _err_msg_="ERROR : Script aborted.......\n" 
	$ECHO "`$DATE '+%Y_%m_%d_%H:%M:%S'` : ERROR : $_err_msg_" >> $LOG_FILE
fi
exit

}
change_schedulerlicensing_log()
{
if line=$($CAT $LICENSINGLOGFILE  |egrep 'FINEST|FINER')
then
    _echo "Logging  to be changed to Default are  $line"  
	sed -i "s/licensing.level=.*/licensing.level=FINE/" $LICENSINGLOGFILE
	if [ $? -ne 0 ] ; then
		_err_msg_="Could not change log level to Default  in licensingLogging.properties"
		abort_script "${_err_msg_}"
	fi;
	_echo "$LICENSINGLOGFILE has been updated. restarting license manager" 
	_echo "running command : licmgr -restart" 
	licmgr -restart	
else
    _echo "No logs in  FINEST level in licensingLogging.properties "  
fi

if line=$($CAT $SCHEDULERLOGFILE  |$GREP -i 'FINE')
then
    _echo "Logging  to be changed to Default are  $line"  
	sed -i "s/sun.rmi.level=.*/sun.rmi.level=WARNING/;s/.level=.*/.level=CONFIG/" $SCHEDULERLOGFILE 
	if [ $? -ne 0 ] ; then
		_err_msg_="Could not change log level to Default in schedulerLogging.properties"
		abort_script "${_err_msg_}"
	fi;
	_echo "$SCHEDULERLOGFILE has been updated. restarting scheduler" 
	_echo "running command : scheduler restart" 
	scheduler restart
else
    _echo "No logs in  FINE level in schedulerLogging.properties  "
fi

$CHMOD 640 $ENGINELOGFILE $LICENSINGLOGFILE $SCHEDULERLOGFILE
}
permission_check()
{
#removing dot at the end of file permissions usring tr command in linux OS
$LS -lrt $1|$NAWK '{ print $1 }' |tr -d '.' |while read output ;
do
if [ "$output" != "-rw-r-----" ] ; then
  $CHMOD 640 $1
  _echo "File permission changed from $output to -rw-r-----" 
if [ $? -ne 0 ] ; then
    _err_msg_="Could not change permission"
    abort_script "${_err_msg_}"
fi;
fi;
done
}

change_all_values(){

if line=$($CAT $ENGINELOGFILE | $GREP  -i 'FINE')
then
    _echo "Logging  to be changed to INFO are  $line"  
	sed -i "s/FINEST/INFO/;s/FINER/INFO/;s/FINE/INFO/"  $ENGINELOGFILE
	if [ $? -ne 0 ] ; then
		_err_msg_="Could not change log level in engineLogging.properties "
		abort_script "${_err_msg_}"
	fi;
	_echo "$ENGINELOGFILE has been updated. reloading engine configuration" 
	_echo "running command : engine -e reloadConfig" 
	engine -e reloadConfig
else
    _echo "No logs in  FINE level in enginelogging.properties "
fi

change_schedulerlicensing_log
if [ $? -ne 0 ] ; then
    _err_msg_="Could not change permission "
    abort_script "${_err_msg_}"
fi;
}

backup_files()
{
cp $ENGINELOGFILE $ENGINELOGFILEBKUP
cp $SCHEDULERLOGFILE $SCHEDULERLOGFILEBKUP
cp $LICENSINGLOGFILE $LICENSINGLOGFILEBKUP
}
check_for_default_values()
{
update_engine=0
update_scheduler=0
update_licensing=0
for key in "${!engine_default[@]}"; do 
if grep -q "^$key" $ENGINELOGFILE
then
    _echo "$key found in $ENGINELOGFILE" 
else
    _echo "$key not found in $ENGINELOGFILE. updating $ENGINELOGFILE with $key=${engine_default[$key]}" 
	$ECHO "$key=${engine_default[$key]}" >> $ENGINELOGFILE
	update_engine=1
fi
done
if [ $update_engine -eq 1 ];then
	_echo "$ENGINELOGFILE has been updated. reloading engine configuration" 
	_echo "running command : engine -e reloadConfig" 
	engine -e reloadConfig
fi

for key in "${!scheduler_default[@]}"; do 
if grep -q "^$key" $SCHEDULERLOGFILE
then
    _echo "$key found in $SCHEDULERLOGFILE" 
else
    _echo "$key not found in $SCHEDULERLOGFILE. updating $SCHEDULERLOGFILE with $key=${scheduler_default[$key]}" 
	$ECHO "$key=${scheduler_default[$key]}" >> $SCHEDULERLOGFILE
	update_scheduler=1
fi
done
if [ $update_scheduler -eq 1 ];then
	_echo "$SCHEDULERLOGFILE has been updated. restarting scheduler" 
	_echo "running command : scheduler restart" 
	scheduler restart
fi

for key in "${!licensing_default[@]}"; do 
if grep -q "^$key" $LICENSINGLOGFILE
then
    _echo "$key found in $LICENSINGLOGFILE" 
else
    _echo "$key not found in $LICENSINGLOGFILE. updating $LICENSINGLOGFILE with $key=${licensing_default[$key]}" 
	$ECHO "$key=${licensing_default[$key]}" >> $LICENSINGLOGFILE
	update_licensing=1
fi
done

if [ $update_licensing -eq 1 ];then
	_echo "$LICENSINGLOGFILE has been updated. restarting license manager" 
	_echo "running command : licmgr -restart" 
	licmgr -restart
fi

}
# ********************************************************************
#
#       Main body of program
#
# ********************************************************************
#
active_process=`ps -aef | grep $0 | grep -v "grep" | wc -l `
# comparing with value 3, since the process count is returning 3, even if only one instance is running currently
# dcuser   26196 26182  0 09:05 ?        00:00:00 /bin/sh -c /bin/bash /eniq/sw/conf/loglevelcheck.sh > /dev/null 2>&1
# dcuser   26199 26196  0 09:05 ?        00:00:00 /bin/bash /eniq/sw/conf/loglevelcheck.sh

if [ $active_process -gt 3 ]; then
	_err_msg_="Another instance of loglevelcheck.sh script is running."
    abort_script "${_err_msg_}"
fi

permission_check $ENGINELOGFILE
permission_check $SCHEDULERLOGFILE
permission_check $LICENSINGLOGFILE

check_for_default_values

usep=$($DF -h '/eniq/log' | $SED "1 d"  | $NAWK '{ print $5}' |  $CUT -d'%' -f1)
maxlimit=$($GREP -i 'Logging.MaxLimit' $FILE  | $CUT -f2 -d'=')
finest=$($GREP -i 'FINEST.Limit' $FILE  | $CUT -f2 -d'=')
finer=$($GREP -i 'FINER.Limit' $FILE  | $CUT -f2 -d'=')
fine=$($GREP -i 'FINE.Limit' $FILE  | $CUT -f2 -d'=')
if [ $usep -gt 100 ] || [ $finest -gt $finer ] || [ $finer -gt $fine ] || [ $fine -gt $maxlimit ] ;  then
        _err_msg_="Properties values are not valid,set proper values" 
		abort_script "${_err_msg_}"
else
if [ $usep -gt $maxlimit ]; then
  _echo "Log filesystem [/eniq/log] utilization is greater than $maxlimit ,Changing  Logging level Default"  
  backup_files
  change_all_values     
fi;
fi;
