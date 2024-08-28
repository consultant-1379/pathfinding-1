#!/bin/bash
# ********************************************************************
# Ericsson Radio Systems AB                                     SCRIPT
# ********************************************************************
#
# (c) Ericsson Radio Systems AB 2015 - All rights reserved.
#
# The copyright to the computer program(s) herein is the property
# of Ericsson Radio Systems AB, Sweden. The programs may be used
# and/or copied only with the written permission from Ericsson Radio
# Systems AB or in accordance with the terms and conditions stipulated
# in the agreement/contract under which the program(s) have been
# supplied.
#
# ********************************************************************
# Name    : RetentionOfNodes.sh
# Date    : 10/11/2016
# Purpose : Adding the fdns present in the ENIQ-S before mounting  
# Usage   : RetentionOfNodes.sh
#
# ********************************************************************
#
# Command Section
#
# ********************************************************************

if [ ! -r "/eniq/sw/bin/common_variables.lib" ] ; then
  echo "ERROR: Source file is not readable at /eniq/sw/bin/common_variables.lib"
  exit 3
fi

. /eniq/sw/bin/common_variables.lib

if [ "$1" == "-h" ]; then
  echo "Usage: /eniq/sw/bin/RetentionOfNodes [ENM_HOST_NAME]"
  echo "where ENM_HOST_NAME is the fully qualified host name of ENM for which the nodes should be retained"
  exit 0
fi

DBISQL="$(ls /eniq/sybase_iq/IQ-*/bin64/dbisql)"

STARTTIMESTAMP=`$DATE '+%Y_%m_%d_%H:%M:%S'`
LOG_FILE=/eniq/log/sw_log/symboliclinkcreator/RetentionOfNodes-${STARTTIMESTAMP}.log
touch ${LOG_FILE}
INSTALLER_DIR=/eniq/sw/installer
TEMP=${INSTALLER_DIR}/temp_nat
engineHostName=`$CAT /eniq/sw/conf/service_names | $GREP engine | $NAWK -F'::' '{print $2}'`
ENM_HOST_NAME=$1

. ${CONF_DIR}/niq.rc

JAVA_HOME="${RT_DIR}/java"
for _jar_ in `find ${PLATFORM_DIR}/*/dclib/ -name \*.jar` ; do
	CPATH="${CPATH}:$_jar_"
done
COMMON_JAR=`ls ${PLATFORM_DIR}/common*/dclib/common.jar`
LICENSING_JAR=`ls ${PLATFORM_DIR}/licensing*/dclib/licensing.jar`
ENGINE_JAR=`ls ${PLATFORM_DIR}/engine*/dclib/engine.jar`
CODEBASE="file://${COMMON_JAR} file://${ENGINE_JAR} file://${LICENSING_JAR}"


if [ ! -d ${TEMP} ] ; then
  $MKDIR -p ${TEMP}
  $CHMOD 777 ${TEMP}
else
  $RM -rf ${TEMP}
  $MKDIR -p ${TEMP}
  $CHMOD 777 ${TEMP}
fi 

if [ -s /eniq/admin/lib/common_functions.lib ]; then
    . /eniq/admin/lib/common_functions.lib
else
        $ECHO "Could not find /eniq/admin/lib/common_functions.lib"
        exit 1
fi

DWHDBPASSWORD=`inigetpassword DWH -v DCPassword -f ${CONF_DIR}/niq.ini`
DWHDB_PORT=`inigetpassword DWH -v PortNumber -f ${CONF_DIR}/niq.ini`
DWH_SERVER_NAME=`inigetpassword DWH -v ServerName -f ${CONF_DIR}/niq.ini`

DWHREPUSER=`inigetpassword REP -v DWHREPUsername -f ${CONF_DIR}/niq.ini`
DWHREPPASSWORD=`inigetpassword REP -v DWHREPPassword -f ${CONF_DIR}/niq.ini`
REP_PORT=`inigetpassword REP -v PortNumber -f ${CONF_DIR}/niq.ini`
REP_SERVER_NAME=`inigetpassword REP -v ServerName -f ${CONF_DIR}/niq.ini`
DBAPASSWORD=`inigetpassword DB -v UtilDBAPASSWORD -f ${CONF_DIR}/niq.ini`

if [ ! -x "$DBISQL" ]; then
    _err_msg_="$DBISQL commands not found or not executable."
    abort_script "$_err_msg_"
fi

### Function: retaining_ExistingNodes ###
#
#   Retain the existing nodes from the DIM tables
#
# Arguments:
#      $1- Topology Table
#      $2- Column which contains the FDN 
#      $3- Column which contains the NETYPE 
#      $4- Eniq_identifier
#	   $5- Eniq_Alias
# Return Values:
#       none
#*************** Retain the existing nodes from the DIM tables **************************************
retaining_ExistingNodes(){
	topologyTable=$1
	fdnColumn=$2
	neType=$3
	eniqIdentifier=$4
	ossId=$5
	$ECHO "Retaining the existing nodes for $1  " | $TEE -a ${LOG_FILE}
	$DBISQL -nogui -onerror exit -c "eng=${DWH_SERVER_NAME};links=tcpip{host=${DWH_SERVER_NAME};port=${DWHDB_PORT}};uid=dc;pwd=${DWHDBPASSWORD}" "select distinct $2,$3,$5 from $1;OUTPUT TO ${TEMP}/tmp.txt DELIMITED BY '|'"   > /dev/null 2>&1
	$DBISQL -nogui -onerror exit -c "eng=${DWH_SERVER_NAME};links=tcpip{host=${DWH_SERVER_NAME};port=${DWHDB_PORT}};uid=dc;pwd=${DWHDBPASSWORD}" "select distinct $2,$3,$5 from $1;OUTPUT TO ${TEMP}/NodeFDN.txt DELIMITED BY '|' APPEND "   > /dev/null 2>&1
	
	if [[ -s ${TEMP}/NodeFDN.txt ]]; then
		$SED "s/'//g" ${TEMP}/NodeFDN.txt >> ${TEMP}/NodeFDNtemp.txt
		$MV ${TEMP}/NodeFDNtemp.txt ${TEMP}/NodeFDN.txt
	fi
	
    if [[ $? -eq 0 ]]; then
	if [[ -s ${TEMP}/tmp.txt ]]; then
		$ECHO "Successfully Retrieved nodes from the $1 topology table"  | $TEE -a ${LOG_FILE}
	else
		$ECHO "Topology table $1 is empty"| $TEE -a ${LOG_FILE}
	fi
	else
		$ECHO "Failed while retrieving nodes from the $1 topology table $1"   | $TEE -a ${LOG_FILE}
	fi

}
### Function: topology_active ###
#
#   Check whether the topology table is active
#
# Arguments:
#      $1- Topology Name
# Return Values:
#       Active or Inactive
topology_active(){
# Is parameter #1 zero length?
if [ -z "$1" ]                           
then
	$ECHO "There is no topology to check" >> ${LOG_FILE}
else															 # Or no parameter passed.
	$ECHO "Topology  \"$1\" is checked for active or not..." >> ${LOG_FILE}
	$DBISQL -nogui -c "eng=${REP_SERVER_NAME};links=tcpip{host=${REP_SERVER_NAME};port=${REP_PORT}};uid=$DWHREPUSER;pwd=$DWHREPPASSWORD" "select status from tpActivation where TYPE='topology' and TECHPACK_NAME='$1';OUTPUT TO ${TEMP}/tempText.txt" > /dev/null 2>&1
	resultSet=$(head -n 1 ${TEMP}/tempText.txt)
	$ECHO "$resultSet" 
fi
}

### Function: insert_NAT ###
#
#   copy the fdn and ne_type to the ENIQS_Node_Assignment table   
#
# Arguments:
#      none
# Return Values:
#       none
insert_NAT(){

if [[ -f ${TEMP}/RetentionOfNodes.txt  && -s ${TEMP}/RetentionOfNodes.txt ]] ; then
	$NAWK '!seen[$0]++' ${TEMP}/RetentionOfNodes.txt > ${TEMP}/RetainNodes.txt
fi
textfile=${TEMP}/RetainNodes.txt
if [[ -f ${textfile}  && -s ${textfile} ]] ; then
	$ECHO "Topology data is present in $engineHostName server for Retention. Nodes will be added in ENIQS_Node_Assignment" >> ${LOG_FILE}
	#$DBISQL -nogui -onerror exit -c "eng=${REP_SERVER_NAME};links=tcpip{host=${REP_SERVER_NAME};port=${REP_PORT}};uid=$DWHREPUSER;pwd=$DWHREPPASSWORD" "LOAD TABLE ENIQS_Node_Assignment (ENIQ_IDENTIFIER , FDN, NETYPE, ENM_HOSTNAME) FROM '$textfile' QUOTES OFF DELIMITED BY '|';"  > /dev/null 2>&1
	${JAVA_HOME}/bin/java -d64 -Dpname="FLSAdmin" -mx64M \
		-Djava.util.logging.config.file=${CONF_DIR}/symboliclinkcreatorLogging.properties \
		-Ddc5000.config.directory=${CONF_DIR} -Ddc.conf.dir=${CONF_DIR} -DLOG_DIR=${LOG_DIR} \
		-classpath ${CPATH} -Djava.rmi.server.codebase="${CODEBASE}" \
		com.ericsson.eniq.enminterworking.automaticNAT.RetainExistingNodes ${TEMP}/RetainNodes.txt $ENM_HOST_NAME
	if [[ $? -eq 0 ]]; then
		$ECHO "Successfully inserted into ENIQS_Node_Assignment table"  | $TEE -a ${LOG_FILE}
	else
		$ECHO "Failed while inserting into ENIQS_Node_Assignment table" | $TEE -a ${LOG_FILE}
	fi
else
	$ECHO "$engineHostName server is not having any Topology data for Retention"| $TEE -a ${LOG_FILE}

fi
}

### Function: check_fdn ###
#
#   Check if both ManagedElement and MeContext is present in the fdn   
#
# Arguments:
#       $1 - FDN which contains ManagedElement
# Return Values:
#       Correct FDN with either Mecontext or ManangedElement
check_fdn(){
	$ECHO "FDN read from the Input file $1" >> ${LOG_FILE}
	if [[ ! -z $1 ]] ; then
		first_matching_fdn=`expr match "$1"  '\(.*,MeContext=[^,]*\).*' `
		if [ "$first_matching_fdn" = "" ] ; then
			second_matching_fdn=`expr match "$1"  '\(.*,ManagedElement=[^,]*\).*' `
			if [ "$second_matching_fdn" = "" ] ;  then
				$ECHO "fdn is $1" >> ${LOG_FILE}
				modified_fdn=$1
				$ECHO $modified_fdn
			else
				$ECHO "fdn is $second_matching_fdn" >> ${LOG_FILE}
				modified_fdn=$second_matching_fdn
				$ECHO $modified_fdn
			fi
		else
			$ECHO "fdn is $first_matching_fdn" >> ${LOG_FILE}
			modified_fdn=$first_matching_fdn
			$ECHO $modified_fdn
		fi
	else 
			$ECHO  $1
	fi
}

#****************************************** Main Body of the Script *********************************************
$ECHO "================================================================================" >> ${LOG_FILE} 
$ECHO "******** Starting Retention of the Existing Nodes for $engineHostName ******   "  >> ${LOG_FILE}
$ECHO "================================================================================" >> ${LOG_FILE}

$ECHO "Execution started at: " $STARTTIMESTAMP >> ${LOG_FILE}
conf_File="/eniq/sw/conf/Topologytables.txt"
#Get the Eniq_identifire from the servicenames:
$ECHO "eniqIdentifier is identified as  $engineHostName" | $TEE -a ${LOG_FILE}
while IFS= read -r line
do


    arr=($line)
	if [[ ! -z ${arr} ]]; then
		topology=${arr[0]}
		topologyTable=${arr[1]}
		fdnColumn=${arr[2]}
		neType=${arr[3]} 
		ossId=${arr[4]} 
		$ECHO "For $topology    $topologyTable" | $TEE -a ${LOG_FILE}
		resultSet=$(topology_active $topology) 
		if [ "${resultSet}" == "'ACTIVE'" ]; then
			$ECHO "$topology is active in the server. Hence retaining the nodes."  | $TEE -a ${LOG_FILE}
			retaining_ExistingNodes $topologyTable $fdnColumn $neType $engineHostName $ossId
		else
			$ECHO "$topology is not active in the server.Hence retention of nodes is skipped for $topology" | $TEE -a ${LOG_FILE}		
		fi
	fi
done < $conf_File
if [[ -s ${TEMP}/NodeFDN.txt ]]; then
	$SED "s/^/$engineHostName|/g" ${TEMP}/NodeFDN.txt >> ${TEMP}/NodeFDNtemp.txt
	manipulate_fdn="${TEMP}/NodeFDNtemp.txt"
	while IFS= read -r line
	do
		temp_identifire=`$ECHO $line | $NAWK -F'|' '{print $1}'` 
		temp_neTtpe=`$ECHO $line |  $NAWK -F'|' '{print $3}'`
		temp_tempFDN=`$ECHO $line |  $NAWK -F'|' '{print $2}'`
		temp_ossId=`$ECHO $line |  $NAWK -F'|' '{print $4}'`
		temp_fdn=`$ECHO $line |  $NAWK -F'|' '{print $2}' | grep -i "ManagedElement"`
		if [[ ! -z ${temp_neType} ]]; then
			if [[ ! -z ${temp_fdn} ]] ; then
				actual_fdn=$(check_fdn $temp_fdn)
				if [[ ! -z ${actual_fdn} ]] ; then
					$ECHO "$temp_identifire|$actual_fdn|$temp_neTtpe|$temp_ossId" >> ${TEMP}/RetentionOfNodes.txt
				else 
					$ECHO "fdn $temp_tempFDN is not Proper... Please check it " | $TEE -a ${LOG_FILE}
				fi
			else
				$ECHO "$temp_identifire|$temp_tempFDN|$temp_neTtpe|$temp_ossId" >> ${TEMP}/RetentionOfNodes.txt
				$ECHO "FDN read from the Input file $temp_tempFDN" >> ${LOG_FILE}
				$ECHO "fdn is $temp_tempFDN" >> ${LOG_FILE}
			fi
		fi
	done < $manipulate_fdn
fi

#insert into ENIQS_Node_Assignment table:
insert_NAT

ENDTIMESTAMP=`$DATE '+%Y_%m_%d_%H:%M:%S'`
$ECHO "Execution ended at: " $ENDTIMESTAMP >> ${LOG_FILE}

##cleanup
rm -rf ${TEMP}