#!/bin/bash

if [ "$2" == "" ]; then
    	echo usage: $0 \<Branch\> \<RState\>
    	exit -1
else
	versionProperties=install/version.properties
	theDate=\#$(date +"%c")
	module=$1
	branch=$2
	workspace=$3
fi

function getProductNumber {
        product=`cat ${WORKSPACE}/workspace/TPC_Release/build.cfg | grep $module | grep $branch | awk -F " " '{print $3}'`
}


function setRstate {

        revision=`cat ${WORKSPACE}/workspace/TPC_Release/build.cfg | grep $module | grep $branch | awk -F " " '{print $4}'`
 
       	if git tag | grep $product-$revision; then
	        rstate=`git tag | grep $revision | tail -1 | sed s/.*-// | perl -nle 'sub nxt{$_=shift;$l=length$_;sprintf"%0${l}d",++$_}print $1.nxt($2) if/^(.*?)(\d+$)/';`
        else
                ammendment_level=01
                rstate=$revision$ammendment_level
        fi
	echo "Building R-State:$rstate"

}

function appendRStateToPlatformReleaseXml {

		versionXml="${WORKSPACE}/workspace/TPC_Release/src/resources/version/release.xml"
		
		if [ ! -e ${versionXml} ] ; then
			echo "version xml file is missing from build: ${versionXml}"
			exit -1
		fi

		mv ${WORKSPACE}/workspace/TPC_Release/src/resources/version/release.xml ${WORKSPACE}/workspace/TPC_Release/src/resources/version/release.${rstate}.xml

}


function nexusDeploy {
	#RepoURL=http://eselivm2v214l.lmera.ericsson.se:8081/nexus/content/repositories/assure-releases
    RepoURL=https://arm1s11-eiffel004.eiffel.gic.ericsson.se:8443/nexus/content/repositories/assure-releases
	GroupId=com.ericsson.eniq
	ArtifactId=$module
	isoName=ERICmomtool
	
	echo "****"	
	echo "Deploying the iso /$isoName-17.0-B.iso as ${ArtifactId}${rstate}.iso to Nexus...."
        mv target/iso/$isoName-*.iso target/${ArtifactId}.iso
	echo "****"	

  	mvn deploy:deploy-file \
	        	-Durl=${RepoURL} \
		        -DrepositoryId=assure-releases \
		        -DgroupId=${GroupId} \
		        -Dversion=${rstate} \
		        -DartifactId=${ArtifactId} \
		        -Dfile=target/${ArtifactId}.iso
		         
 				

}

getProductNumber
setRstate
git checkout $branch
git pull origin $branch
appendRStateToPlatformReleaseXml



#export currentVersion=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version|grep "^[0-9]*\.[0-9]*\.[0-9]*"`
#export newVersion=`echo ls -t1 | git tag  | tail -1 |head -n 1 | cut -f2 -d -`
#find . -name "pom.xml" -exec sed -i "s/${currentVersion}/${$product-$rstate}/g" {} \; 2>/dev/null


#add maven command here
/proj/eiffel004_config/fem156/slaves/RHEL_ENIQ_STATS/tools/hudson.tasks.Maven_MavenInstallation/Maven_3.0.5/bin/mvn exec:exec

#nexusDeploy 

rsp=$?

if [ $rsp == 0 ]; then

  git tag $product-$rstate
  git pull
  git push --tag origin $branch

fi

exit $rsp


