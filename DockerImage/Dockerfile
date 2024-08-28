FROM openjdk:8-jdk-alpine 
RUN apk --no-cache add curl
VOLUME /tmp
RUN mkdir -p /eniq/installer/INTF_DC_E_AFG /eniq/installer/dwhmtemplate /eniq/installer/DC_E_AFG
WORKDIR /root/installerapplication
ENV PORT 8090
EXPOSE 8090
COPY dwhmtemplate/ /eniq/installer/dwhmtemplate
COPY ETLCImport.properties /eniq/installer
COPY static.properties /eniq/installer
COPY INTF_DC_E_AFG/ /eniq/installer/INTF_DC_E_AFG/
COPY DC_E_AFG/ /eniq/installer/DC_E_AFG/
COPY InstallerApplication-0.0.1-SNAPSHOT.jar /eniq/installer/InstallerApplication-0.0.1-SNAPSHOT.jar
ADD start.sh /eniq/installer/ 
ADD inputList.json /eniq/installer/
RUN chmod 755 /eniq/installer/start.sh
RUN nohup java -jar /eniq/installer/InstallerApplication-0.0.1-SNAPSHOT.jar > /eniq/installer/output.txt &
ENTRYPOINT ["/eniq/installer/start.sh"]

