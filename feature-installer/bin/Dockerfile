#
# COPYRIGHT Ericsson 2020
#
#
#
# The copyright to the computer program(s) herein is the property of
#
# Ericsson Inc. The programs may be used and/or copied only with written
#
# permission from Ericsson Inc. or in accordance with the terms and
#
# conditions stipulated in the agreement/contract under which the
#
# program(s) have been supplied.
#

FROM armdocker.rnd.ericsson.se/proj-am/sles/sles-openjdk11:3.29.0-10
ARG JAR_FILE
ARG COMMIT
ARG BUILD_DATE
ARG APP_VERSION
LABEL \
    org.opencontainers.image.title=eric-ms-chassis-jsb \
    org.opencontainers.image.created=$BUILD_DATE \
    org.opencontainers.image.revision=$COMMIT \
    org.opencontainers.image.vendor=Ericsson \
    org.opencontainers.image.version=$APP_VERSION

ARG USER_ID=40514
ARG USER_NAME="eric-ms-chassis"

ADD target/${JAR_FILE} demo-app.jar
COPY src/main/resources/jmx/* /jmx/
RUN chmod 600 /jmx/jmxremote.password
RUN chown $USER_ID /jmx/jmxremote.password

RUN echo "$USER_ID:x:$USER_ID:0:An Identity for $USER_NAME:/nonexistent:/bin/false" >>/etc/passwd
RUN echo "$USER_ID:!::0:::::" >>/etc/shadow

USER $USER_ID

CMD java ${JAVA_OPTS} -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.port=1099 \
-Dcom.sun.management.jmxremote.authenticate=true -Dcom.sun.management.jmxremote.ssl=false \
-Dcom.sun.management.jmxremote.rmi.port=1099 -Dcom.sun.management.jmxremote.password.file=/jmx/jmxremote.password \
-Dcom.sun.management.jmxremote.access.file=/jmx/jmxremote.access -jar demo-app.jar
