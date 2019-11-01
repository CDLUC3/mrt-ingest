# Publish as cdluc3/mrt-ingest

#   docker build -t cdluc3/mrt-ingest .

FROM cdluc3/mrt-dependencies as build
WORKDIR /tmp/mrt-ingest

ADD . /tmp/mrt-ingest

RUN mvn install -D=environment=local && \
    mvn clean

FROM tomcat:8-jre8
COPY --from=build /root/.m2/repository/org/cdlib/mrt/mrt-ingestwar/1.0-SNAPSHOT/mrt-ingestwar-1.0-SNAPSHOT.war /usr/local/tomcat/webapps/ingest.war

EXPOSE 8080 8009

RUN mkdir -p /tdr/ingest /usr/local/tomcat/webapps/ingestqueue

RUN echo -e '\n\
name: UC3 Docker Ingest\n\
identifier: ingest\n\
target: http://localhost\n\
description: UC3 ingest docker micro-service\n\
service-scheme: Ingest/0.1\n\
access-uri: http://localhost\n\
support-uri: http://www.cdlib.org/services/uc3/contact.html\n\
admin:\n\
purl: http://n2t.net/\n\
ezid: merritt:merritt'\
>> /tdr/ingest/ingest-info.txt

RUN echo -e '\n\
QueueService: zoo:2181
QueueName: /ingest
InventoryName: /mrt.inventory.full
QueueHoldFile: /tdr/ingest/queue/HOLD
PollingInterval: 10
NumThreads: 5'\
>> /tdr/ingest/queue.txt

RUN echo -e '\n\
store.1: http://store:8080\n\
access.1: http://store:8080\n\
localID: http://inventory:8080/mrtinv'\
>> /tdr/ingest/stores.txt
