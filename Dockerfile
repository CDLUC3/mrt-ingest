#*********************************************************************
#   Copyright 2021 Regents of the University of California
#   All rights reserved
#*********************************************************************

ARG ECR_REGISTRY=ecr_registry_not_set

FROM ${ECR_REGISTRY}/merritt-tomcat:dev

COPY ingest-war/target/mrt-ingestwar-*.war /usr/local/tomcat/webapps/ingest.war

RUN mkdir -p /tdr/tmpdir/logs 
RUN mkdir -p /tdr/ingest/queue && \
    ln -s /tdr/ingest/queue /usr/local/tomcat/webapps/ingestqueue

COPY docker/profiles /tdr/ingest/profiles/
COPY docker/admin-submit /tdr/ingest/admin-submit
