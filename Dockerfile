#*********************************************************************
#   Copyright 2021 Regents of the University of California
#   All rights reserved
#*********************************************************************

ARG ECR_REGISTRY=ecr_registry_not_set

FROM ${ECR_REGISTRY}/merritt-tomcat:dev

# Postfix
# SES_PARMS is defined in SSM /uc3/mrt/prd/config/ses
ARG SES_PARMS=ses_passwd_not_set
RUN yum -y install cyrus-sasl cyrus-sasl-plain cyrus-sasl-lib postfix mailx
RUN /usr/sbin/postconf -e "relayhost = [email-smtp.us-west-2.amazonaws.com]:587" "smtp_sasl_auth_enable = yes" "smtp_sasl_security_options = noanonymous" "smtp_sasl_password_maps = hash:/etc/postfix/sasl_passwd" "smtp_use_tls = yes" "smtp_tls_security_level = secure" "smtp_tls_note_starttls_offer = yes" 'smtp_tls_CAfile = /etc/ssl/certs/ca-bundle.crt'
RUN echo "myhostname = merritt.cdlib.org" >> /etc/postfix/main.cf
RUN /usr/sbin/postconf -e "inet_interfaces = loopback-only" "inet_protocols = ipv4"
RUN echo "[email-smtp.us-west-2.amazonaws.com]:587 ${SES_PARMS}" > /etc/postfix/sasl_passwd
RUN /usr/sbin/postmap hash:/etc/postfix/sasl_passwd

COPY ingest-war/target/mrt-ingestwar-*.war /usr/local/tomcat/webapps/ingest.war
RUN mkdir -p /tdr/tmpdir/logs 
RUN mkdir -p /merritt-filesys/ingest/queue && \
    ln -s /merritt-filesys/ingest/queue /usr/local/tomcat/webapps/ingestqueue

COPY docker/profiles /merritt-filesys/ingest/profiles/
COPY docker/admin-submit /merritt-filesys/ingest/admin-submit

# Override entrypoint
RUN echo "#!/bin/bash"  > /entrypoint.sh
RUN echo /usr/sbin/postfix start >> /entrypoint.sh
RUN echo /usr/local/tomcat/bin/catalina.sh run >> /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
