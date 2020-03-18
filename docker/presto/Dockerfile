# Global args
ARG HUDI_VERSION
ARG PRESTO_VERSION

FROM maven:3.6.1 as presto_builder
ARG PRESTO_VERSION
ARG JMX_EXPORTER_VERSION

COPY certs/root-ca.cert.pem /usr/local/share/ca-certificates/cloudkitchens-root.crt
COPY certs/intermed-ca.cert.pem /usr/local/share/ca-certificates/cloudkitchens-intermed.crt
COPY certs/staging-root-ca.cert.pem /usr/local/share/ca-certificates/cloudkitchens-staging-root.crt
COPY certs/staging-intermed-ca.cert.pem /usr/local/share/ca-certificates/cloudkitchens-staging-intermed.crt

RUN update-ca-certificates

# Use mvn dependency:copy as mvn dependency:get -Ddest option is deprecated
RUN mvn dependency:get -Dartifact=io.prometheus.jmx:jmx_prometheus_javaagent:${JMX_EXPORTER_VERSION}:jar && \
    mvn dependency:copy -Dartifact=io.prometheus.jmx:jmx_prometheus_javaagent:${JMX_EXPORTER_VERSION}:jar \
      -DoutputDirectory=/tmp && \
    mv /tmp/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar /tmp/jmx_prometheus_javaagent.jar

COPY . /tmp/presto
# Build presto and run tests. Only include modules with css tests.
# This is because presto-mysql and raptor implement abstract test modules and
# the parent class set up methods which set required fields are never executed
# due to not being in the "test-css" group.
RUN cd /tmp/presto && \
    mvn package -DskipTests && \
    mvn test -Dgroups=test-css -pl presto-css-functions,presto-hive,presto-css-event-listener

FROM google/cloud-sdk:latest as hudi_libs
ARG KEY_FILE
ARG HUDI_BUCKET
ARG HUDI_DIR
ARG HUDI_VERSION
ARG HUDI_TAG
WORKDIR /tmp
COPY ${KEY_FILE} key.json
RUN gcloud auth activate-service-account --key-file key.json && \
    gcloud config set project css-data-warehouse && \
    gsutil -m cp -r gs://${HUDI_BUCKET}/${HUDI_DIR}/${HUDI_VERSION}-${HUDI_TAG}/hudi-presto-bundle-${HUDI_VERSION}.jar .

FROM centos:centos7
ARG PRESTO_VERSION
ARG HUDI_VERSION
# Stage 1 - Install java
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0
RUN \
    set -xeu && \
    yum -y -q update && \
    yum -y -q install java-1.8.0-openjdk-devel less && \
    yum -q clean all && \
    rm -rf /var/cache/yum && \
    rm -rf /tmp/* /var/tmp/* && \
    groupadd presto --gid 1000 && \
    useradd presto --uid 1000 --gid 1000 && \
    mkdir -p /usr/lib/presto/jmx_exporter /data/presto && \
    chown -R "presto:presto" /usr/lib/presto /data/presto

# Stage 2 - Build presto

COPY --from=presto_builder /tmp/presto/presto-server/target/presto-server-${PRESTO_VERSION} /usr/lib/presto/

# Stage 3 - Copy hudi libraries
# Note: docker/out must contain the hudi-presto-bundle artifact:
# * For Jenkins builds this is done via the JenkinsFile - Jenkinsfile-master.groovy, Jenkinsfile-release.groovy
# * For local builds this is done via the local-docker-build.sh script
COPY --from=hudi_libs /tmp/hudi-presto-bundle-${HUDI_VERSION}.jar /usr/lib/presto/plugin/hive-hadoop2/

# Stage 4 - Install prometheus jmx exporter
COPY --from=presto_builder /tmp/jmx_prometheus_javaagent.jar /usr/lib/presto/jmx_exporter/

USER root
RUN chown -R presto:presto /usr/lib/presto

WORKDIR /usr/lib/presto/plugin
RUN rm -rf accumulo atop blackhole example-http kudu mongodb phoenix presto-elasticsearch redshift sqlserver
RUN rm -rf /tmp/workdir
WORKDIR /usr/lib/presto
# Default http port, https port, jmx reg port, jmx rmi port, jdwp port, custom http port,
EXPOSE 8080 8060 8059 8058 5005 8443

RUN echo "root:root" | chpasswd

USER presto:presto
