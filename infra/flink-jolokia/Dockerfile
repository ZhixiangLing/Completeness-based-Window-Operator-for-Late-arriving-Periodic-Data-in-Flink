FROM flink:2.0.1-java17

ARG JOLOKIA_VERSION=2.1.2
ARG JOLOKIA_JAR=jolokia-agent-jvm-${JOLOKIA_VERSION}-javaagent.jar

ADD https://repo1.maven.org/maven2/org/jolokia/jolokia-agent-jvm/${JOLOKIA_VERSION}/${JOLOKIA_JAR} /opt/jolokia/jolokia-jvm-agent.jar
