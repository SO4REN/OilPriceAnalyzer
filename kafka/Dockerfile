FROM amazoncorretto:17

ENV PATH /opt/kafka/bin:$PATH
ENV KAFKA_DIR "/opt/kafka"

ARG KAFKA_VERSION="2.13-3.4.0"

ADD setup/kafka_${KAFKA_VERSION}.tgz /opt

RUN ln -s /opt/kafka_${KAFKA_VERSION} ${KAFKA_DIR} 

ADD kafka-manager.sh ${KAFKA_DIR}/bin/kafka-manager

ADD conf/kraft/* ${KAFKA_DIR}/config/kraft/

RUN chmod +x ${KAFKA_DIR}/bin/kafka-manager

ENTRYPOINT [ "kafka-manager" ]
