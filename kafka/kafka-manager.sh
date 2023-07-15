#!/bin/bash

case ${KAFKA_ACTION} in
    "start-zk")
    zookeeper-server-start.sh ${KAFKA_DIR}/config/zookeeper.properties
    ;;
    "start-kafka")
    if [[ ${KAFKA_CONFIG} == "kraft" ]]; then
        KAFKA_CONFIG="kraft/server.properties";
        KAFKA__UUID=$(kafka-storage.sh random-uuid)
        
        kafka-storage.sh format -t ${KAFKA__UUID} -c ${KAFKA_DIR}/config/${KAFKA_CONFIG}
    else
        KAFKA_CONFIG="server.properties"
    fi
    kafka-server-start.sh ${KAFKA_DIR}/config/${KAFKA_CONFIG}
    ;;
    "create-topic")
    kafka-topics.sh --bootstrap-server ${KAFKA_SERVER} --create --topic ${KAFKA_TOPIC} --replication-factor 1 --partitions 1  --config max.message.bytes=10485880
    ;;
esac
