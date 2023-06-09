#!/bin/bash
EXTRA_KAFKA_GROUP_ID=""

[[ -z "${KAFKA_ACTION}" ]] && { echo "KAFKA_ACTION required"; exit 1; }
[[ -z "${KAFKA_DIR}" ]] && { echo "KAFKA_DIR missing"; exit 1; }
[[ -z "${KAFKA_CONFIG}" ]] && { KAFKA_CONFIG="server.properties"; }
[[ -z "${KAFKA_PARTITION}" ]] && { KAFKA_PARTITION=1; }
[[ "${KAFKA_GROUP_ID}" ]] && { EXTRA_KAFKA_GROUP_ID="--consumer-property group.id=${KAFKA_GROUP_ID}"; } 

case ${KAFKA_ACTION} in
    "start-zk")
    mkdir -p ${ZK_DATA_DIR}; # Data dir is setup in conf/zookeeper.properties
    zookeeper-server-start.sh ${KAFKA_DIR}/config/zookeeper.properties
    ;;
    "start-kafka")
    kafka-server-start.sh ${KAFKA_DIR}/config/${KAFKA_CONFIG}
    ;;
    "create-topic")
    kafka-topics.sh --create --bootstrap-server ${KAFKA_SERVER} --replication-factor 1 --partitions ${KAFKA_PARTITION} --topic ${KAFKA_TOPIC}
    kafka-configs.sh --bootstrap-server ${KAFKA_SERVER} --alter --entity-type topics --entity-name ${KAFKA_TOPIC} --add-config max.message.bytes=10485880 
    ;;
    "producer")
    kafka-console-producer.sh --broker-list ${KAFKA_SERVER} --topic ${KAFKA_TOPIC}
    ;;
    "consumer")
    kafka-console-consumer.sh --bootstrap-server ${KAFKA_SERVER} --topic ${KAFKA_TOPIC} --from-beginning ${KAFKA_CONSUMER_PROPERTIES} ${EXTRA_KAFKA_GROUP_ID}
    ;;
    "connect-standalone")
    cd ${KAFKA_DIR}
    #connect-standalone-twitter.properties mysqlSinkTwitter.conf
    touch /tmp/my-test.txt
    connect-standalone.sh ${KAFKA_DIR}/config/${KAFKA_WORKER_PROPERTIES} ${KAFKA_CONNECTOR_PROPERTIES}
    #${KAFKA_DIR}/config/${KAFKA_CONNECTOR_PROPERTIES}  
    ;;
    "run-class")
    cd ${KAFKA_DIR}
    kafka-run-class.sh ${KAFKA_DIR}/${KAFKA_CLASS} --bootstrap-server ${KAFKA_SERVER} --zookeeper ${KAFKA_ZK} --broker-list ${KAFKA_SERVER}
    ;;
esac

echo "Done"