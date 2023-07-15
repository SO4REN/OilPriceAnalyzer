FROM amazoncorretto:17
ENV SPARK_VERSION=3.4.0
ENV HADOOP_VERSION=hadoop3
ENV SPARK_DIR=/opt/spark
ENV PATH $SPARK_DIR/bin:$PATH

ADD setup/spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz /opt

RUN yum -y update && yum install -y procps gcc openssl-devel bzip2-devel libffi-devel wget tar make

RUN yum -y install python3

RUN yum -y install glibc

RUN pip3 install elasticsearch kafka-python numpy pyspark 

RUN ln -s /opt/spark-${SPARK_VERSION}-bin-${HADOOP_VERSION} ${SPARK_DIR}
