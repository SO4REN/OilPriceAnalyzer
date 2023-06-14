FROM amazoncorretto:17
ENV SPARK_VERSION=3.4.0
ENV HADOOP_VERSION=hadoop3
ENV SPARK_DIR=/opt/spark
ENV PATH $SPARK_DIR/bin:$PATH

ADD setup/spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz /opt

RUN yum -y update && yum install -y procps gcc openssl-devel bzip2-devel libffi-devel wget tar make

#RUN wget https://www.python.org/ftp/python/3.11.1/Python-3.11.1.tgz && tar xzf Python-3.11.1.tgz && cd Python-3.11.1 && ./configure --enable-optimizations && make install   
RUN yum -y install python3

RUN yum -y install glibc
# RUN apt-get update && apt-get -y install bash python3 python3-pip netcat

RUN pip3 install pyspark numpy elasticsearch kafka-python
# Create Sym Link 
RUN ln -s /opt/spark-${SPARK_VERSION}-bin-${HADOOP_VERSION} ${SPARK_DIR}

# Add Python Code
ADD code/*.py opt/code/
ADD code/dataset/anagrafica_impianti_CT.parquet opt/code/dataset/
ADD code/dataset/Prezzi/* opt/code/dataset/Prezzi/


# ENTRYPOINT [ "/opt/spark/bin/spark-submit", "--packages",\
#             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.7.1",\
#             "--master", "local[*]",\
#             "opt/code/clean.py"]

# ENTRYPOINT [ "/opt/spark/bin/spark-submit", "--packages",\
#             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.7.1",\
#             "--master", "local[*]",\
#             "opt/code/predict.py"]
