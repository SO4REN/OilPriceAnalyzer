FROM docker.elastic.co/logstash/logstash:8.7.0

ENV XPACK_MONITORING_ENABLED=false

RUN logstash-plugin install logstash-input-http_poller 
RUN logstash-plugin install logstash-codec-csv
RUN logstash-plugin install logstash-filter-fingerprint