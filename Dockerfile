ARG base
FROM $base

USER root
RUN pip install pyarrow pyiceberg[pandas,snappy,daft,s3fs] avro
USER dev
RUN sbt clean compile
