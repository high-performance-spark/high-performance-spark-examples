ARG base
FROM $base

USER root
RUN pip install --no-cache-dir pyarrow pyiceberg[pandas,snappy,daft,s3fs] avro fastavro
USER dev
RUN sbt clean compile
